# 第9章：上游供应方适配 (ota-supplier)

> **目标**：搭建独立的上游供应方适配服务 `ota-supplier:8089`，基于端口与适配器模式（Hexagonal Architecture）统一抽象供应商能力（`SupplierAdapter`），支持多供应商接入（Mock 内存模拟与华住 CRS/B2B 真实协议），实现静态酒店、实时价库与集团推单履约闭环。

---

## 9.1 业务背景与架构设计

OTA 平台的“上游（Inbound）”面向各大酒店集团（如华住、锦江、首旅如家等）与批发商（如天下房仓、美团 B2B）。不同供应商的技术生态差异极大：
- **通信协议异构**：部分提供 RESTful JSON，部分沿用 SOAP/XML 或专用二进制协议；
- **鉴权体系各异**：华住采用 OAuth2 Client Credentials 配合高频刷新机制（需处理 HTTP 545 Token 过期状态码），其他厂商可能采用 HMAC-SHA256 签名；
- **履约模型不同**：大部分要求三阶段履约：试单校验（`prebook` / `checkAvailability`）➔ 正式锁单出票（`book` / `order/add`）➔ 取消退订（`cancel`）。

为了防止上游多变的技术细节污染核心交易中台（`ota-order`、`ota-hotel`、`ota-inventory`），系统设计了独立的适配层服务 `ota-supplier`：

```
                    ┌─────────────────────────┐
                    │    ota-supplier (8089)   │
                    └────────────┬────────────┘
                                 │
                          SupplierRouter
                                 │
            ┌─────────────────────┴─────────────────────┐
            ▼                                           ▼
┌──────────────────────┐                    ┌──────────────────────┐
│  MockSupplierAdapter │                    │   HzSupplierAdapter  │
│      (内存模拟)      │                    │ (华住 CRS/B2B 协议)  │
└──────────────────────┘                    └──────────────────────┘
```

---

## 9.2 核心端口抽象 (`SupplierAdapter`)

供应商适配器采用 SPI 设计思想，统一提供酒店静态数据、实时房价库存与订单履约三组核心能力：

```java
package com.ota.cloud.supplier.port;

import com.ota.cloud.supplier.domain.SupplierCode;

/**
 * 供应商统一适配器主端口
 */
public interface SupplierAdapter {

    /**
     * 供应商标识（如 MOCK、HUZHU）
     */
    SupplierCode supplierCode();

    /**
     * 酒店静态内容端口
     */
    HotelContentPort hotelContent();

    /**
     * 实时房价与库存端口
     */
    RateInventoryPort rateInventory();

    /**
     * 订单履约端口
     */
    BookingPort booking();

    /**
     * 适配器是否启用（支持 Nacos 动态配置灰度切换）
     */
    default boolean enabled() {
        return true;
    }
}
```

### 订单履约端口契约 (`BookingPort`)

```java
package com.ota.cloud.supplier.port;

import com.ota.cloud.supplier.domain.BookingRequest;
import com.ota.cloud.supplier.domain.BookingResult;
import com.ota.cloud.supplier.domain.CancelResult;
import com.ota.cloud.supplier.domain.PrebookRequest;
import com.ota.cloud.supplier.domain.PrebookResult;
import com.ota.cloud.supplier.domain.SupplierOrderStatus;

/**
 * 订单履约端口（抽象自供应商下单接口，保证全局 requestNo 幂等）
 */
public interface BookingPort {

    /**
     * 预订前试单验价（验证房态、房量与价格有效性）
     */
    PrebookResult prebook(PrebookRequest request);

    /**
     * 正式锁房出票（返回集团确认单号 supplierOrderNo）
     */
    BookingResult book(BookingRequest request);

    /**
     * 取消订单并释放集团预订
     */
    CancelResult cancel(String supplierOrderNo, String requestNo);

    /**
     * 查询集团远端真实履约状态
     */
    SupplierOrderStatus query(String supplierOrderNo);
}
```

---

## 9.3 供应方动态路由器 (`SupplierRouter`)

`SupplierRouter` 负责根据请求中的 `supplierCode` 动态解析并路由到具体的 Adapter 实现。如果请求的供应商未启用或未接入，自动平稳回退至默认的 MOCK 适配器：

```java
package com.ota.cloud.supplier.router;

import com.ota.cloud.supplier.config.SupplierProperties;
import com.ota.cloud.supplier.domain.SupplierCode;
import com.ota.cloud.supplier.port.SupplierAdapter;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 供应商适配器核心路由器组件。
 * <p>
 * 职责：依据请求参数中的 {@code supplierCode}，将请求动态分发至具体的 {@link SupplierAdapter} 实现类；
 * 当遇到未知或已禁用的供应商编码时，平稳回退至默认适配器（默认通常为 MOCK）。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SupplierRouter {

    /** 容器自动注入的所有供应商适配器列表。 */
    private final List<SupplierAdapter> adapters;

    /** 供应商模块配置属性。 */
    private final SupplierProperties properties;

    /** 内存快速索引映射表。 */
    private final Map<SupplierCode, SupplierAdapter> adapterMap = new HashMap<>();

    /**
     * 初始化回调方法：遍历所有已注入的适配器建立枚举映射索引。
     */
    @PostConstruct
    public void init() {
        for (SupplierAdapter adapter : adapters) {
            adapterMap.put(adapter.supplierCode(), adapter);
            log.info("注册供应商适配器: code={}, enabled={}", adapter.supplierCode(), adapter.enabled());
        }
    }

    /**
     * 根据供应商编码获取适配器；未找到或未启用时平稳降级至默认适配器。
     *
     * @param supplierCode 外部请求传入的供应商代码（如 HUZHU、MOCK）
     * @return 匹配且已启用的供应商适配器实例
     * @throws IllegalStateException 当默认适配器未配置或不可用时抛出异常
     */
    public SupplierAdapter route(String supplierCode) {
        SupplierCode code = SupplierCode.fromCode(supplierCode);
        SupplierAdapter adapter = adapterMap.get(code);
        // 1. 精确匹配且启用的适配器优先返回
        if (adapter != null && adapter.enabled()) {
            return adapter;
        }
        // 2. 未匹配或禁用，降级回退至默认兜底适配器
        SupplierAdapter fallback = adapterMap.get(SupplierCode.fromCode(properties.getRouter().getDefaultAdapter()));
        if (fallback == null || !fallback.enabled()) {
            throw new IllegalStateException("默认供应商适配器未就绪");
        }
        return fallback;
    }

    /**
     * 获取系统当前配置的默认兜底适配器。
     *
     * @return 默认适配器实例
     */
    public SupplierAdapter defaultAdapter() {
        return route(properties.getRouter().getDefaultAdapter());
    }
}
```

---

## 9.4 订单中心改造与协同

订单中心（`ota-order`）表结构新增 4 个供应商追踪字段，并在创建、支付与取消时通过 Feign 联动：

### 1. 数据库 DDL 扩展

```sql
ALTER TABLE `orders`
    ADD COLUMN `supplier_code` VARCHAR(32) NOT NULL DEFAULT 'MOCK' COMMENT '供应商编码（MOCK/HUZHU）' AFTER `status`,
    ADD COLUMN `supplier_order_no` VARCHAR(64) DEFAULT '' COMMENT '供应商远端订单号' AFTER `supplier_code`,
    ADD COLUMN `supplier_prebook_no` VARCHAR(64) DEFAULT '' COMMENT '供应商试单流水号' AFTER `supplier_order_no`,
    ADD COLUMN `supplier_status` VARCHAR(32) DEFAULT 'INIT' COMMENT '供应商履约状态' AFTER `supplier_order_no`,
    ADD INDEX `idx_supplier_order_no` (`supplier_order_no`);
```

### 2. 创单与支付链路协同

```
[客户端] ──(创单)──> [ota-order] ──(Feign: prebook)──> [ota-supplier] ──(校验通过)
                         │ (更新本地订单 supplier_prebook_no)
                         ▼
[客户端] ──(支付)──> [ota-order] ──(本地事务扣库存+状态PAID)
                         │
                         ▼
                  [ota-supplier] ──(Feign: book)──> 远端集团出票
                         │ (成功回写 supplier_order_no)
```

---

## 9.5 异常订单补偿机制 (`SupplierCompensationJob`)

当用户支付成功后，调用 `ota-supplier.book` 发生瞬时网络超时或第三方接口抖动时，订单会停留在 `PAID` 状态，但 `supplier_order_no` 缺失。系统通过后台定时任务进行兜底补偿：

```java
package com.ota.cloud.order.job;

import com.ota.cloud.common.model.Result;
import com.ota.cloud.order.entity.Order;
import com.ota.cloud.order.feign.SupplierFeignClient;
import com.ota.cloud.order.feign.dto.SupplierBookingRequestDTO;
import com.ota.cloud.order.feign.dto.SupplierBookingResponseDTO;
import com.ota.cloud.order.mapper.OrderMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 供应商异步推单异常补偿定时任务。
 * <p>
 * 处理场景：
 * 订单已支付成功（PAID），但在向供应商发起正式锁房出票时因网络超时或第三方接口抖动未拿到 supplierOrderNo。
 * 定时任务周期性扫描这些单据，重新向供应商发起具有全局唯一幂等约束的预订请求。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SupplierCompensationJob {

    /** 订单数据 Mapper。 */
    private final OrderMapper orderMapper;

    /** 供应商 Feign 客户端。 */
    private final SupplierFeignClient supplierFeignClient;

    /** 任务启停开关配置。 */
    @Value("${order.supplier-compensation-job.enabled:true}")
    private boolean enabled;

    /** 单批次扫描补单上限。 */
    @Value("${order.supplier-compensation-job.batch-size:20}")
    private int batchSize;

    /**
     * 定时执行补偿扫描。
     * <p>
     * 业务流程：
     * <ol>
     *   <li>扫描状态为 PAID 但 supplier_order_no 为空的订单候选集；</li>
     *   <li>重新构建 {@link SupplierBookingRequestDTO}，强制设置 {@code requestNo = orderId}；</li>
     *   <li>调用供应商 book 接口执行出票；</li>
     *   <li>出票成功后将供应商单号回填订单主表。</li>
     * </ol>
     */
    @Scheduled(initialDelayString = "${order.supplier-compensation-job.initial-delay:90000}",
            fixedDelayString = "${order.supplier-compensation-job.fixed-delay:60000}")
    public void compensate() {
        if (!enabled) {
            return;
        }
        // 抓取已支付但缺少供应商订单号的孤儿订单
        List<Order> candidates = orderMapper.selectPaidWithoutSupplierOrderNo(batchSize);
        if (candidates.isEmpty()) {
            return;
        }
        log.info("供应商补偿任务扫描到 {} 条待补单订单", candidates.size());
        for (Order order : candidates) {
            try {
                SupplierBookingRequestDTO request = new SupplierBookingRequestDTO();
                request.setSupplierCode(order.getSupplierCode());
                request.setOrderId(order.getId());
                // requestNo 等于 orderId，保证无论重试多少次，供应商端严格执行幂等，不重复扣房
                request.setRequestNo(order.getId());

                // 发起远程 Feign 补单调用
                Result<SupplierBookingResponseDTO> result = supplierFeignClient.book(request);
                if (result.isSuccess() && result.getData() != null) {
                    // 出票成功，回填供应商单号与终态
                    orderMapper.updateSupplierInfo(
                            order.getId(),
                            result.getData().getSupplierOrderNo(),
                            result.getData().getSupplierStatus(),
                            ""
                    );
                    log.info("订单 {} 供应商补偿出票成功，supplierOrderNo={}",
                            order.getId(), result.getData().getSupplierOrderNo());
                }
            } catch (Exception e) {
                // 异常隔离：单单失败只记警告日志，不影响批次内其他订单
                log.warn("订单 {} 供应商补偿异常: {}", order.getId(), e.getMessage());
            }
        }
    }
}
```

---

## 9.6 平台与供应方对账中心

`ota-admin` 提供了 `SupplierReconcileController`，支持运营人员按供应商和日期范围对账：
1. **拉取本地订单状态**：过滤指定供应商的本地交易流水；
2. **并发反查远端状态**：并发调用 `ota-supplier.query` 获取集团真实出票状态；
3. **输出三方核对结果**：标记状态完全对齐（`MATCHED`）、集团订单号缺失（`MISSING_REMOTE_NO`）或状态不一致（`STATUS_MISMATCH`），并提供控制台单单手动触发补偿。

---

## 9.7 自动化验证与测试

```bash
# 执行第 9 章供应商适配冒烟测试（覆盖健康检查、MOCK 预订、HUZHU 降级与对账）
bash scripts/chapter9-supplier-smoke.sh
```

