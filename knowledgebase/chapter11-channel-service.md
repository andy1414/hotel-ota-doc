# 第11章：下游渠道分发 (ota-channel)

> **目标**：搭建独立的下游渠道分发微服务 `ota-channel:8090`，完成 OTA 平台的双向模型闭环（第 9 章上游供应 Inbound + 本章下游渠道 Outbound）：将平台聚合后的标准房型与价库推流至下游分销渠道（美团、携程、飞猪、逸扉），集成动态加价规则引擎、推单/查单/取消三件套以及基于 Kafka 的异步状态事件总线。

---

## 11.1 业务架构与双向闭环模型

OTA 平台的商业本质是“连接供应与渠道，赚取分销差价”：
- **上游供应（Inbound，8089 `ota-supplier`）**：对接华住、锦江等酒店集团，将异构房源与价库拉取并转换为内部标准模型；
- **核心中台（Core Services）**：`ota-hotel`（酒店主数据）、`ota-inventory`（价库合一表与乐观锁）、`ota-order`（状态机与事务发件箱）；
- **下游渠道（Outbound，8090 `ota-channel`）**：针对不同销售渠道（美团分销、携程代理等）进行规则动态加价，并将平台订单履约状态实时推送至渠道系统。

```
【上游供应 Inbound】
  华住 / 锦江 / Mock
         │ (8089 ota-supplier)
         ▼
【核心中台 Core】
  ota-hotel (8083)  +  ota-inventory (8084)  +  ota-order (8085)
                                                      │
                         ┌────────────────────────────┘
                         ▼ (Kafka: order-paid / order-cancelled)
【下游渠道 Outbound】
  ota-channel (8090) ──> 渠道推单 / 查单 / 取消 ──> 美团 / 携程 / 飞猪 / 逸扉
```

---

## 11.2 核心渠道端口抽象 (`ChannelAdapter`)

渠道分发服务同样基于端口与适配器架构，对所有下游分销平台定义统一的调用规范：

```java
package com.ota.cloud.channel.port;

import com.ota.cloud.channel.domain.ChannelCode;

/**
 * 下游渠道统一适配器主端口
 */
public interface ChannelAdapter {

    /**
     * 渠道唯一标识编码（如 MOCK、YESTE）
     */
    ChannelCode channelCode();

    /**
     * 渠道订单履约三件套端口
     */
    ChannelBookingPort channelBooking();

    /**
     * 渠道是否启用（支持 Nacos 动态灰度控制）
     */
    default boolean enabled() {
        return true;
    }
}
```

### 订单履约三件套端口 (`ChannelBookingPort`)

```java
package com.ota.cloud.channel.port;

import com.ota.cloud.channel.domain.ChannelBookingRequest;
import com.ota.cloud.channel.domain.ChannelBookingResult;
import com.ota.cloud.channel.domain.ChannelCancelResult;
import com.ota.cloud.channel.domain.ChannelOrderStatus;

/**
 * 渠道履约三件套：推单出票、查单核对、退单取消
 */
public interface ChannelBookingPort {

    /**
     * 向渠道推送订单出票请求（返回渠道订单号 channelOrderNo）
     */
    ChannelBookingResult pushOrder(ChannelBookingRequest request);

    /**
     * 查询渠道远端订单状态
     */
    ChannelOrderStatus queryOrder(String channelOrderNo);

    /**
     * 向渠道申请退订取消
     */
    ChannelCancelResult cancelOrder(String channelOrderNo, String reason);
}
```

---

## 11.3 动态渠道加价规则引擎

平台向不同渠道分发时，不能直接暴露底价，需根据中台配置的费率与加价系数进行动态算价：

1. **基础费率规则 (`downstream_channel`)**：渠道全局基础加价百分比（如基础加价 5%）；
2. **范围定向加价 (`channel_data_range`)**：针对特定城市、特定酒店集团或房型设置额外浮动系数（如核心商圈加价 8%）；
3. **加价计算公式**：
   $$\text{FinalSellPrice} = \text{BasePrice} \times (1 + \text{BaseRate} + \text{RangeRate})$$

### 加价引擎实现 (`ChannelPricingService`)

```java
package com.ota.cloud.inventory.service;

import com.ota.cloud.inventory.feign.AdminPricingClient;
import com.ota.cloud.inventory.feign.dto.ChannelPricingRuleDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

/**
 * 渠道加价计算引擎服务。
 * <p>
 * 核心机制：
 * 1. 动态规则匹配：根据 (channelCode, hotelId, roomType) 三维坐标拉取 Admin 运营规则；
 * 2. 读时加价：不在库存底价表落库加价结果，保证基准价与渠道价解耦；
 * 3. 熔断降级保护：远程 Feign 失败时自动降级回退零加价。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ChannelPricingService {

    /** 平台 Admin 加价规则 Feign 客户端。 */
    private final AdminPricingClient adminPricingClient;

    /**
     * 查询指定渠道针对特定酒店/房型的生效加价系数。
     * <p>
     * 规则遍历累加：
     * 匹配渠道全局规则与房型特定规则，将多个有效规则的 coefficient 进行代数叠加。
     *
     * @param channelCode 下游渠道代码（如 CTRIP、FLIGGY、MOCK 等）
     * @param hotelId     酒店主键 ID
     * @param roomType    房型代码
     * @return 实际生效的加价百分比小数（例如 0.05 代表加价 5%）；异常或未配置返回 ZERO
     */
    public BigDecimal queryEffectiveCoe(String channelCode, Long hotelId, String roomType) {
        if (channelCode == null || channelCode.isBlank()) {
            return BigDecimal.ZERO;
        }
        try {
            // 调用 Admin 微服务查询当前渠道在目标酒店配置的规则列表
            List<ChannelPricingRuleDTO> rules = adminPricingClient.getPricingRules(channelCode, hotelId);
            BigDecimal totalCoe = BigDecimal.ZERO;
            for (ChannelPricingRuleDTO rule : rules) {
                // 逐条判定维度匹配条件
                if (rule.matches(hotelId, roomType)) {
                    totalCoe = totalCoe.add(rule.getCoefficient());
                }
            }
            return totalCoe;
        } catch (Exception e) {
            // 异常降级处理：规则服务不可用时不阻塞正常售卖，按不加价兜底
            log.warn("拉取加价规则失败，采用基准零加价: channelCode={}, hotelId={}", channelCode, hotelId, e);
            return BigDecimal.ZERO;
        }
    }

    /**
     * 应用加价系数计算最终渠道售卖价。
     * <p>
     * 计算数学公式：{@code finalPrice = basePrice * (1 + effectiveCoe)}
     *
     * @param basePrice    基准售卖底价
     * @param effectiveCoe 综合有效加价系数
     * @return 加价后四舍五入保留两位小数的价格
     */
    public BigDecimal applyChannelCoe(BigDecimal basePrice, BigDecimal effectiveCoe) {
        if (basePrice == null) {
            return BigDecimal.ZERO;
        }
        if (effectiveCoe == null || effectiveCoe.compareTo(BigDecimal.ZERO) <= 0) {
            return basePrice;
        }
        // 执行系数乘算并保留两位小数
        BigDecimal multiplier = BigDecimal.ONE.add(effectiveCoe);
        return basePrice.multiply(multiplier).setScale(2, RoundingMode.HALF_UP);
    }
}
```

---

## 11.4 订单中心改造与异步事件驱动

### 1. 订单表增加渠道跟踪字段

```sql
ALTER TABLE `orders`
    ADD COLUMN `channel_code` VARCHAR(32) DEFAULT NULL COMMENT '下游分销渠道编码（MOCK/YESTE）' AFTER `supplier_status`,
    ADD COLUMN `channel_order_no` VARCHAR(64) DEFAULT '' COMMENT '下游渠道订单编号' AFTER `channel_code`,
    ADD COLUMN `channel_status` VARCHAR(32) DEFAULT 'INIT' COMMENT '渠道履约状态' AFTER `channel_order_no`,
    ADD COLUMN `channel_pushed_at` DATETIME DEFAULT NULL COMMENT '向渠道成功推单时间' AFTER `channel_status`,
    ADD INDEX `idx_channel_order_no` (`channel_order_no`);
```

### 2. Kafka 异步推单消费者 (`OrderPaidEventConsumer`)

订单支付完成后，`ota-order` 的事务发件箱投递 `order-paid` 事件到 Kafka。`ota-channel` 建立独立消费者组订阅并驱动渠道出票：

```java
package com.ota.cloud.channel.consumer;

import com.ota.cloud.channel.service.ChannelOrderService;
import com.ota.cloud.common.event.OrderPaidEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * 订单支付事件 Kafka 消费者。
 * <p>
 * 职责：订阅 order-paid 主题，当订单完成线上支付后，
 * 异步通知下游分销渠道执行最终确认并出票，保证分布式最终一致性。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderPaidEventConsumer {

    /** 渠道订单履约服务。 */
    private final ChannelOrderService channelOrderService;

    /**
     * Kafka 消息监听处理入口。
     *
     * @param record Kafka 消息消费记录，包含 Topic、Partition、Offset 与 Key-Value
     * @param ack    手动位移确认句柄
     */
    @KafkaListener(topics = "order-paid", groupId = "${spring.kafka.consumer.group-id:ota-channel-service}")
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String payload = record.value();
        String orderId = record.key();
        try {
            // 空消息防御性处理
            if (!StringUtils.hasText(payload) && !StringUtils.hasText(orderId)) {
                ack.acknowledge();
                return;
            }
            log.info("渠道服务收到 order-paid 事件: orderId={}, partition={}, offset={}",
                    orderId, record.partition(), record.offset());

            // 触发向美团/携程推单出票或状态确认
            channelOrderService.confirmByOrderId(orderId);
            // 手动提交消费位移
            ack.acknowledge();
        } catch (Exception e) {
            // 异常隔离：记录日志并提交位移，未推单成功的单据由后台定时补偿任务 ChannelCompensationJob 处理，防止阻塞其他消息
            log.error("渠道推单消费异常, orderId={}", orderId, e);
            ack.acknowledge();
        }
    }
}
```

---

## 11.5 渠道对账与控制台追踪

`ota-admin` 运营中台提供了 `ChannelReconcileController`，支持运营人员排查渠道推单状态：
- **`GET /admin/channel/orders`**：分页查询平台订单与渠道关联流水；
- **`GET /admin/channel/diff`**：拉取渠道推单失败或状态不一致的异常清单；
- **`POST /admin/channel/retry/{orderId}`**：针对异常订单手动发起渠道重新推单。

---

## 11.6 快速验证

```bash
# 串行执行第 11 章全部 Phase 冒烟验证（覆盖适配器路由、动态加价、推单查单与 Kafka 事件闭环）
bash scripts/chapter11-smoke-all.sh
```

