# ota-cloud Sentinel 流量防护与熔断降级全景架构指南

> **定位**：系统性梳理 `ota-cloud` 中基于 Alibaba Sentinel 1.8.x 的**微服务高可用流量防护体系**。涵盖 QPS 限流、热点参数流控（Hotspot Param Flow）、慢调用比例与异常熔断降级（Circuit Breaking）、`blockHandler` 与 `fallback` 优雅降级返回，以及控制台动态规则管控。

---

## 一、架构定位与设计模式：Flow Control & Circuit Breaker

在旅游出行业务中，大促活动、突发秒杀抢购或恶意爬虫极易导致某家爆款酒店或某些接口 QPS 呈指数级飙升：
- **流量整形与限流（Rate Limiting）**：设定入口 QPS 阈值，超出部分执行“快速失败（Fast Fail）”或“排队等待（Warm Up）”，绝不允许非核心流量拖垮整个微服务；
- **熔断降级（Circuit Breaking）**：当依赖的下游服务（如上游供应直连接口）发生慢调用比例超标或连续抛出异常时，及时切断调用链路（进入 OPEN 状态），直接返回兜底 Mock 数据，阻止级联雪崩；
- **热点参数防护（Hotspot Protection）**：对查询参数（如 `hotelId`）进行细粒度频次统计，只限制个别被打爆的热门酒店，绝不影响其他正常酒店的业务预订。

```
【客户端流量涌入】
       │
       ▼
┌──────────────────────────────────────────────┐
│           SentinelResource: hotelDetail      │
└──────────────────────┬───────────────────────┘
                       │
       ┌───────────────┴───────────────┐
       ▼ (QPS / 热点参数未超标)         ▼ (触发限流规则 BlockException)
【正常执行业务与查库】               【blockHandler: hotelDetailBlockHandler】
       │                               │
       │ (若业务发生未预期系统异常)        ▼
       └───────────────► 触发 fallback: hotelDetailFallback
                         返回 429 / 500 优雅降级报文
```

---

## 二、微服务拓扑与角色分工

1. **`ota-gateway`（服务端口 8080）**：
   - 网关层前置限流：基于客户端 IP、请求 URL 维度的全局流量整形。
2. **`ota-hotel`（酒店中心，端口 8083）**：
   - 核心防刷入口：对 `hotelDetail` 酒店详情接口实施热点参数防护（限制单一酒店 ID 每秒最大访问频次）。
3. **`ota-supplier`（上游供应适配，端口 8089）**：
   - 外部调用熔断：针对外部华住 CRS / B2B 接口设置慢调用降级规则，避免外部超时拖垮内部线程。

---

## 三、核心规则与控制维度矩阵

| 规则类型 | 监控资源名 (Resource) | 判定维度 | 阈值标准 | 降级行为 |
| :--- | :--- | :--- | :--- | :--- |
| **QPS 限流** | `hotelDetail` | 单机 QPS | > 1000 次/秒 | 返回 HTTP 429 提示稍后再试 |
| **热点参数限流** | `hotelDetail` (Param 0: `id`) | 单个 `hotelId` QPS | > 100 次/秒 | 单独限制该爆款酒店，不伤及无辜 |
| **慢调用熔断** | `supplierAdapter.queryRate` | RT > 2000ms 比例 > 50% | 持续 5s 熔断 | 熔断 10s，直接返回缓存兜底底价 |
| **异常比例熔断** | `supplierAdapter.createBooking` | 异常比例 > 30% | 持续 5s 熔断 | 快速失败，提示切入人工出票 |

---

## 四、关键落地技术细节

### 1. 业务 Controller 中无侵入注解埋点（`HotelController.java`）

```java
package com.ota.cloud.hotel.controller;

import com.alibaba.csp.sentinel.annotation.SentinelResource;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import com.ota.cloud.common.model.Result;
import com.ota.cloud.hotel.service.HotelService;
import com.ota.cloud.hotel.vo.HotelDetailVO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/api/hotel")
@RequiredArgsConstructor
public class HotelController {

    private final HotelService hotelService;

    /**
     * 酒店详情接口（配置 SentinelResource）
     */
    @GetMapping("/{id}")
    @SentinelResource(
            value = "hotelDetail",
            blockHandler = "hotelDetailBlockHandler",
            fallback = "hotelDetailFallback"
    )
    public Result<HotelDetailVO> detail(@PathVariable Long id) {
        HotelDetailVO detail = hotelService.getDetailWithCache(id);
        if (detail == null) {
            return Result.error(404, "酒店不存在或已下线");
        }
        return Result.success(detail);
    }

    /**
     * 触发限流规则（BlockException）时的回调处理器
     * 注意：方法签名参数必须与原方法一致，并追加 BlockException
     */
    public Result<HotelDetailVO> hotelDetailBlockHandler(Long id, BlockException ex) {
        log.warn("酒店详情接口被 Sentinel 限流，hotelId={}, rule={}", id, ex.getRule());
        return Result.error(429, "该酒店查询过于频繁，请稍后再试");
    }

    /**
     * 运行时出现系统异常（Throwable）时的降级处理器
     */
    public Result<HotelDetailVO> hotelDetailFallback(Long id, Throwable ex) {
        log.error("酒店详情接口异常，hotelId={}", id, ex);
        return Result.error(500, "酒店服务繁忙，请稍后重试");
    }
}
```

### 2. 客户端 Sentinel 传输配置（`application.yml`）

各微服务在启动时向 Sentinel 控制台注册自身：

```yaml
spring:
  cloud:
    sentinel:
      transport:
        dashboard: ${SENTINEL_DASHBOARD:100.99.175.108:8088}
        port: 8719      # 客户端向控制台上报心跳和接收规则指令的 HTTP 端口
      eager: true       # 取消懒加载，启动时立即向控制台注册
```

---

## 五、监控指标与运维排查

### 1. Sentinel Dashboard 实时链路监控
- **Pass QPS**：成功通过并执行业务的每秒请求数；
- **Block QPS**：因命中阈值被拦截丢弃的请求数；
- **Response Time (ms)**：平均业务执行耗时。

### 2. 常见排查技巧
- 为什么在控制台看不到微服务？
  - 检查客户端 `port: 8719` 是否与其他本地进程冲突；若已被占用，Sentinel 会自动递增尝试 `8720`, `8721`。

---

## 六、基础设施与运维部署

- **镜像与版本**：`bladex/sentinel-dashboard:1.8.10`
- **Docker 运行参数**：
  ```yaml
  sentinel:
    image: bladex/sentinel-dashboard:1.8.10
    container_name: ota-sentinel
    restart: unless-stopped
    ports:
      - "8088:8858"
    environment:
      JAVA_OPTS: -Dserver.port=8858
    networks:
      - ota-network
  ```
- **控制台入口**：`http://localhost:8088` 或 `http://100.99.175.108:8088`（默认账号密码：`sentinel` / `sentinel`）。
