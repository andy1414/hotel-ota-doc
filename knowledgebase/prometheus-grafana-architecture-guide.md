# ota-cloud 监控度量与告警体系全景架构指南 (Prometheus + Grafana + Alertmanager)

> **定位**：系统性梳理 `ota-cloud` 中基于 Prometheus v3.x、Grafana 11.x 与 Alertmanager 的**生产级指标度量、多维度仪表盘与智能告警体系**。涵盖全微服务 Micrometer 指标暴露、端口自动化对账机制、8 条核心业务告警规则、分级告警路由与通知抑制策略。

---

## 一、架构定位与设计模式：Metrics-Based Observability

在分布式微服务集群中，系统不可用、内存泄漏或下游接口失败率上升往往具有“静默性”：
- **Pull 模式主动拉取**：Prometheus 遵循定时轮询机制（15s），按规范抓取各微服务内置的 `/actuator/prometheus` 端点；
- **端口单一来源（Single Source of Truth）**：严禁各处散落端口硬编码，Prometheus 的 `scrape_configs` 必须严格与 `ServicePortConstants` 保持双向对账，任一服务遗漏即触发自动化单元测试阻断；
- **分级告警路由（Severity Routing）**：
  - `critical`：核心服务宕机（`up == 0`）等灾难级异常，0 秒延迟立即发送；
  - `warning`：如堆内存偏高（>85%）、Kafka 偶发积压，执行 5 分钟滑动窗口收敛合并，杜绝告警风暴。

```
【微服务集群 (ota-gateway ~ ota-channel)】
  └── 暴露 /actuator/prometheus (Micrometer)
                 ▲
                 │ 每 15s 定时 Pull 拉取
┌────────────────┴─────────────────────────────┐
│          Prometheus Server (v3.2.1)          │
│  - TSDB 时序数据库 (存储 15 天)              │
│  - PromQL 计算引擎 (评估 alerts.yml 规则)    │
└────────────────┬──────────────┬──────────────┘
                 │              │
        (PromQL 查询渲染)        │ (触发阈值推单)
                 ▼              ▼
┌─────────────────────────┐  ┌───────────────────────────────────┐
│     Grafana (v11.5.2)   │  │     Alertmanager (v0.27.0)        │
│  - 微服务全局总览大屏   │  │  - 分级路由 (critical / warning) │
│  - JVM 资源与 GC 监控   │  │  - 静默抑制与邮件/Webhook 推送   │
│  - 渠道推单与业务交易盘 │  └───────────────────────────────────┘
└─────────────────────────┘
```

---

## 二、微服务抓取拓扑与端口对账矩阵

Prometheus 统一抓取 `host.docker.internal:<port>`，端口映射严格对齐 `ServicePortConstants`：

| 微服务 Job 名称 | 监控端口 | 核心度量关注点 |
| :--- | :--- | :--- |
| **`ota-gateway`** | **8080** | 网关 QPS、路由分发状态码、SSL/CORS 耗时 |
| **`ota-auth`** | **8081** | 登录成功/失败频次、JWT 颁发量 |
| **`ota-user`** | **8082** | 活跃用户数、用户注册曲线 |
| **`ota-hotel`** | **8083** | 缓存命中率、详情接口耗时 |
| **`ota-inventory`**| **8084** | 库存乐观锁冲突重试率、价库查询吞吐 |
| **`ota-order`** | **8085** | 下单成功率、支付状态机转换数、Outbox 待发积压量 |
| **`ota-merchant`** | **8086** | 商家端独立会话数、跨租户拦截阻断数 |
| **`ota-admin`** | **8087** | 中台管理 API 吞吐、人工补偿触发频次 |
| **`ota-ai`** | **8088** | 智能大模型调用延时、Token 消耗统计 |
| **`ota-supplier`** | **8089** | 上游供应接口 RT、外部 CRS 成功率 |
| **`ota-channel`** | **8090** | 下游渠道推单失败率、Kafka 消费 Lag |

---

## 三、核心告警规则矩阵（`alerts.yml`）

系统生产配置严格落地如下 8 条核心 Alertmanager 告警规则：

| 规则名称 (Alert) | 级别 (Severity) | 触发条件 (PromQL 表达式) | 持续时间 | 业务排查指引 |
| :--- | :--- | :--- | :--- | :--- |
| **`ServiceDown`** | `critical` | `up == 0` | 2m | 某微服务实例下线或宕机，需立即排查进程 |
| **`HighHttpErrorRate`**| `warning` | `rate(http_server_requests_seconds_count{status=~"5.."}[5m]) > 0.5` | 5m | 5xx 错误持续激增，关注下游服务异常 |
| **`JVMHeapHigh`** | `warning` | `jvm_memory_used_bytes{area="heap"} / jvm_memory_max_bytes > 0.85` | 5m | 堆内存超过 85%，排查内存泄漏或扩大 Xmx |
| **`ChannelPushFailureRate`** | `warning` | 渠道推单失败率 > 10% | 5m | 外部 OTA 平台接口抖动或凭据失效 |
| **`KafkaConsumerBacklog`** | `warning` | `ota_channel_kafka_consume_lag > 1000` | 5m | 渠道消费者积压，推单出现显著延迟 |
| **`OutboxBacklog`** | `warning` | `ota_order_outbox_pending_total > 500`| 5m | 订单发件箱积压，排查 Kafka 集群连通性 |

---

## 四、关键落地技术细节

### 1. Prometheus 全局抓取配置（`prometheus/prometheus.yml`）

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  - alerts.yml

alerting:
  alertmanagers:
    - static_configs:
        - targets: ["alertmanager:9093"]

scrape_configs:
  - job_name: ota-order
    metrics_path: /actuator/prometheus
    static_configs:
      - targets: ["host.docker.internal:8085"]
        labels:
          application: ota-order
```

### 2. Spring Boot 暴露与自定义业务指标定义

在订单服务中注册 Gauge 监控待发送发件箱积压：

```java
package com.ota.cloud.order.metric;

import com.ota.cloud.order.outbox.mapper.OutboxMapper;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class OrderBusinessMetrics {

    private final MeterRegistry meterRegistry;
    private final OutboxMapper outboxMapper;

    @PostConstruct
    public void bindMetrics() {
        // 动态注册 Gauge 指标，供 Prometheus 轮询计算
        Gauge.builder("ota.order.outbox.pending.total", outboxMapper, mapper -> mapper.countPendingEvents())
                .description("订单发件箱待发送事件积压总量")
                .register(meterRegistry);
    }
}
```

---

## 五、基础设施与运维部署

- **组件版本矩阵**：
  - Prometheus: `prom/prometheus:v3.2.1`（端口 9090）
  - Grafana: `grafana/grafana:11.5.2`（端口 3000）
  - Alertmanager: `prom/alertmanager:v0.27.0`（端口 9093）
- **访问入口**：
  - Prometheus 控制台：[http://localhost:9090](http://localhost:9090)
  - Grafana 监控面板：[http://localhost:3000](http://localhost:3000)（默认：`admin` / `admin`）
  - Alertmanager 告警中心：[http://localhost:9093](http://localhost:9093)
