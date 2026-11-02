# 第12章：可观测性与监控告警 (Observability)

> **目标**：全面建设生产就绪的微服务可观测性与自动化告警底座。基于 Micrometer 统一业务指标埋点框架、搭建 Prometheus 抓取矩阵、配置 Alertmanager 8 条核心业务告警规则、定制 Grafana 4 大业务大盘，并实现 ELK 集中式日志与全链路 TraceId 跨进程/跨 Kafka 异步透传闭环。

---

## 12.1 可观测性体系整体架构

在由 11 个微服务构成的分布式系统中，排障不能依赖单机登录 `tail -f` 日志，必须建立包含度量、看板、告警、追踪和集中日志的一体化可观测性闭环：

```
                    ┌───────────────────────────────────────────────┐
                    │               客户端 / 外部请求               │
                    └───────────────────────┬───────────────────────┘
                                            │ 注入 TraceId
                                            ▼
                    ┌───────────────────────────────────────────────┐
                    │             ota-gateway 统一网关              │
                    └───────┬───────────────────────────────┬───────┘
                            │                               │
            ┌───────────────┘                               └───────────────┐
            ▼ (HTTP 调用携带 TraceId)                                         ▼ (异步 Kafka 透传 TraceId)
┌───────────────────────┐                                       ┌───────────────────────┐
│ 各核心业务微服务 (808x)│                                       │   ota-channel (8090)  │
│ Micrometer 埋点指标   │                                       │ 从 Record 恢复 TraceId │
└───────────┬───────────┘                                       └───────────┬───────────┘
            │                                                               │
            │ (Pull 模式抓取 /actuator/prometheus)                           │ (TCP 推送 JSON 日志)
            ▼                                                               ▼
┌───────────────────────┐                                       ┌───────────────────────┐
│  Prometheus (9090)    │                                       │   Logstash ➔ ES ➔     │
│ 8 条核心告警规则计算  │                                       │     Kibana (ELK)      │
└───────────┬───────────┘                                       └───────────────────────┘
            │
            ├───────────────────────────────┐
            ▼                               ▼
┌───────────────────────┐       ┌───────────────────────┐
│  Alertmanager (9093)  │       │    Grafana (3000)     │
│   Webhook 分级告警    │       │     4 大业务大盘      │
└───────────────────────┘       └───────────────────────┘
```

---

## 12.2 服务端口采集对账与 Prometheus 配置

为避免遗漏任何服务的监控采集，系统确立了单一来源规范（`ServicePortConstants`），所有业务服务统一暴露 `/actuator/prometheus` 端点：

```yaml
# prometheus/prometheus.yml 抓取目标配置
scrape_configs:
  - job_name: 'ota-services'
    metrics_path: '/actuator/prometheus'
    scrape_interval: 15s
    static_configs:
      - targets:
          - 'ota-gateway:8080'
          - 'ota-auth:8081'
          - 'ota-user:8082'
          - 'ota-hotel:8083'
          - 'ota-inventory:8084'
          - 'ota-order:8085'
          - 'ota-merchant:8086'
          - 'ota-admin:8087'
          - 'ota-ai:8088'
          - 'ota-supplier:8089'
          - 'ota-channel:8090'
```

---

## 12.3 业务指标统一埋点框架

微服务通过继承 `AbstractBusinessMetrics`，将业务事件（创单、支付、扣减库存、推单等）转换为多维时序指标：

```java
package com.ota.cloud.common.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 微服务通用业务指标埋点基础抽象类。
 * <p>
 * 核心设计：
 * 1. 统一接入 Micrometer 核心 API，屏蔽底层监控实现（支持输出到 Prometheus、InfluxDB 等）；
 * 2. 线程安全缓存：通过 ConcurrentHashMap 本地缓存已注册的多维标签 Counter，避免高频创建对象的 GC 开销；
 * 3. 维度动态聚合：支持以 key-value 变长可变参数传入业务标签（例如 application, status, channelCode 等）。
 */
@Slf4j
public abstract class AbstractBusinessMetrics {

    /** Micrometer 核心仪表注册中心。 */
    protected final MeterRegistry registry;

    /** 多维标签 Counter 线程安全内存高速缓存。 */
    private final ConcurrentMap<String, Counter> counterCache = new ConcurrentHashMap<>();

    /**
     * 基础构造函数：注入 Spring 容器管理的全局 MeterRegistry。
     *
     * @param registry 指标注册表
     */
    protected AbstractBusinessMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    /**
     * 获取或动态注册带有标签维度的 Counter 计数器实例。
     * <p>
     * 使用 computeIfAbsent 保证在并发场景下同一维度只向注册表绑定一次。
     *
     * @param name 指标全局唯一名称（例如 ota.order.create）
     * @param tags 变长标签键值对（必须成对传入，如 "result", "success"）
     * @return 注册完成的 Micrometer Counter 实例
     */
    protected Counter counter(String name, String... tags) {
        String key = buildCacheKey(name, tags);
        return counterCache.computeIfAbsent(key, k -> Counter.builder(name)
                .tags(tags)
                .register(registry));
    }

    /**
     * 记录指定维度的业务计数（自增 1）。
     *
     * @param name 指标名称
     * @param tags 关联维度标签
     */
    public void recordCount(String name, String... tags) {
        counter(name, tags).increment();
    }

    /**
     * 拼接指标与标签的缓存唯一复合 Key。
     *
     * @param name 指标名称
     * @param tags 标签数组
     * @return 格式化拼接后的缓存键
     */
    private String buildCacheKey(String name, String... tags) {
        StringBuilder sb = new StringBuilder(name);
        for (String tag : tags) {
            sb.append(":").append(tag);
        }
        return sb.toString();
    }
}
```

### 业务指标命名规范（`BusinessMetricsConstants`）

- **Counter 计数器**：`ota.order.create`、`ota.order.pay`、`ota.channel.push`、`ota.supplier.book`；
- **Timer 耗时**：`ota.order.create_duration`、`ota.channel.push_duration`；
- **Gauge 瞬时值**：`ota.outbox.pending`（待发送发件箱积压量）、`ota_channel_kafka_consume_lag`（消费延迟）。

---

## 12.4 Alertmanager 8 条核心业务告警规则

在 `prometheus/alerts.yml` 中固化定义了 8 条与 OTA 业务生命线直接挂钩的生产告警规则：

```yaml
groups:
  - name: ota-cloud.alerts
    rules:
      # 1. 服务心跳存活告警 (P0)
      - alert: ServiceDown
        expr: up{application=~"ota-.*"} == 0
        for: 2m
        labels: { severity: critical }
        annotations:
          summary: "微服务 {{ $labels.application }} 宕机"
          description: "服务实例下线超过 2 分钟，请立即排查容器状态。"

      # 2. HTTP 5xx 高错误率告警
      - alert: HighHttpErrorRate
        expr: sum by (application) (rate(http_server_requests_seconds_count{status=~"5.."}[5m])) > 0.5
        for: 5m
        labels: { severity: warning }
        annotations:
          summary: "服务 {{ $labels.application }} 5xx 异常激增"

      # 3. 渠道推单失败率告警
      - alert: ChannelPushFailureRate
        expr: sum by (channelCode) (rate(ota_channel_push_total{result="fail"}[5m])) / clamp_min(sum by (channelCode) (rate(ota_channel_push_total[5m])), 0.001) > 0.1
        for: 5m
        labels: { severity: warning }
        annotations:
          summary: "渠道 {{ $labels.channelCode }} 推单失败率超过 10%"

      # 4. 供应商出票失败率告警
      - alert: SupplierBookFailureRate
        expr: sum by (supplierCode) (rate(ota_supplier_book_total{result="fail"}[5m])) / clamp_min(sum by (supplierCode) (rate(ota_supplier_book_total[5m])), 0.001) > 0.1
        for: 5m
        labels: { severity: warning }
        annotations:
          summary: "供应商 {{ $labels.supplierCode }} 集团锁房失败率超过 10%"

      # 5. Outbox 本地发件箱堆积告警
      - alert: OutboxBacklog
        expr: ota_outbox_pending > 500
        for: 5m
        labels: { severity: critical }
        annotations:
          summary: "本地事务发件箱积压超过 500 条"
          description: "可能因 Kafka 集群连通性中断导致异步投递任务阻塞。"

      # 6. JVM 堆内存高水位告警
      - alert: JVMHeapHigh
        expr: jvm_memory_used_bytes{area="heap"} / jvm_memory_max_bytes{area="heap"} > 0.85
        for: 5m
        labels: { severity: warning }
        annotations:
          summary: "服务 {{ $labels.application }} 堆内存占用超过 85%"

      # 7. Kafka 消费端 Lag 延迟告警
      - alert: KafkaConsumerBacklog
        expr: ota_channel_kafka_consume_lag{topic="order-paid"} > 1000
        for: 5m
        labels: { severity: warning }
        annotations:
          summary: "渠道服务消费 order-paid 积压超过 1000 条"

      # 8. 创单接口堆积与慢处理告警
      - alert: OrderCreateBacklog
        expr: rate(ota_order_create_duration_seconds_count[5m]) > 100
        for: 3m
        labels: { severity: warning }
        annotations:
          summary: "订单创建吞吐过高，需关注库存锁竞争"
```

---

## 12.5 ELK 日志与全链路 TraceId 透传闭环

### 1. 通用 Logback 模板 (`ota-common/logback-base.xml`)

全模块采用统一的 Logback 基础配置，将日志结构化为 JSON 并通过 Logstash TCP 直推：

```xml
<configuration>
    <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level [%X{traceId:-},%X{spanId:-}] %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>

    <appender name="LOGSTASH" class="net.logstash.logback.appender.LogstashTcpSocketAppender">
        <destination>logstash:5000</destination>
        <encoder class="net.logstash.logback.encoder.LogstashEncoder">
            <customFields>{"app":"${spring.application.name}"}</customFields>
        </encoder>
    </appender>

    <root level="INFO">
        <appender-ref ref="CONSOLE" />
        <appender-ref ref="LOGSTASH" />
    </root>
</configuration>
```

### 2. Kafka 跨进程异步 TraceId 传递与恢复

- **生产者端写入**：在 `OutboxService` 将事件写入数据库时，抓取 MDC 中的 `traceId` 保存进事件 Payload；
- **消费者端恢复**：在 `OrderPaidEventConsumer` 监听消费时解析 `traceId` 并回填 MDC：

```java
String traceId = event.getTraceId();
if (StringUtils.hasText(traceId)) {
    MDC.put("traceId", traceId);
}
try {
    channelOrderService.confirmByOrderId(orderId);
} finally {
    MDC.remove("traceId");
}
```

---

## 12.6 快速验证

```bash
# 执行第 12 章可观测性 4 个维度全流程冒烟（指标抓取对账、Logstash 日志入库、告警规则校验、TraceId 贯通验证）
bash scripts/chapter12-smoke-all.sh
```

