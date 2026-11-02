# ota-cloud Kafka 全景架构与生产实践指南

> **定位**：系统性梳理 `ota-cloud` 中 Kafka 作为**分布式事务最终一致性事件总线（Event Bus）**的完整架构设计、Topic 矩阵、微服务分工、跨进程链路追踪以及监控告警体系。

---

## 一、架构定位与设计模式：Transactional Outbox

在分布式微服务架构中，直接在业务事务中调用 `kafkaTemplate.send()` 会带来典型的“双写一致性”风险：
- 若先写数据库再发 Kafka：可能数据库提交成功，但发 MQ 时因网络抖动超时，导致消息丢失；
- 若先发 Kafka 再写数据库：可能 MQ 发送成功，但本地数据库事务抛出异常回滚，导致下游消费了不存在的数据（脏数据）。

```java
// Transactional Outbox 事务发件箱模式
@Transactional
public void payOrder(String orderId) {
    // 1. 同一本地事务中更新业务主表
    orderMapper.updateStatus(orderId, "PAID");
    // 2. 同一本地事务中持久化待发送事件到 outbox_event（状态 PENDING）
    outboxService.saveEvent("order-paid", orderId, new OrderPaidEvent(orderId));
    // 3. 事务安全提交后，由后台独立任务可靠异步投递，失败自动重试
}
```

---

## 二、微服务拓扑与角色分工

```
                 【ota-order 订单中心 (8085)】
                  本地事务写入 orders + outbox_event
                                 │
                            OutboxSendJob
                                 │ (KafkaTemplate.send)
                                 ▼
                     ┌───────────────────────┐
                     │     Kafka Cluster     │
                     │  Topic: order-paid    │
                     │  Topic: order-cancelled│
                     └───────────┬───────────┘
                                 │
         ┌───────────────────────┴───────────────────────┐
         ▼ (订阅消费)                                    ▼ (独立消费者组订阅)
【ota-order 内部消费者】                         【ota-channel 渠道分发服务 (8090)】
• OrderPaidEventConsumer (状态收敛/归档)         • OrderPaidEventConsumer (向美团/携程推单出票)
• OrderCancelledEventConsumer (内部取消)         • OrderCancelledEventConsumer (向渠道同步退订)
• OrderDeadLetterConsumer (死信告警)
```

1. **`ota-order`（订单中心，端口 8085）**：
   - **生产者**：在订单支付与取消时持久化事件，通过 `OutboxSendJob` 定时（默认 5s）批量投递 Kafka。
   - **内部消费者**：监听 `order-paid` 与 `order-cancelled` 驱动订单内部归档；监听死信队列记录异常告警。
2. **`ota-channel`（下游分发渠道，端口 8090 - 第 11 章）**：
   - **跨服务消费者（独立 Consumer Group `ota-channel-service`）**：订阅 `order-paid` 自动向外部渠道（携程、美团、飞猪等）推单出票；订阅 `order-cancelled` 执行下游渠道退单。

---

## 三、核心 Topic 矩阵

系统在 Kafka 中规划并落地的核心 Topic 如下：

| Topic 名称 | 生产者 (Producer) | 消费者组 (Consumer Group) | 触发时机与核心业务作用 |
| :--- | :--- | :--- | :--- |
| **`order-paid`** | `ota-order`<br>([OutboxSendJob.java](file:///Users/fuxinwei/work/ota-cloud/ota-order/src/main/java/com/ota/cloud/order/outbox/OutboxSendJob.java)) | • `ota-order-service`<br>• `ota-channel-service` | **订单支付成功事件**：<br>1. 广播通知订单服务做内部状态收敛；<br>2. 驱动下游渠道服务（`ota-channel`）异步执行渠道推单（Confirm/Push）与外部集团出票。 |
| **`order-cancelled`** | `ota-order`<br>([OutboxSendJob.java](file:///Users/fuxinwei/work/ota-cloud/ota-order/src/main/java/com/ota/cloud/order/outbox/OutboxSendJob.java)) | • `ota-order-service`<br>• `ota-channel-service` | **订单取消/超时未支付事件**：<br>1. 广播通知库存中心释放预占房量（Frozen ➔ Available）；<br>2. 通知下游分销渠道执行订单取消操作。 |
| **`order-paid-dlq`** | Kafka 错误重试处理器 | • `ota-order`<br>([OrderDeadLetterConsumer.java](file:///Users/fuxinwei/work/ota-cloud/ota-order/src/main/java/com/ota/cloud/order/consumer/OrderDeadLetterConsumer.java)) | **支付事件死信队列**：消费重试耗尽后的隔离队列，避免阻塞正常业务分区，并记录错误日志告警。 |
| **`order-cancelled-dlq`** | Kafka 错误重试处理器 | • `ota-order`<br>([OrderDeadLetterConsumer.java](file:///Users/fuxinwei/work/ota-cloud/ota-order/src/main/java/com/ota/cloud/order/consumer/OrderDeadLetterConsumer.java)) | **取消事件死信队列**：取消消息消费失败的隔离队列。 |

---

## 四、关键落地技术细节

### 1. 异步生产者：Outbox 定时可靠投递

[OutboxSendJob.java](file:///Users/fuxinwei/work/ota-cloud/ota-order/src/main/java/com/ota/cloud/order/outbox/OutboxSendJob.java) 实现了基于数据库轮询的可靠发送，投递成功标记为 `SENT`，投递失败时累加重试计数与异常根因：

```java
package com.ota.cloud.order.outbox;

import com.ota.cloud.order.outbox.entity.OutboxEvent;
import com.ota.cloud.order.outbox.mapper.OutboxMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "outbox", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OutboxSendJob {

    private final OutboxMapper outboxMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(initialDelayString = "${outbox.send-job.initial-delay:5000}",
            fixedDelayString = "${outbox.send-job.fixed-delay:5000}")
    public void sendPendingEvents() {
        List<OutboxEvent> events = outboxMapper.selectPending(50);
        for (OutboxEvent event : events) {
            try {
                kafkaTemplate.send(event.getTopic(), event.getEventKey(), event.getPayload())
                        .get(5, TimeUnit.SECONDS);
                outboxMapper.markSent(event.getId());
            } catch (Exception e) {
                String message = rootMessage(e);
                outboxMapper.incrementRetry(event.getId(), message);
                log.warn("订单 outbox 投递失败，eventId={}, topic={}, key={}",
                        event.getId(), event.getTopic(), event.getEventKey(), e);
            }
        }
    }

    private String rootMessage(Exception e) {
        Throwable root = e.getCause() == null ? e : e.getCause();
        String message = root.getMessage();
        if (message == null || message.isBlank()) {
            return root.getClass().getSimpleName();
        }
        return message.length() > 512 ? message.substring(0, 512) : message;
    }
}
```

### 2. 全链路 TraceId 跨进程透传（第 12 章可观测性）

在分布式异步场景下，如果不显式透传链路上下文，消费端打印的日志将失去 `traceId`，无法在 ELK / Kibana 中与前端用户的 HTTP 请求关联。

- **发送端保存**：在 [OutboxService.java](file:///Users/fuxinwei/work/ota-cloud/ota-order/src/main/java/com/ota/cloud/order/outbox/OutboxService.java) 中抓取 MDC 的 `traceId` 保存至事件体。
- **消费端恢复**：在 [OrderPaidEventConsumer.java](file:///Users/fuxinwei/work/ota-cloud/ota-channel/src/main/java/com/ota/cloud/channel/consumer/OrderPaidEventConsumer.java) 中解析事件并回填 MDC：

```java
// 消费端恢复 TraceId 示例
@KafkaListener(topics = "order-paid", groupId = "${spring.kafka.consumer.group-id:ota-channel-service}")
public void onMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
    OrderPaidEvent event = parseEvent(record.value());
    // 恢复 MDC 上下文
    if (event != null && StringUtils.hasText(event.getTraceId())) {
        MDC.put("traceId", event.getTraceId());
    }
    try {
        channelOrderService.confirmByOrderId(event.getOrderId());
        ack.acknowledge(); // 手动 ACK 确认
    } finally {
        MDC.remove("traceId");
    }
}
```

### 3. 手动应答与死信隔离（Manual ACK & DLQ）

- 消费者关闭自动位移提交（`enable-auto-commit: false`），采用 Spring Kafka 的 `AckMode.MANUAL_IMMEDIATE`。
- 业务处理成功后才执行 `ack.acknowledge()`，防止服务宕机导致消息丢弃。
- 重试达到上限的消息路由至 `-dlq` 死信主题，由 `OrderDeadLetterConsumer` 统一记录持久化并触发人工排查，绝不阻塞正常业务分区。

---

## 五、监控指标与告警规则

### 1. Prometheus 业务指标
- `ota.order.outbox.pending`（Gauge）：统计 `outbox_event` 表中待发送消息积压总数。
- `ota_channel_kafka_consume_lag`（Gauge）：统计下游渠道服务针对特定 Topic 的消费 Lag 延迟。

### 2. Alertmanager 告警规则配置（`prometheus/alerts.yml`）

```yaml
- alert: KafkaConsumerBacklog
  expr: ota_channel_kafka_consume_lag{topic="order-paid"} > 1000
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Kafka 消费端积压严重 (topic={{ $labels.topic }})"
    description: "渠道分发服务消费 order-paid 积压超过 1000 条并持续 5 分钟，可能导致渠道推单延迟。"

- alert: OutboxBacklog
  expr: ota_order_outbox_pending_total > 500
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "订单 Outbox 事件积压"
    description: "本地事务发件箱未发送事件数持续超过 500 条，需排查 Kafka 集群连通性。"
```

---

## 六、基础设施与运维

### 1. Kafka 集群容器规范
- **镜像**：`apache/kafka:3.9.0`（KRaft 模式单节点，内置元数据管理，彻底摆脱 ZooKeeper 依赖）。
- **网络与端口**：
  - 容器内部端口：`9092`
  - 外部访问端口：`29092`（`100.99.175.108:29092`）

### 2. 可视化管理控制台
- **Kafka UI**：`provectuslabs/kafka-ui:v0.7.2`
- **访问入口**：[http://localhost:18081](http://localhost:18081)（本地 SSH 隧道）或 `http://100.99.175.108:18081`。
- **功能**：实时查看 4 个 Topic 的消息详情、Consumer Groups 消费进度与分区分配。
