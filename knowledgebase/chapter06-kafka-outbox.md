# 第6章：Kafka 事件驱动与 outbox 模式

> **对应 P0-6 (Bonus)，预计 1 天**
>
> 📖 **专题速查**：系统全模块 Kafka 的使用现状、Topic 矩阵、生产者/消费者映射与生产配置，详见 [Kafka 全景架构与生产实践指南](./kafka-architecture-guide.md)。
>
> 在订单支付和取消后，通过 Kafka 异步通知其他服务（酒店、库存、AI等）。
> 采用 **outbox 模式** 确保消息可靠投递：先写 outbox 表（本地事务），再由定时任务发送到 Kafka。
>
> **版本规划说明**：Kafka 在 P0 阶段末期即可引入（作为 P0-6 Bonus），不必等到 P1。
> 核心链路在 P0-4/P0-5 已跑通后，引入事件驱动让系统具备异步解耦能力，
> 为 P1 的服务拆分和多消费者场景打好基础。若时间紧张，P0-6 可跳过，不影响核心功能。P1 阶段会在此基础进一步增强（多消费者、死信告警、事务性 outbox）。

## 6.1 本章目标

- Docker Compose 增加 Kafka + ZooKeeper
- 4 个 Kafka Topic 的定义和说明
- outbox 模式：写入 outbox 表（本地事务）→ 定时任务重试发送 → 死信
- 事件发布：OrderPaidEvent、OrderCancelledEvent
- 事件消费：支付事件和取消事件的消费者
- 消费失败重试（固定次数）→ 死信 topic → 落库告警

> **P0-6 Bonus 说明**：本章内容不影响核心订单链路（下单→支付），可在 P0 末期或 P1 初期完成。
> 若时间紧迫，跳过后直接进入第7章也不会破坏系统可用性——核心链路的支付和取消事件
> 在 P1 阶段引入 Kafka 后会通过 outbox 定时任务自动补发，不会丢失。

## 6.2 Kafka 主题设计

### 6.2.1 主题定义

| Topic | 分区数 | 副本数 | 说明 |
|-------|--------|--------|------|
| `order-paid` | 3 | 1 | 订单支付成功事件 |
| `order-cancelled` | 3 | 1 | 订单取消事件 |
| `inventory-changed` | 3 | 1 | 库存变更事件 |
| `cache-delete-retry` | 1 | 1 | 缓存删除重试事件 |
| `order-paid-dlq` | 1 | 1 | 死信队列：支付事件消费失败 |
| `order-cancelled-dlq` | 1 | 1 | 死信队列：取消事件消费失败 |

### 6.2.2 自动创建主题

Kafka 支持自动创建主题（`auto.create.topics.enable=true`，默认开启），但生产环境推荐手动创建：

```bash
# 进入 Kafka 容器
docker exec -it ota-kafka kafka-topics --bootstrap-server localhost:9092 --create \
  --topic order-paid \
  --partitions 3 \
  --replication-factor 1

docker exec -it ota-kafka kafka-topics --bootstrap-server localhost:9092 --create \
  --topic order-cancelled \
  --partitions 3 \
  --replication-factor 1

docker exec -it ota-kafka kafka-topics --bootstrap-server localhost:9092 --create \
  --topic order-paid-dlq \
  --partitions 1 \
  --replication-factor 1

docker exec -it ota-kafka kafka-topics --bootstrap-server localhost:9092 --create \
  --topic order-cancelled-dlq \
  --partitions 1 \
  --replication-factor 1
```

查看已创建的主题：

```bash
docker exec -it ota-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

## 6.3 Docker Compose 增加 Kafka

在 `chapter-01/docker-compose.yml` 中增加 Kafka + ZooKeeper：

```yaml
# 在 services 中添加
  zookeeper:
    image: bitnami/zookeeper:3.9
    container_name: ota-zookeeper
    ports:
      - "2181:2181"
    environment:
      - ALLOW_ANONYMOUS_LOGIN=yes
      - ZOO_4LW_COMMANDS_WHITELIST=*
    volumes:
      - zk-data:/bitnami/zookeeper

  kafka:
    image: bitnami/kafka:3.9
    container_name: ota-kafka
    ports:
      - "9092:9092"
      - "9093:9093"
    environment:
      - KAFKA_CFG_NODE_ID=1
      - KAFKA_CFG_PROCESS_ROLES=broker,controller
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@localhost:9093
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT
      - KAFKA_CFG_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,INTERNAL://0.0.0.0:29092
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092,INTERNAL://ota-kafka:29092
      - KAFKA_CFG_INTER_BROKER_LISTENER_NAME=INTERNAL
      - ALLOW_PLAINTEXT_LISTENER=yes
      - KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true
      - KAFKA_CFG_DELETE_TOPIC_ENABLE=true
      - KAFKA_CFG_LOG_RETENTION_HOURS=72
      - KAFKA_CFG_LOG_RETENTION_BYTES=1073741824  # 1GB
    volumes:
      - kafka-data:/bitnami/kafka
    depends_on:
      - zookeeper

volumes:
  zk-data:
  kafka-data:
```

> **注意**：使用 Bitnami 镜像而非 Confluent 镜像，因为 Bitnami 镜像的配置方式更简洁，且对 KRaft 模式（去掉 ZooKeeper）支持更好。这里保留 ZooKeeper 以便管理。

## 6.4 Outbox 表设计

### 6.4.1 outbox_event 表 DDL

```sql
CREATE TABLE `outbox_event` (
  `id`          BIGINT       NOT NULL AUTO_INCREMENT COMMENT '主键',
  `topic`       VARCHAR(128) NOT NULL COMMENT 'Kafka 主题',
  `key`         VARCHAR(128) NOT NULL DEFAULT '' COMMENT '消息Key（用于分区）',
  `payload`     JSON         NOT NULL COMMENT '消息体（JSON格式）',
  `status`      VARCHAR(16)  NOT NULL DEFAULT 'PENDING' COMMENT '状态：PENDING/SENT/FAILED',
  `retry_count` INT          NOT NULL DEFAULT 0 COMMENT '已重试次数',
  `max_retry`   INT          NOT NULL DEFAULT 3 COMMENT '最大重试次数',
  `error_msg`   VARCHAR(512) NOT NULL DEFAULT '' COMMENT '最后一次错误信息',
  `created_at`  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_status` (`status`),
  KEY `idx_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Outbox事件表（可靠消息投递）';
```

**设计说明**：

- `payload` 使用 JSON 类型——MySQL 8.0+ 原生支持，可读性好
- `status` 用于追踪消息生命周期：PENDING → SENT / FAILED
- `retry_count` 和 `max_retry` 配合定时任务控制重试次数
- `key` 用于 Kafka 消息的分区路由（相同 key 的消息进入同一分区，保证顺序）

### 6.4.2 状态流转

```
PENDING ──发送成功──▶ SENT
   │
   └──重试超过 max_retry ──▶ FAILED（告警人工处理）
```

## 6.5 项目依赖

在 `ota-order/pom.xml` 中增加 Kafka 依赖：

```xml
<!-- Kafka -->
<dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka</artifactId>
</dependency>

<!-- Jackson JSON（Spring Boot 已自带，此处仅用于 outbox 序列化工具类） -->
<!-- 无需额外依赖 -->
```

## 6.6 配置

### 6.6.1 application.yml（Kafka 配置）

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092

    # 生产者配置
    producer:
      acks: all                    # 等待所有副本确认，最强一致性
      retries: 3                   # 发送重试次数
      properties:
        enable.idempotence: true   # 启用幂等性（防止重复消息）
        max.in.flight.requests.per.connection: 5
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer

    # 消费者配置
    consumer:
      enable-auto-commit: false    # 手动提交偏移量
      auto-offset-reset: earliest  # 从最早的消息开始消费
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "*"

    # 监听器（消费者）配置
    listener:
      ack-mode: manual_immediate   # 手动确认，处理完后立即提交
      missing-topics-fatal: false  # 启动时 topic 不存在不报错

# Outbox 定时任务配置
outbox:
  job:
    initial-delay: 5000    # 首次延迟 5 秒
    fixed-delay: 5000      # 每 5 秒扫描一次
    batch-size: 50         # 每次最多处理 50 条
  dead-letter:
    topic-suffix: -dlq     # 死信 topic 后缀
```

## 6.7 事件模型

```java
package com.ota.order.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * 订单支付事件
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderPaidEvent {
    private String orderId;
    private Long userId;
    private Long hotelId;
    private String roomType;
    private LocalDate checkIn;
    private LocalDate checkOut;
    private BigDecimal totalPrice;
    private String paidAt;
}

/**
 * 订单取消事件
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderCancelledEvent {
    private String orderId;
    private Long userId;
    private Long hotelId;
    private String roomType;
    private String reason;
    private LocalDate checkIn;
    private LocalDate checkOut;
    private String cancelledAt;
}
```

## 6.8 Outbox 服务

### 6.8.1 OutboxEvent 实体

```java
package com.ota.order.outbox.entity;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@TableName("outbox_event")
public class OutboxEvent {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String topic;

    private String key;

    private String payload;

    private String status;

    private Integer retryCount;

    private Integer maxRetry;

    private String errorMsg;

    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createdAt;

    @TableField(fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updatedAt;
}
```

### 6.8.2 OutboxMapper

```java
package com.ota.order.outbox.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.ota.order.outbox.entity.OutboxEvent;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

@Mapper
public interface OutboxMapper extends BaseMapper<OutboxEvent> {

    /**
     * 获取待发送的消息（PENDING + retry_count < max_retry）
     */
    @Select("SELECT * FROM outbox_event WHERE status = 'PENDING' " +
            "AND retry_count < max_retry " +
            "ORDER BY created_at ASC LIMIT #{limit}")
    List<OutboxEvent> selectPending(@Param("limit") int limit);

    /**
     * 标记为已发送
     */
    @Update("UPDATE outbox_event SET status = 'SENT', updated_at = NOW() WHERE id = #{id}")
    int markSent(@Param("id") Long id);

    /**
     * 增加重试次数
     */
    @Update("UPDATE outbox_event SET retry_count = retry_count + 1, " +
            "error_msg = #{errorMsg}, updated_at = NOW() " +
            "WHERE id = #{id}")
    int incrementRetry(@Param("id") Long id, @Param("errorMsg") String errorMsg);

    /**
     * 获取失败的记录（重试耗尽）
     */
    @Select("SELECT * FROM outbox_event WHERE status = 'PENDING' " +
            "AND retry_count >= max_retry " +
            "ORDER BY updated_at DESC LIMIT #{limit}")
    List<OutboxEvent> selectFailed(@Param("limit") int limit);
}
```

### 6.8.3 OutboxService

```java
package com.ota.order.outbox.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ota.order.outbox.entity.OutboxEvent;
import com.ota.order.outbox.mapper.OutboxMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class OutboxService {

    private final OutboxMapper outboxMapper;
    private final ObjectMapper objectMapper;

    /**
     * 保存事件到 outbox 表（与业务操作在同一个本地事务中）
     *
     * @param topic   Kafka 主题
     * @param key     消息 key（用于分区路由）
     * @param payload 消息体（任何可 JSON 序列化的对象）
     */
    @Transactional(rollbackFor = Exception.class)
    public void saveEvent(String topic, String key, Object payload) {
        try {
            String payloadJson = objectMapper.writeValueAsString(payload);

            OutboxEvent event = new OutboxEvent();
            event.setTopic(topic);
            event.setKey(key);
            event.setPayload(payloadJson);
            event.setStatus("PENDING");
            event.setRetryCount(0);
            event.setMaxRetry(3);

            outboxMapper.insert(event);
            log.debug("Outbox事件已保存：topic={}, key={}", topic, key);
        } catch (Exception e) {
            log.error("保存Outbox事件异常", e);
            throw new RuntimeException("保存事件失败", e);
        }
    }
}
```

## 6.9 Outbox 发送定时任务

```java
package com.ota.order.outbox.job;

import com.ota.order.outbox.entity.OutboxEvent;
import com.ota.order.outbox.mapper.OutboxMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Outbox 消息发送任务
 *
 * 定时扫描 outbox 表中 PENDING 状态的消息，发送到 Kafka。
 * 发送成功后标记为 SENT，失败后增加重试次数。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxSendJob {

    private final OutboxMapper outboxMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${outbox.job.batch-size:50}")
    private int batchSize;

    /**
     * 每5秒扫描一次待发送消息
     */
    @Scheduled(initialDelayString = "${outbox.job.initial-delay:5000}",
               fixedDelayString = "${outbox.job.fixed-delay:5000}")
    public void sendPendingEvents() {
        List<OutboxEvent> pendingEvents = outboxMapper.selectPending(batchSize);

        if (pendingEvents.isEmpty()) {
            return;
        }

        log.info("Outbox发送任务：发现 {} 条待发送消息", pendingEvents.size());

        for (OutboxEvent event : pendingEvents) {
            try {
                // 发送 Kafka 消息
                CompletableFuture<SendResult<String, String>> future =
                        kafkaTemplate.send(event.getTopic(), event.getKey(), event.getPayload());

                // 同步等待发送结果（使用虚拟线程，阻塞开销低）
                future.join();

                // 标记已发送
                outboxMapper.markSent(event.getId());
                log.debug("Outbox消息已发送：id={}, topic={}", event.getId(), event.getTopic());
            } catch (Exception e) {
                log.warn("Outbox消息发送失败：id={}, topic={}, error={}",
                        event.getId(), event.getTopic(), e.getMessage());

                outboxMapper.incrementRetry(event.getId(),
                        e.getClass().getSimpleName() + ": " + e.getMessage());
            }
        }
    }
}
```

## 6.10 事件发布（在订单 Service 中集成）

修改 `OrderServiceImpl`，在支付和取消操作后发布事件：

### 6.10.1 在支付成功后发布 OrderPaidEvent

```java
// 在 payOrder 方法中，扣减库存成功后，发布支付事件
import com.ota.order.event.OrderPaidEvent;
import com.ota.order.outbox.service.OutboxService;

// 注入
private final OutboxService outboxService;

// 在扣减库存成功、状态更新为 PAID 后：
// 发布 OrderPaidEvent
OrderPaidEvent paidEvent = OrderPaidEvent.builder()
        .orderId(order.getId())
        .userId(order.getUserId())
        .hotelId(order.getHotelId())
        .roomType(order.getRoomType())
        .checkIn(order.getCheckIn())
        .checkOut(order.getCheckOut())
        .totalPrice(order.getTotalPrice())
        .paidAt(java.time.LocalDateTime.now().toString())
        .build();

outboxService.saveEvent("order-paid", order.getId(), paidEvent);
```

### 6.10.2 在取消后发布 OrderCancelledEvent

```java
// 在 cancelOrder 方法中，变更状态为 CANCELLED 后：
import com.ota.order.event.OrderCancelledEvent;

OrderCancelledEvent cancelledEvent = OrderCancelledEvent.builder()
        .orderId(order.getId())
        .userId(order.getUserId())
        .hotelId(order.getHotelId())
        .roomType(order.getRoomType())
        .reason(order.getErrorMsg())
        .checkIn(order.getCheckIn())
        .checkOut(order.getCheckOut())
        .cancelledAt(java.time.LocalDateTime.now().toString())
        .build();

outboxService.saveEvent("order-cancelled", order.getId(), cancelledEvent);
```

## 6.11 事件消费者

### 6.11.1 订单支付事件消费者

```java
package com.ota.order.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ota.order.event.OrderPaidEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * 订单支付事件消费者
 *
 * 支付事件触发后的业务动作：
 * - 库存服务：将 frozen 转为 booked（已由扣减接口完成）
 * - 酒店服务：更新酒店热度/销量统计
 * - AI 服务：触发个性化推荐计算
 * - 通知服务：发送确认短信/邮件（预留）
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderPaidEventConsumer {

    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "order-paid",
            groupId = "ota-order-paid-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            OrderPaidEvent event = objectMapper.readValue(record.value(), OrderPaidEvent.class);

            log.info("收到订单支付事件：orderId={}, userId={}, amount={}",
                    event.getOrderId(), event.getUserId(), event.getTotalPrice());

            // TODO: 处理支付事件（更新销量统计、发送通知等）

            // 手动确认
            ack.acknowledge();
        } catch (Exception e) {
            log.error("处理订单支付事件失败：record={}", record.value(), e);
            // 不 ack，让重试机制处理
            throw new RuntimeException("消费支付事件失败", e);
        }
    }
}
```

### 6.11.2 订单取消事件消费者

```java
package com.ota.order.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ota.order.event.OrderCancelledEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderCancelledEventConsumer {

    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "order-cancelled",
            groupId = "ota-order-cancelled-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            OrderCancelledEvent event = objectMapper.readValue(record.value(), OrderCancelledEvent.class);

            log.info("收到订单取消事件：orderId={}, reason={}",
                    event.getOrderId(), event.getReason());

            // TODO: 处理取消事件（更新酒店统计、释放优惠券等）

            ack.acknowledge();
        } catch (Exception e) {
            log.error("处理订单取消事件失败：record={}", record.value(), e);
            throw new RuntimeException("消费取消事件失败", e);
        }
    }
}
```

## 6.12 死信队列（DLQ）处理

### 6.12.1 死信消费者

当业务消费者连续失败超过重试次数后，Spring Kafka 会自动将消息转发到死信 topic。我们需要一个专门的消费者来处理死信消息。

```java
package com.ota.order.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ota.order.event.OrderPaidEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * 死信队列消费者
 *
 * 死信消息不会重试，直接记录告警日志并持久化到数据库。
 * 需要人工介入排查问题。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DeadLetterConsumer {

    private final ObjectMapper objectMapper;

    /**
     * 消费 order-paid 的死信
     */
    @KafkaListener(
            topics = "order-paid-dlq",
            groupId = "ota-dlq-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumePaidDlq(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            log.error("🔥 [死信] 订单支付事件消费失败超过重试次数，需要人工处理！");
            log.error("🔥 [死信] Topic: order-paid-dlq, Key: {}, Value: {}",
                    record.key(), record.value());

            // 将死信消息持久化到告警表（存入死信日志）
            // TODO: insert into alert_log(topic, key, payload, error, created_at)

            ack.acknowledge();
        } catch (Exception e) {
            log.error("处理死信消息异常", e);
            // 死信处理也要 ack，不要再重试死信本身
            ack.acknowledge();
        }
    }

    /**
     * 消费 order-cancelled 的死信
     */
    @KafkaListener(
            topics = "order-cancelled-dlq",
            groupId = "ota-dlq-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeCancelledDlq(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            log.error("🔥 [死信] 订单取消事件消费失败超过重试次数，需要人工处理！");
            log.error("🔥 [死信] Topic: order-cancelled-dlq, Key: {}, Value: {}",
                    record.key(), record.value());

            // TODO: 持久化告警

            ack.acknowledge();
        } catch (Exception e) {
            log.error("处理死信消息异常", e);
            ack.acknowledge();
        }
    }
}
```

### 6.12.2 Kafka 配置类（重试 + 死信）

```java
package com.ota.order.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.CommonLoggingErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Slf4j
@Configuration
public class KafkaConfig {

    /**
     * 配置 Kafka 监听器容器工厂
     * - 手动提交偏移量
     * - 消费失败重试3次，间隔1秒，超过进入死信
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        // 手动确认模式
        factory.getContainerProperties().setAckMode(
                org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // 错误处理器：重试3次，间隔1秒
        FixedBackOff backOff = new FixedBackOff(1000L, 3);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                (consumerRecord, exception) -> {
                    log.error("消息消费失败，已超过重试次数，进入死信: topic={}, key={}",
                            consumerRecord.topic(), consumerRecord.key(), exception);
                },
                backOff);

        // 死信 topic 自动命名：原 topic + ".DLT"（Spring Kafka 默认规则）
        // 我们使用自定义后缀 -dlq，通过配置实现
        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);
        factory.setCommonErrorHandler(errorHandler);

        // 并发消费者数
        factory.setConcurrency(3);

        return factory;
    }
}
```

> **关于死信 topic 命名**：Spring Kafka 默认死信 topic 为 `原Topic.DLT`（如 `order-paid.DLT`）。
> 我们手动创建了 `order-paid-dlq` 后缀的死信 topic。要使用自定义后缀，可以自定义 `DeadLetterPublishingRecoverer`。
> 简单起见，本章直接让消费失败的消息进入默认的 `.DLT` topic，并再创建一个消费者监听该 topic。
> 更常见的做法是：在 `DefaultErrorHandler` 的 recoverer 中指定死信 topic。

## 6.13 完整的顺序图

```
OrderService                  OutboxService            OutboxSendJob          Kafka               Consumer
    │                              │                       │                    │                    │
    │ 1. 支付/取消                  │                       │                    │                    │
    │─────────────────────────────▶│                       │                    │                    │
    │                              │                       │                    │                    │
    │ 2. INSERT outbox_event       │                       │                    │                    │
    │   (同一本地事务)              │                       │                    │                    │
    │◀─────────────────────────────│                       │                    │                    │
    │                              │                       │                    │                    │
    │                              │  3. @Scheduled(5s)    │                    │                    │
    │                              │──────────────────────▶│                    │                    │
    │                              │                       │                    │                    │
    │                              │                       │ 4. send()          │                    │
    │                              │                       │───────────────────▶│                    │
    │                              │                       │                    │                    │
    │                              │                       │ 5. ack             │ 6. Listener        │
    │                              │                       │◀───────────────────│───────────────────▶│
    │                              │                       │                    │                    │
    │                              │                       │ 7. markSent()      │                    │
    │                              │                       │                    │                    │
    │                              │                       │ 7. 失败→重试+1     │                    │
    │                              │                       │                    │                    │
    │                              │                       │ 超过max_retry      │ 8. DLQ             │
    │                              │                       │ 死信标记           │───────────────────▶│
```

## 6.14 验证步骤

### 6.14.1 启动基础设施

```bash
# 启动所有服务（包含 Kafka + ZooKeeper）
cd chapter-01
docker compose down -v    # 清理旧数据
docker compose up -d

# 等待 Kafka 就绪
sleep 10
docker logs ota-kafka 2>&1 | grep "started"
```

### 6.14.2 创建 Kafka Topic

```bash
docker exec -it ota-kafka kafka-topics --bootstrap-server localhost:9092 --create \
  --topic order-paid --partitions 3 --replication-factor 1

docker exec -it ota-kafka kafka-topics --bootstrap-server localhost:9092 --create \
  --topic order-cancelled --partitions 3 --replication-factor 1

docker exec -it ota-kafka kafka-topics --bootstrap-server localhost:9092 --create \
  --topic order-paid-dlq --partitions 1 --replication-factor 1

docker exec -it ota-kafka kafka-topics --bootstrap-server localhost:9092 --create \
  --topic order-cancelled-dlq --partitions 1 --replication-factor 1
```

### 6.14.3 插入 outbox 表 DDL

```bash
mysql -h127.0.0.1 -uroot -proot123456 ota_order < chapter-06/sql/init.sql
```

### 6.14.4 启动服务并测试

```bash
# 启动 inventory 和 order 服务
# 确保 order 服务已集成 outbox 和 Kafka 配置

# 1. 创建订单（外部演示统一走 Gateway；先通过第2章登录拿到 TOKEN）
curl -X POST http://localhost:8080/api/order \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"hotelId":1,"roomType":"高级大床房","checkIn":"2026-06-01","checkOut":"2026-06-03","quantity":1}'

# 拿到 orderId
# 2. 支付
curl -X POST http://localhost:8080/api/order/pay \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"orderId":"<从上面获取的orderId>"}'
```

### 6.14.5 验证 Kafka 消息

```bash
# 启动 Kafka 控制台消费者，查看 order-paid topic 的消息
docker exec -it ota-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic order-paid \
  --from-beginning

# 或查看死信 topic
docker exec -it ota-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic order-paid-dlq \
  --from-beginning
```

### 6.14.6 验证 outbox 表

```sql
-- 查看 outbox 表中的消息状态
SELECT id, topic, `key`, `status`, retry_count, error_msg, created_at
FROM outbox_event
ORDER BY id DESC;

-- 正常情况：支付和取消事件 status = 'SENT'
-- 失败情况：status = 'PENDING' 且 retry_count >= max_retry
```

### 6.14.7 测试重试和死信

```bash
# 临时停掉 order 服务（模拟消费者不可用）
# 发起支付操作，等待 outbox 定时任务发送消息到 Kafka
# 重新启动 order 服务，观察消费者是否消费到堆积的消息
# 如果消费者持续异常，消息最终进入死信 topic
```

## 6.15 高级话题：Kafka 事务性 outbox

本章使用了**定时轮询 outbox 表**的方式，这是最简单可靠的方案。对于更高吞吐的场景，也可以使用 **Kafka 事务性 outbox**（Exactly-Once Semantics）：

```yaml
spring.kafka.producer:
  acks: all
  properties:
    enable.idempotence: true
    transactional.id: ota-order-outbox-${random.value}
```

然后在 outbox 发送时开启事务：

```java
kafkaTemplate.executeInTransaction(operations -> {
    operations.send(topic, key, payload);
    outboxMapper.markSent(event.getId());
    return true;
});
```

但事务性 Kafka 需要额外的 Kafka 配置（`transaction.state.log.min.isr` 等），对于本章的教程场景，定时轮询方案已经足够。

## 6.16 常见坑

### 🕳️ 坑1：Kafka 默认配置的 auto.create.topics.enable 陷阱

**现象**：启动时自动创建了 topic，但分区数、副本数与预期不符（默认 1 分区 1 副本）。

**解决**：
- 生产环境禁用自动创建：`auto.create.topics.enable=false`
- 运维脚本统一创建 topic 并指定分区数
- 开发环境可以用自动创建，但需要明确主题设计

### 🕳️ 坑2：消费者 enable.auto.commit=true 导致重复消费

**现象**：消息已处理但偏移量未提交，重启后重复消费。

**解决**：
- 必须设置 `enable.auto.commit=false`
- 使用手动确认模式 `MANUAL_IMMEDIATE`
- 消费者处理完业务逻辑后立即 `ack.acknowledge()`

### 🕳️ 坑3：虚拟线程 + Kafka 消费者

**现象**：在虚拟线程模式下使用 Spring Kafka 消费者时出现线程泄漏。

**解决**：
- Spring Kafka 的消费者线程默认使用平台线程（`KafkaMessageListenerContainer` 的线程模型）
- 设置 `spring.kafka.listener.concurrency` 不要超过分区数
- 虚拟线程主要影响生产者端（`kafkaTemplate.send()` 的非阻塞调用）

### 🕳️ 坑4：Outbox 定时任务与业务事务的边界

**现象**：业务事务已回滚，但 outbox 消息已经发出。

**解决**：
- outbox 表的数据写入**必须在业务事务内**，与业务操作在同一 `@Transactional` 中
- outbox 的发送（定时任务）是独立的事务，不在业务事务内
- 确保业务事务中对 `OutboxService.saveEvent()` 的调用在 `@Transactional` 方法内

### 🕳️ 坑5：JSON 反序列化时区问题

**现象**：LocalDateTime 序列/反序列化后时区不一致。

**解决**：
- 在 `application.yml` 中统一 Jackson 配置：

```yaml
spring.jackson:
  date-format: yyyy-MM-dd HH:mm:ss
  time-zone: Asia/Shanghai
  serialization:
    write-dates-as-timestamps: false
```

- 或在事件中使用字符串（如 `2026-06-01T10:30:00`）而非 LocalDateTime 对象

---

## 6.17 面试问答

### Q1：如果让你重做这个项目，你会改什么？（关于 Kafka 的回答）

**1. Kafka 引入时机提前到 P0 末期**

原来将 Kafka 放在 P1-1，但实际项目中事件驱动可以更早引入。P0-5 完成订单核心链路后，outbox 模式不依赖 Kafka 可用性——即使 Kafka 宕机，消息积压在 outbox 表，恢复后自动补发。提前引入的好处：

- 团队更早熟悉 Kafka 运维（topic 创建、分区调整、消费滞后监控）
- 为 P1 的库存变更事件通知、缓存清除事件等基础能力先搭好通道
- 事件驱动的架构风格尽早确立，避免 P1 再大改

**2. 增加事件版本字段**

当前事件模型（OrderPaidEvent、OrderCancelledEvent）没有版本号。实际生产中事件 schema 会演进（增加字段、修改类型），可以通过 `event_version` 字段让消费者兼容处理不同版本的消息。

**3. 死信告警走独立通道**

当前死信只是落库 + 日志告警。如果 Kafka 消费大面积失败，日志可能被淹没。应该将死信告警直接接入飞书/钉钉 Webhook 或 Prometheus AlertManager，确保运维第一时间感知。

**4. Kafka 事务性 outbox 作为 P1 优化**

当前定时轮询方案在 P0 阶段足够，但存在秒级延迟。如果业务对事件投递延迟要求更高（如库存变更需实时通知其他服务），可在 P1 引入 Kafka 事务性 outbox，将延迟降到亚秒级。

### Q2：为什么 outbox 模式要定时轮询，而不是事务性消息？

P0 阶段优先考虑**简单可靠**：

| 方案 | 复杂度 | 吞吐 | 延迟 | 依赖 |
|------|--------|------|------|------|
| 定时轮询 outbox | 低 | 中 | 秒级 | 仅 MySQL |
| Kafka 事务性 outbox | 高 | 高 | 亚秒 | Kafka 事务 + MySQL |

P0 阶段每日订单量有限，秒级延迟完全可接受。定时轮询方案不依赖 Kafka 可用性（Kafka 挂了消息还在 DB），适合团队初期快速迭代。等 P1 业务量上升后，再考虑 Kafka 事务性 outbox 作为优化。

---

**下一步**：完成 Kafka 事件驱动后，进入第7章 — 进阶技术大杂烩，包括 Zipkin 链路追踪、Sentinel 限流、Seata 分布式事务、ShardingSphere 分库分表、Spring AI 推荐、Prometheus/Grafana 监控以及 ELK 日志分析。
