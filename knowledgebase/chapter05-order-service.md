# 第5章：Order 订单核心链路与状态机

> **对应 P0-4，预计 2 天**
>
> 核心业务流程：用户登录(JWT) → Gateway校验JWT → 查询酒店/价库 → **创建订单(冻结库存) → 支付(扣减库存)** → Kafka事件通知

## 5.1 本章目标

完成 `ota-order`（端口 8084）订单服务，并通过 `ota-inventory` 的幂等库存接口实现订单与库存最终一致：

- **订单表 DDL** 与实体设计
- **订单状态机**：INIT → WAIT_PAY → PAID → CONFIRMED → CHECKED_IN → COMPLETED / CANCELLED / CLOSED，以及 REFUNDING → REFUNDED (P2)
- **下单流程**：创建订单草稿 → 调用 inventory 幂等冻结接口 → 成功进入 WAIT_PAY，失败进入 CLOSED/补偿
- **支付流程**：支付回调幂等处理 → 调用 inventory 幂等扣减接口 → 成功进入 PAID，失败进入 PAYING_RETRY/补偿
- **取消流程**：WAIT_PAY 取消 → 调用 inventory 幂等释放接口 → CANCELLED
- **退款流程（P2）**：PAID → REFUNDING → REFUNDED，已支付订单不能直接取消，需走退款流程
- **超时取消定时任务**：扫描超时 WAIT_PAY 订单
- **孤儿冻结补偿任务**：扫描已 frozen 但订单已关闭的记录
- **OpenFeign 配置**：连接超时 2s、读取超时 5s、幂等重试边界、虚拟线程兼容

## 5.2 数据库表设计

### 5.2.1 orders（订单表）

```sql
CREATE TABLE `orders` (
  `id`            VARCHAR(64)   NOT NULL COMMENT '订单ID（UUID，便于分布式）',
  `user_id`       BIGINT        NOT NULL COMMENT '用户ID',
  `hotel_id`      BIGINT        NOT NULL COMMENT '酒店ID',
  `hotel_name`    VARCHAR(128)  NOT NULL DEFAULT '' COMMENT '酒店名称（冗余）',
  `room_type`     VARCHAR(32)   NOT NULL COMMENT '房型',
  `room_type_name` VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '房型名称（冗余）',
  `check_in`      DATE          NOT NULL COMMENT '入住日期',
  `check_out`     DATE          NOT NULL COMMENT '离店日期',
  `nights`        INT           NOT NULL DEFAULT 0 COMMENT '入住晚数',
  `quantity`      INT           NOT NULL DEFAULT 1 COMMENT '预订间数',
  `total_price`   DECIMAL(10,2) NOT NULL DEFAULT 0.00 COMMENT '总价',
  `status`        VARCHAR(16)   NOT NULL DEFAULT 'INIT' COMMENT '订单状态',
  `error_msg`     VARCHAR(512)  NOT NULL DEFAULT '' COMMENT '错误信息（支付失败原因等）',
  `paid_at`       DATETIME      NULL COMMENT '支付时间',
  `created_at`    DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`    DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_user_id` (`user_id`),
  KEY `idx_hotel_id` (`hotel_id`),
  KEY `idx_status` (`status`),
  KEY `idx_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单表';
```

**设计说明**：

- 主键使用 `VARCHAR(64)` 的 UUID，而非自增 ID —— 分布式环境下自增 ID 需要额外中间件（如雪花算法），UUID 简洁可靠
- 冗余 `hotel_name`、`room_type_name`：避免每次查询订单都要 JOIN 酒店服务，典型的"空间换时间"策略
- `total_price` 在创建订单时计算好：金额计算不应该依赖外部服务在后续的每次查询中重复计算
- `error_msg` 记录支付失败原因，便于排查

### 5.2.2 初始化数据

```sql
-- 需要在 ota_order 库中先跑 DDL，本章不涉及初始化业务数据
CREATE DATABASE IF NOT EXISTS `ota_order` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE `ota_order`;
-- 然后执行上面的 orders 表 DDL
```

## 5.3 订单状态机

```
                ┌──────────────────────────────────────────────────────────────────────┐
                │                                                                      │
                ▼                                                                      │
    ┌──────┐   ┌──────────┐    ┌────────┐    ┌───────────┐   ┌───────────┐   ┌───────────┐
    │  *   │──▶│   INIT   │───▶│WAIT_PAY│───▶│   PAID    │──▶│ CONFIRMED  │──▶│CHECKED_IN │
    └──────┘   └────┬─────┘    └───┬────┘    └─────┬─────┘   └─────┬─────┘   └─────┬─────┘
                    │              │               │               │               │
                    │ 冻结失败     │ 超时取消/用户  │ 退款申请(P2)  │               │
                    ▼              ▼               ▼               ▼               ▼
               ┌────────┐   ┌──────────┐    ┌──────────┐    ┌───────────┐    ┌───────────┐
               │ CLOSED │   │CANCELLED │    │REFUNDING │    │COMPLETED  │    │COMPLETED  │
               └────────┘   └──────────┘    └────┬─────┘    └───────────┘    └───────────┘
                                                  │
                                                  │ 退款完成
                                                  ▼
                                             ┌──────────┐
                                             │ REFUNDED │
                                             └──────────┘
```

| 当前状态 | 操作 | 下一状态 | 说明 |
|---------|------|---------|------|
| `*` (初始) | 创建订单 | INIT | 订单刚创建，尚未尝试冻结 |
| INIT | 冻结库存成功 | WAIT_PAY | 库存已锁定，等待用户支付 |
| INIT | 冻结库存失败 | CLOSED | 库存不足直接关闭 |
| WAIT_PAY | 用户支付 | PAID | 支付成功，扣减库存 |
| WAIT_PAY | 超时取消 | CANCELLED | 定时任务触发，释放冻结库存 |
| WAIT_PAY | 用户取消 | CANCELLED | 用户主动取消 |
| PAID | 确认入住 | CONFIRMED | 酒店确认用户已入住（后台操作） |
| PAID | 退款申请 | REFUNDING | 已支付订单不能直接取消，需走退款流程 |
| REFUNDING | 退款完成 | REFUNDED | 退款处理完成，订单终态 |
| CONFIRMED | 离店完成 | COMPLETED | 订单完成 |
| CHECKED_IN | 离店完成 | COMPLETED | 入住后退房完成 |
| 任意状态 | — | CANCELLED | 用户取消（仅 INIT/WAIT_PAY 有效） |
| 任意状态 | — | CLOSED | 系统关闭（异常情况） |

**状态变更约束**（在代码中校验）：

1. 从 INIT 只能到 WAIT_PAY 或 CLOSED
2. 从 WAIT_PAY 只能到 PAID 或 CANCELLED
3. 从 PAID 只能到 CONFIRMED 或 REFUNDING
4. 从 REFUNDING 只能到 REFUNDED
5. CONFIRMED → CHECKED_IN → COMPLETED
6. CANCELLED、CLOSED、REFUNDED 是终态，不可再变更

## 5.4 项目结构与依赖

### 5.4.1 pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.ota</groupId>
        <artifactId>ota-cloud</artifactId>
        <version>1.0.0-SNAPSHOT</version>
        <relativePath>../pom.xml</relativePath>
    </parent>

    <artifactId>ota-order</artifactId>
    <version>1.0.0-SNAPSHOT</version>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>

        <!-- MyBatis-Plus -->
        <dependency>
            <groupId>com.baomidou</groupId>
            <artifactId>mybatis-plus-spring-boot3-starter</artifactId>
            <version>3.5.16</version>
        </dependency>

        <!-- MySQL -->
        <dependency>
            <groupId>com.mysql</groupId>
            <artifactId>mysql-connector-j</artifactId>
            <scope>runtime</scope>
        </dependency>

        <!-- Nacos 服务发现 + OpenFeign -->
        <dependency>
            <groupId>com.alibaba.cloud</groupId>
            <artifactId>spring-cloud-starter-alibaba-nacos-discovery</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.cloud</groupId>
            <artifactId>spring-cloud-starter-openfeign</artifactId>
        </dependency>
        <dependency>
            <groupId>io.github.openfeign</groupId>
            <artifactId>feign-micrometer</artifactId>
        </dependency>

        <!-- Lombok -->
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <optional>true</optional>
        </dependency>

        <!-- Validation -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-validation</artifactId>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

### 5.4.2 application.yml

```yaml
server:
  port: 8084

spring:
  application:
    name: ota-order

  threads:
    virtual:
      enabled: true

  datasource:
    url: jdbc:mysql://localhost:3306/ota_order?useUnicode=true&characterEncoding=utf8mb4&useSSL=false&serverTimezone=Asia/Shanghai
    username: root
    password: root123456
    driver-class-name: com.mysql.cj.jdbc.Driver
    hikari:
      maximum-pool-size: 20
      minimum-idle: 5
      connection-timeout: 10000

  cloud:
    nacos:
      discovery:
        server-addr: localhost:8848
        namespace: public

# OpenFeign 配置
spring.cloud.openfeign:
  client:
    config:
      default:
        connect-timeout: 2000
        read-timeout: 5000
        logger-level: basic
  compression:
    request:
      enabled: true
    response:
      enabled: true

# 订单超时配置（分钟）
order:
  payment-timeout-minutes: 30
  cancel-job:
    initial-delay: 10000   # 首次启动后10秒执行
    fixed-delay: 30000     # 每30秒扫描一次

mybatis-plus:
  configuration:
    map-underscore-to-camel-case: true
    log-impl: org.apache.ibatis.logging.stdout.StdOutImpl
  global-config:
    db-config:
      id-type: none   # 订单ID由程序生成 UUID

logging:
  level:
    com.ota.order: debug
    com.ota.order.feign: debug
```

### 5.4.3 启动类

```java
package com.ota.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableFeignClients
@EnableScheduling
public class OrderApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderApplication.class, args);
    }
}
```

> **说明**：`@EnableFeignClients` 启用 OpenFeign 客户端，`@EnableScheduling` 启用定时任务（超时取消 + 孤儿补偿）。

## 5.5 实体类

### 5.5.1 Order 实体

```java
package com.ota.order.entity;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Data
@TableName("orders")
public class Order {

    @TableId
    private String id;

    private Long userId;

    private Long hotelId;

    private String hotelName;

    private String roomType;

    private String roomTypeName;

    private LocalDate checkIn;

    private LocalDate checkOut;

    private Integer nights;

    private Integer quantity;

    private BigDecimal totalPrice;

    /** 订单状态：INIT/WAIT_PAY/PAID/CONFIRMED/CHECKED_IN/COMPLETED/CANCELLED/CLOSED/REFUNDING/REFUNDED */
    private String status;

    /** 错误信息（支付失败原因等） */
    private String errorMsg;

    private LocalDateTime paidAt;

    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createdAt;

    @TableField(fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updatedAt;
}
```

### 5.5.2 OrderStatus 枚举

```java
package com.ota.order.enums;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * 订单状态枚举
 */
public enum OrderStatus {

    INIT("INIT", "初始化"),
    WAIT_PAY("WAIT_PAY", "待支付"),
    PAID("PAID", "已支付"),
    CONFIRMED("CONFIRMED", "已确认"),
    CHECKED_IN("CHECKED_IN", "已入住"),
    COMPLETED("COMPLETED", "已完成"),
    CANCELLED("CANCELLED", "已取消"),
    CLOSED("CLOSED", "已关闭"),
    REFUNDING("REFUNDING", "退款中"),
    REFUNDED("REFUNDED", "已退款");

    private final String code;
    private final String desc;

    OrderStatus(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public String getCode() { return code; }
    public String getDesc() { return desc; }

    /** 终态：不可再变更 */
    private static final Set<String> FINAL_STATES = new HashSet<>(
            Arrays.asList(CANCELLED.getCode(), CLOSED.getCode(), COMPLETED.getCode(), REFUNDED.getCode()));

    /** 可取消的状态（PAID 已支付，需走退款流程） */
    private static final Set<String> CANCELLABLE_STATES = new HashSet<>(
            Arrays.asList(INIT.getCode(), WAIT_PAY.getCode()));

    /** 可支付的状态（理论上只有 WAIT_PAY） */
    private static final Set<String> PAYABLE_STATES = new HashSet<>(
            Arrays.asList(WAIT_PAY.getCode()));

    public static boolean isFinal(String status) {
        return FINAL_STATES.contains(status);
    }

    public static boolean isCancellable(String status) {
        return CANCELLABLE_STATES.contains(status);
    }

    public static boolean isPayable(String status) {
        return PAYABLE_STATES.contains(status);
    }

    /**
     * 状态机校验：从 from 状态到 to 状态是否合法
     */
    public static boolean isValidTransition(String from, String to) {
        if (from == null || to == null) return false;

        // 终态不可再变更
        if (isFinal(from) && !from.equals(to)) return false;

        // 定义合法的状态转移
        switch (from) {
            case "INIT":
                return "WAIT_PAY".equals(to) || "CLOSED".equals(to) || "CANCELLED".equals(to);
            case "WAIT_PAY":
                return "PAID".equals(to) || "CANCELLED".equals(to);
            case "PAID":
                return "CONFIRMED".equals(to) || "COMPLETED".equals(to) || "REFUNDING".equals(to);
            case "REFUNDING":
                return "REFUNDED".equals(to);
            case "CONFIRMED":
                return "CHECKED_IN".equals(to) || "COMPLETED".equals(to);
            case "CHECKED_IN":
                return "COMPLETED".equals(to);
            default:
                return false;
        }
    }
}
```

### 5.5.3 VO 与 DTO

**请求 DTO：**

```java
package com.ota.order.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import java.time.LocalDate;

/**
 * 创建订单请求
 */
@Data
public class OrderCreateDTO {

    @NotNull(message = "酒店ID不能为空")
    private Long hotelId;

    @NotNull(message = "房型不能为空")
    private String roomType;

    @NotNull(message = "入住日期不能为空")
    private LocalDate checkIn;

    @NotNull(message = "离店日期不能为空")
    private LocalDate checkOut;

    @Min(value = 1, message = "预订间数必须大于0")
    private Integer quantity = 1;
}
```

```java
package com.ota.order.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

/**
 * 支付请求
 */
@Data
public class OrderPayDTO {

    @NotBlank(message = "订单ID不能为空")
    private String orderId;
}
```

```java
package com.ota.order.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

/**
 * 取消订单请求
 */
@Data
public class OrderCancelDTO {

    @NotBlank(message = "订单ID不能为空")
    private String orderId;

    private String reason;
}
```

**响应 VO：**

```java
package com.ota.order.vo;

import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Data
public class OrderVO {
    private String id;
    private Long userId;
    private Long hotelId;
    private String hotelName;
    private String roomType;
    private String roomTypeName;
    private LocalDate checkIn;
    private LocalDate checkOut;
    private Integer nights;
    private Integer quantity;
    private BigDecimal totalPrice;
    private String status;
    private String statusDesc;       // 状态中文描述
    private String errorMsg;
    private LocalDateTime paidAt;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
```

## 5.6 OpenFeign 客户端（调用 ota-inventory）

### 5.6.1 库存操作 DTO（复用 inventory 服务的定义）

```java
package com.ota.order.feign.dto;

import lombok.Data;
import java.time.LocalDate;

@Data
public class InventoryOperateDTO {
    private Long hotelId;
    private String roomType;
    private LocalDate startDate;
    private LocalDate endDate;
    private Integer quantity;
    private String orderId;
}
```

```java
package com.ota.order.feign.dto;

import lombok.Data;
import java.util.List;

@Data
public class InventoryResultDTO {
    private boolean success;
    private String message;
    private List<DateResult> details;

    @Data
    public static class DateResult {
        private String bizDate;
        private boolean success;
        private String message;
    }
}
```

### 5.6.2 InventoryFeignClient

```java
package com.ota.order.feign;

import com.ota.common.result.Result;
import com.ota.order.feign.dto.InventoryOperateDTO;
import com.ota.order.feign.dto.InventoryResultDTO;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;

@FeignClient(name = "ota-inventory", path = "/inventory")
public interface InventoryFeignClient {

    /**
     * 冻结库存（下单锁定）
     */
    @PostMapping("/freeze")
    Result<InventoryResultDTO> freeze(@RequestBody InventoryOperateDTO dto);

    /**
     * 扣减库存（支付确认）
     */
    @PostMapping("/deduct")
    Result<InventoryResultDTO> deduct(@RequestBody InventoryOperateDTO dto);

    /**
     * 释放库存（取消订单）
     */
    @PostMapping("/release")
    Result<InventoryResultDTO> release(@RequestBody InventoryOperateDTO dto);

    /**
     * 查询某酒店某房型某天是否有可用库存
     */
    @GetMapping("/check")
    Result<Boolean> checkAvailability(
            @RequestParam("hotelId") Long hotelId,
            @RequestParam("roomType") String roomType,
            @RequestParam("bizDate") String bizDate,
            @RequestParam("quantity") Integer quantity);
}
```

> **注意**：FeignClient 的 `name` 属性对应 `ota-inventory` 在 Nacos 中的 `spring.application.name`。
> OpenFeign 会自动通过 Nacos 服务发现找到目标服务的 IP:端口。

## 5.7 订单状态变更工具类

```java
package com.ota.order.service;

import com.ota.order.entity.Order;
import com.ota.order.enums.OrderStatus;

/**
 * 订单状态变更工具类——保证原子性变更
 */
public class OrderStateMachine {

    /**
     * 校验并执行状态转移
     *
     * @param order  订单实体
     * @param newStatus 目标状态
     * @param errorMsg 可选错误信息
     * @throws IllegalStateException 如果状态转移非法
     */
    public static void transition(Order order, String newStatus, String errorMsg) {
        String oldStatus = order.getStatus();

        if (!OrderStatus.isValidTransition(oldStatus, newStatus)) {
            throw new IllegalStateException(
                    String.format("非法状态转移: %s → %s", oldStatus, newStatus));
        }

        order.setStatus(newStatus);
        if (errorMsg != null) {
            order.setErrorMsg(errorMsg);
        }

        // 支付时间
        if ("PAID".equals(newStatus)) {
            order.setPaidAt(java.time.LocalDateTime.now());
        }
    }

    public static void transition(Order order, String newStatus) {
        transition(order, newStatus, null);
    }
}
```

## 5.8 OrderService 核心业务

### 5.8.1 创建订单

> 事务边界说明：下面的 `@Transactional` 只覆盖 order 服务本地数据库操作；inventory 的冻结由库存服务自己的本地事务完成。跨服务一致性依赖幂等接口、库存流水和补偿任务，而不是依赖本地事务传播。

```java
package com.ota.order.service.impl;

import com.ota.common.result.Result;
import com.ota.order.dto.OrderCreateDTO;
import com.ota.order.entity.Order;
import com.ota.order.enums.OrderStatus;
import com.ota.order.feign.InventoryFeignClient;
import com.ota.order.feign.dto.InventoryOperateDTO;
import com.ota.order.feign.dto.InventoryResultDTO;
import com.ota.order.mapper.OrderMapper;
import com.ota.order.service.OrderStateMachine;
import com.ota.order.vo.OrderVO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements OrderService {

    private final OrderMapper orderMapper;
    private final InventoryFeignClient inventoryFeignClient;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public OrderVO createOrder(OrderCreateDTO dto, Long userId) {
        // 1. 计算入住晚数和总价
        long nights = ChronoUnit.DAYS.between(dto.getCheckIn(), dto.getCheckOut());
        if (nights <= 0) {
            throw new IllegalArgumentException("离店日期必须晚于入住日期");
        }

        // 2. 先查询库存和价格（通过 Feign 调用 inventory 服务）
        //    实际项目中应该先查价格再算总价，本章简化：先检查库存
        Result<InventoryResultDTO> checkResult = inventoryFeignClient.checkAvailability(
                dto.getHotelId(), dto.getRoomType(),
                dto.getCheckIn().toString(), dto.getQuantity());
        if (checkResult == null || !checkResult.isSuccess() || !Boolean.TRUE.equals(checkResult.getData())) {
            throw new RuntimeException("库存不足，无法创建订单");
        }

        // 3. 创建订单（INIT 状态）
        String orderId = UUID.randomUUID().toString().replace("-", "");
        Order order = new Order();
        order.setId(orderId);
        order.setUserId(userId);
        order.setHotelId(dto.getHotelId());
        order.setHotelName("");       // 可在查询酒店服务后填充
        order.setRoomType(dto.getRoomType());
        order.setRoomTypeName("");    // 同上
        order.setCheckIn(dto.getCheckIn());
        order.setCheckOut(dto.getCheckOut());
        order.setNights((int) nights);
        order.setQuantity(dto.getQuantity());
        order.setTotalPrice(BigDecimal.ZERO); // 实际应该从 inventory 返回价格计算
        order.setStatus(OrderStatus.INIT.getCode());

        orderMapper.insert(order);

        // 4. 调用 inventory 服务冻结库存
        InventoryOperateDTO freezeDTO = new InventoryOperateDTO();
        freezeDTO.setHotelId(dto.getHotelId());
        freezeDTO.setRoomType(dto.getRoomType());
        freezeDTO.setStartDate(dto.getCheckIn());
        freezeDTO.setEndDate(dto.getCheckOut());
        freezeDTO.setQuantity(dto.getQuantity());
        freezeDTO.setOrderId(orderId);

        try {
            Result<InventoryResultDTO> freezeResult = inventoryFeignClient.freeze(freezeDTO);
            if (freezeResult != null && freezeResult.isSuccess() && freezeResult.getData() != null
                    && freezeResult.getData().isSuccess()) {
                // 冻结成功 → WAIT_PAY
                OrderStateMachine.transition(order, OrderStatus.WAIT_PAY.getCode());
                orderMapper.updateById(order);
                log.info("订单创建成功，冻结库存成功，orderId={}", orderId);
            } else {
                // 冻结失败 → CLOSED
                String errMsg = (freezeResult != null) ? freezeResult.getMessage() : "库存冻结失败";
                OrderStateMachine.transition(order, OrderStatus.CLOSED.getCode(), errMsg);
                orderMapper.updateById(order);
                log.warn("订单创建失败，冻结库存失败，orderId={}, reason={}", orderId, errMsg);
            }
        } catch (Exception e) {
            // 远程调用异常 → CLOSED；若库存侧实际已冻结，由孤儿冻结补偿任务释放
            log.error("订单创建异常，调用库存服务失败，orderId={}", orderId, e);
            OrderStateMachine.transition(order, OrderStatus.CLOSED.getCode(),
                    "调用库存服务异常: " + e.getMessage());
            orderMapper.updateById(order);
        }

        return convertToVO(order);
    }

    // ... 其他方法后续展开
}
```

### 5.8.2 支付订单

```java
    @Override
    @Transactional(rollbackFor = Exception.class)
    public OrderVO payOrder(String orderId) {
        // 1. 查询订单
        Order order = orderMapper.selectById(orderId);
        if (order == null) {
            throw new IllegalArgumentException("订单不存在: " + orderId);
        }

        // 2. 校验状态
        if (!OrderStatus.isPayable(order.getStatus())) {
            throw new IllegalStateException("订单状态不可支付，当前状态: " + order.getStatus());
        }

        // 3. 调用 inventory 扣减库存
        InventoryOperateDTO deductDTO = new InventoryOperateDTO();
        deductDTO.setHotelId(order.getHotelId());
        deductDTO.setRoomType(order.getRoomType());
        deductDTO.setStartDate(order.getCheckIn());
        deductDTO.setEndDate(order.getCheckOut());
        deductDTO.setQuantity(order.getQuantity());
        deductDTO.setOrderId(orderId);

        try {
            Result<InventoryResultDTO> deductResult = inventoryFeignClient.deduct(deductDTO);
            if (deductResult != null && deductResult.isSuccess() && deductResult.getData() != null
                    && deductResult.getData().isSuccess()) {
                // 扣减成功 → PAID
                OrderStateMachine.transition(order, OrderStatus.PAID.getCode());
                orderMapper.updateById(order);
                log.info("订单支付成功，扣减库存完成，orderId={}", orderId);
            } else {
                // 扣减失败 → 保持 WAIT_PAY + 记录错误
                String errMsg = (deductResult != null) ? deductResult.getMessage() : "库存扣减失败";
                order.setErrorMsg(errMsg);
                orderMapper.updateById(order);
                log.warn("订单支付失败，库存扣减异常，orderId={}, reason={}", orderId, errMsg);
                throw new RuntimeException("支付失败: " + errMsg);
            }
        } catch (Exception e) {
            if (e instanceof RuntimeException) throw (RuntimeException) e;
            log.error("订单支付异常，orderId={}", orderId, e);
            order.setErrorMsg("扣减库存异常: " + e.getMessage());
            orderMapper.updateById(order);
            throw new RuntimeException("支付失败: " + e.getMessage());
        }

        return convertToVO(order);
    }
```

### 5.8.3 取消订单

```java
    @Override
    @Transactional(rollbackFor = Exception.class)
    public OrderVO cancelOrder(String orderId, String reason) {
        Order order = orderMapper.selectById(orderId);
        if (order == null) {
            throw new IllegalArgumentException("订单不存在: " + orderId);
        }

        // 校验是否可取消
        if (!OrderStatus.isCancellable(order.getStatus())) {
            throw new IllegalStateException("当前状态不可取消: " + order.getStatus());
        }

        // 调用 inventory 释放库存
        InventoryOperateDTO releaseDTO = new InventoryOperateDTO();
        releaseDTO.setHotelId(order.getHotelId());
        releaseDTO.setRoomType(order.getRoomType());
        releaseDTO.setStartDate(order.getCheckIn());
        releaseDTO.setEndDate(order.getCheckOut());
        releaseDTO.setQuantity(order.getQuantity());
        releaseDTO.setOrderId(orderId);

        try {
            Result<InventoryResultDTO> releaseResult = inventoryFeignClient.release(releaseDTO);
            if (releaseResult == null || !releaseResult.isSuccess()) {
                log.warn("取消订单时释放库存返回失败，orderId={}, result={}", orderId, releaseResult);
                // 即使释放失败，订单也要取消（后续由补偿任务处理）
            }
        } catch (Exception e) {
            log.error("取消订单时释放库存异常，orderId={}", orderId, e);
            // 不抛异常，订单仍然取消，由补偿任务兜底
        }

        // 变更状态为 CANCELLED
        OrderStateMachine.transition(order, OrderStatus.CANCELLED.getCode(), reason);
        orderMapper.updateById(order);

        log.info("订单已取消，orderId={}", orderId);
        return convertToVO(order);
    }
```

### 5.8.4 查询订单

```java
    @Override
    public OrderVO getOrder(String orderId) {
        Order order = orderMapper.selectById(orderId);
        if (order == null) {
            throw new IllegalArgumentException("订单不存在: " + orderId);
        }
        return convertToVO(order);
    }

    @Override
    public List<OrderVO> listUserOrders(Long userId, int page, int size) {
        Page<Order> pageParam = new Page<>(page, size);
        LambdaQueryWrapper<Order> wrapper = new LambdaQueryWrapper<Order>()
                .eq(Order::getUserId, userId)
                .orderByDesc(Order::getCreatedAt);
        Page<Order> orderPage = orderMapper.selectPage(pageParam, wrapper);
        return orderPage.getRecords().stream()
                .map(this::convertToVO)
                .collect(Collectors.toList());
    }

    private OrderVO convertToVO(Order order) {
        if (order == null) return null;
        OrderVO vo = new OrderVO();
        BeanUtils.copyProperties(order, vo);
        // 设置状态中文描述
        for (OrderStatus status : OrderStatus.values()) {
            if (status.getCode().equals(order.getStatus())) {
                vo.setStatusDesc(status.getDesc());
                break;
            }
        }
        return vo;
    }
```

## 5.9 定时任务

### 5.9.1 超时取消任务

```java
package com.ota.order.job;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.ota.order.entity.Order;
import com.ota.order.enums.OrderStatus;
import com.ota.order.feign.InventoryFeignClient;
import com.ota.order.feign.dto.InventoryOperateDTO;
import com.ota.order.mapper.OrderMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 超时未支付订单取消任务
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderTimeoutCancelJob {

    private final OrderMapper orderMapper;
    private final InventoryFeignClient inventoryFeignClient;

    @Value("${order.payment-timeout-minutes:30}")
    private int paymentTimeoutMinutes;

    /**
     * 每30秒扫描一次超时 WAIT_PAY 订单
     */
    @Scheduled(initialDelayString = "${order.cancel-job.initial-delay:10000}",
               fixedDelayString = "${order.cancel-job.fixed-delay:30000}")
    @Transactional(rollbackFor = Exception.class)
    public void cancelTimeoutOrders() {
        LocalDateTime deadline = LocalDateTime.now().minusMinutes(paymentTimeoutMinutes);

        List<Order> timeoutOrders = orderMapper.selectList(
                new LambdaQueryWrapper<Order>()
                        .eq(Order::getStatus, OrderStatus.WAIT_PAY.getCode())
                        .lt(Order::getCreatedAt, deadline)
                        .last("LIMIT 100")  // 每次最多处理100条，防止一次扫描太多
        );

        if (timeoutOrders.isEmpty()) {
            return;
        }

        log.info("超时取消任务：发现 {} 笔超时订单待取消", timeoutOrders.size());

        for (Order order : timeoutOrders) {
            try {
                // 释放库存
                InventoryOperateDTO releaseDTO = new InventoryOperateDTO();
                releaseDTO.setHotelId(order.getHotelId());
                releaseDTO.setRoomType(order.getRoomType());
                releaseDTO.setStartDate(order.getCheckIn());
                releaseDTO.setEndDate(order.getCheckOut());
                releaseDTO.setQuantity(order.getQuantity());
                releaseDTO.setOrderId(order.getId());

                try {
                    inventoryFeignClient.release(releaseDTO);
                } catch (Exception e) {
                    log.warn("超时取消释放库存异常，orderId={}", order.getId(), e);
                }

                // 变更状态
                order.setStatus(OrderStatus.CANCELLED.getCode());
                order.setErrorMsg("超时未支付，系统自动取消");
                orderMapper.updateById(order);
                log.info("超时订单已取消，orderId={}", order.getId());
            } catch (Exception e) {
                log.error("取消超时订单异常，orderId={}", order.getId(), e);
            }
        }
    }
}
```

### 5.9.2 孤儿冻结补偿任务

> 场景：订单创建时调用了 inventory 的 freeze 接口，但订单因超时/异常被关闭，冻结的库存未被释放，造成"孤儿冻结"。

```java
package com.ota.order.job;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.ota.order.entity.Order;
import com.ota.order.enums.OrderStatus;
import com.ota.order.feign.InventoryFeignClient;
import com.ota.order.feign.dto.InventoryOperateDTO;
import com.ota.order.mapper.OrderMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 孤儿冻结补偿任务
 *
 * 问题场景：订单创建时调用了 freeze 但订单最终没有进入 WAIT_PAY（如超时），
 * 导致 inventory 中存在 frozen 记录但订单已不在 WAIT_PAY 状态。
 * 本任务扫描这类"孤儿"订单，主动释放冻结库存。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrphanFreezeCompensationJob {

    private final OrderMapper orderMapper;
    private final InventoryFeignClient inventoryFeignClient;

    /**
     * 每5分钟扫描一次
     */
    @Scheduled(initialDelay = 60000, fixedDelay = 300000)
    public void compensateOrphanFreezes() {
        // 扫描已关闭/已取消的订单，它们的库存可能还没释放
        List<String> orphanStatuses = List.of(
                OrderStatus.CLOSED.getCode(),
                OrderStatus.CANCELLED.getCode()
        );

        // 查找最近30分钟内进入终态但 errorMsg 不包含"释放"相关信息的订单
        // （说明可能释放操作未成功执行）
        LocalDateTime since = LocalDateTime.now().minusMinutes(30);

        List<Order> orphans = orderMapper.selectList(
                new LambdaQueryWrapper<Order>()
                        .in(Order::getStatus, orphanStatuses)
                        .ge(Order::getUpdatedAt, since)
                        .notLike(Order::getErrorMsg, "释放")
                        .last("LIMIT 50")
        );

        if (orphans.isEmpty()) {
            return;
        }

        log.info("孤儿冻结补偿任务：发现 {} 笔可能需要释放库存的订单", orphans.size());

        for (Order order : orphans) {
            try {
                InventoryOperateDTO releaseDTO = new InventoryOperateDTO();
                releaseDTO.setHotelId(order.getHotelId());
                releaseDTO.setRoomType(order.getRoomType());
                releaseDTO.setStartDate(order.getCheckIn());
                releaseDTO.setEndDate(order.getCheckOut());
                releaseDTO.setQuantity(order.getQuantity());
                releaseDTO.setOrderId(order.getId());

                inventoryFeignClient.release(releaseDTO);
                log.info("孤儿冻结已补偿释放，orderId={}", order.getId());

                // 追加错误信息
                order.setErrorMsg(order.getErrorMsg() + "；补偿释放库存");
                orderMapper.updateById(order);
            } catch (Exception e) {
                log.error("孤儿冻结补偿失败，orderId={}", order.getId(), e);
            }
        }
    }
}
```

## 5.10 OrderController

```java
package com.ota.order.controller;

import com.ota.common.result.Result;
import com.ota.order.dto.OrderCancelDTO;
import com.ota.order.dto.OrderCreateDTO;
import com.ota.order.dto.OrderPayDTO;
import com.ota.order.service.OrderService;
import com.ota.order.vo.OrderVO;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/order")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    /**
     * 创建订单
     */
    @PostMapping("/create")
    public Result<OrderVO> createOrder(@Valid @RequestBody OrderCreateDTO dto,
                                       @RequestHeader("X-User-Id") Long userId) {
        OrderVO orderVO = orderService.createOrder(dto, userId);
        return Result.success(orderVO);
    }

    /**
     * 查询订单详情
     */
    @GetMapping("/{id}")
    public Result<OrderVO> getOrder(@PathVariable("id") String orderId) {
        OrderVO orderVO = orderService.getOrder(orderId);
        return Result.success(orderVO);
    }

    /**
     * 取消订单
     */
    @PostMapping("/cancel")
    public Result<OrderVO> cancelOrder(@Valid @RequestBody OrderCancelDTO dto) {
        OrderVO orderVO = orderService.cancelOrder(dto.getOrderId(), dto.getReason());
        return Result.success(orderVO);
    }

    /**
     * 支付订单
     */
    @PostMapping("/pay")
    public Result<OrderVO> payOrder(@Valid @RequestBody OrderPayDTO dto) {
        OrderVO orderVO = orderService.payOrder(dto.getOrderId());
        return Result.success(orderVO);
    }

    /**
     * 查询用户订单列表
     */
    @GetMapping("/list")
    public Result<List<OrderVO>> listOrders(
            @RequestHeader("X-User-Id") Long userId,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "10") int size) {
        List<OrderVO> list = orderService.listUserOrders(userId, page, size);
        return Result.success(list);
    }
}
```

## 5.11 Mapper 层

```java
package com.ota.order.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.ota.order.entity.Order;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface OrderMapper extends BaseMapper<Order> {
}
```

由于订单表查询条件不复杂，使用 MyBatis-Plus 的 `LambdaQueryWrapper` 即可，无需自定义 XML Mapper。

## 5.12 完整配置与启动

### 5.12.1 数据库初始化

```sql
CREATE DATABASE IF NOT EXISTS `ota_order`
  DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE `ota_order`;

-- 执行 5.2.1 节的 orders 表 DDL
```

### 5.12.2 Nacos 配置（可选）

如使用 Nacos 配置中心，可在 Nacos 中创建 `ota-order.yml`：

```yaml
spring:
  datasource:
    url: jdbc:mysql://localhost:3306/ota_order?useUnicode=true&characterEncoding=utf8mb4&useSSL=false&serverTimezone=Asia/Shanghai
    username: root
    password: root123456

order:
  payment-timeout-minutes: 30
```

然后在 `application.yml` 中添加：

```yaml
spring:
  cloud:
    nacos:
      config:
        server-addr: localhost:8848
        file-extension: yml
        namespace: public
        group: DEFAULT_GROUP
  config:
    import:
      - optional:nacos:ota-order.yml
```

### 5.12.3 编译与启动

```bash
# 确认 inventory 服务已启动（8085端口）
# 启动 order 服务
cd chapter-05/ota-order
mvn spring-boot:run
```

## 5.13 OpenFeign 配置详解

### 5.13.1 虚拟线程兼容

Spring Boot 3.5 启用虚拟线程后，OpenFeign 默认使用 `HttpClient` 或 `HttpURLConnection`。推荐使用 `feign-okhttp` 或 `feign-hc5` 以获得更好的虚拟线程兼容性：

```xml
<dependency>
    <groupId>io.github.openfeign</groupId>
    <artifactId>feign-hc5</artifactId>
</dependency>
```

然后配置：

```yaml
spring.cloud.openfeign:
  httpclient:
    hc5:
      enabled: true
```

### 5.13.2 超时与重试

```yaml
spring.cloud.openfeign.client.config:
  default:
    connect-timeout: 2000    # 连接超时 2 秒
    read-timeout: 5000       # 读取超时 5 秒
    logger-level: basic
  ota-inventory:            # 针对 inventory 服务单独配置
    connect-timeout: 2000
    read-timeout: 3000
```

### 5.13.3 请求/响应压缩

```yaml
spring.cloud.openfeign:
  compression:
    request:
      enabled: true
      mime-types: application/json
      min-request-size: 2048
    response:
      enabled: true
```

## 5.14 完整请求流程时序

```
Client          Gateway         Order              Inventory         DB
  │                │              │                   │               │
  │ POST /order/create            │                   │               │
  │──────────────▶│              │                   │               │
  │               │ JWT校验      │                   │               │
  │               │─────────────▶│                   │               │
  │               │              │ 1. check           │               │
  │               │              │───────────────────▶│               │
  │               │              │◀───────────────────│               │
  │               │              │                    │               │
  │               │              │ 2. INSERT order     │               │
  │               │              │────────────────────│──────────────▶│
  │               │              │◀───────────────────│───────────────│
  │               │              │                    │               │
  │               │              │ 3. POST /freeze     │               │
  │               │              │───────────────────▶│               │
  │               │              │   (乐观锁+流水)     │──────────────▶│
  │               │              │◀───────────────────│───────────────│
  │               │              │                    │               │
  │               │              │ 4. UPDATE WAIT_PAY  │               │
  │               │              │────────────────────│──────────────▶│
  │               │              │◀───────────────────│───────────────│
  │               │              │                    │               │
  │               │ 返回订单VO   │                    │               │
  │               │◀─────────────│                    │               │
  │◀──────────────│              │                    │               │
```

## 5.15 验证步骤

### 5.15.1 环境准备

```bash
# 1. 确保基础设施启动
cd chapter-01
docker compose up -d    # Nacos + MySQL + Redis

# 2. 初始化数据库
mysql -h127.0.0.1 -uroot -proot123456 -e "CREATE DATABASE IF NOT EXISTS ota_order DEFAULT CHARSET utf8mb4;"
mysql -h127.0.0.1 -uroot -proot123456 ota_order < chapter-05/sql/init.sql

# 3. 启动 inventory 服务（必须优先启动）
cd chapter-04/ota-inventory
mvn spring-boot:run

# 4. 新终端，启动 order 服务
cd chapter-05/ota-order
mvn spring-boot:run
```

### 5.15.2 创建订单

```bash
# 外部演示统一走 Gateway；先通过第2章登录拿到 TOKEN
curl -X POST http://localhost:8080/api/order \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "hotelId": 1,
    "roomType": "高级大床房",
    "checkIn": "2026-06-01",
    "checkOut": "2026-06-03",
    "quantity": 1
  }'
```

预期响应：

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "id": "xxx-xxx-xxx",
    "status": "WAIT_PAY",
    "totalPrice": 0,
    ...
  }
}
```

### 5.15.3 支付订单

```bash
# 替换为上面返回的订单ID；外部演示统一走 Gateway
curl -X POST http://localhost:8080/api/order/pay \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"orderId": "xxx-xxx-xxx"}'
```

预期响应状态为 `PAID`。

### 5.15.4 查询订单详情

```bash
curl http://localhost:8084/order/{orderId}
```

### 5.15.5 查询用户订单列表

```bash
curl http://localhost:8084/order/list?page=1&size=10 \
  -H "X-User-Id: 1"
```

### 5.15.6 测试库存扣减

支付后查询 inventory 服务确认冻结的库存已被扣减：

```bash
curl "http://localhost:8085/inventory/query?hotelId=1&roomType=高级大床房&startDate=2026-06-01&endDate=2026-06-03"
```

## 5.16 常见坑

### 🕳️ 坑1：Feign 调用超时导致跨服务状态不一致

**现象**：创建订单时 inventory freeze 实际成功，但网络波动导致 Feign 返回超时，order 侧把订单标记为 CLOSED，库存侧仍保留冻结流水。

**解决**：
- 设置较短的 Feign 超时（2s 连接 + 5s 读取）
- inventory 的 freeze/release/deduct 必须按 orderId 幂等
- 配合 **5.9.2 孤儿冻结补偿任务** 兜底
- Seata AT 只作为 P1 技术演示，不作为 P0 核心链路默认方案

### 🕳️ 坑2：@Transactional 与 Feign 的边界

**现象**：容易误以为 `@Transactional` 能覆盖 Feign 调用的远程服务。实际上订单库本地事务只管理 order 服务自己的数据库连接，inventory 服务会开启自己的本地事务，二者不会自动传播。

**解决**：
- 不把 Feign 调用描述成本地事务的一部分，避免面试时被追问打穿
- order 侧用订单状态记录远程调用结果，失败时进入 CLOSED 或 PAYING_RETRY
- inventory 侧按 orderId 做幂等，并写 `inventory_record` 库存流水
- 使用定时补偿扫描异常订单和孤儿冻结记录；Seata AT 放在 P1 演示章节

### 🕳️ 坑3：UUID 主键导致索引碎片

**现象**：使用 UUID 作为订单表主键，插入时页分裂严重。

**解决**：
- 如果数据量大，可在 DDL 中将 `id` 改为 `BINARY(16)` 存储 UUID
- 或使用雪花算法生成有序 ID（推荐 `MyBatis-Plus 的 IdWorker`）
- 或直接使用自增 ID + 分布式 ID 号段

### 🕳️ 坑4：定时任务重复执行（多实例）

**现象**：部署多个 order 服务实例时，超时取消任务在所有实例上同时执行。

**解决**：
- 使用 `@SchedulerLock`（来自 `net.javacrumbs.shedlock`）实现分布式锁
- 或使用 `LIMIT 100` + 乐观更新（先 UPDATE 再 SELECT 受影响的条数）
- 本教程简化版在每个实例上执行，通过 `LIMIT` 减少冲突概率

### 🕳️ 坑5：虚拟线程 + Feign 的 ClassLoader 问题

**现象**：在虚拟线程中调用 Feign 时出现 `ClassCastException` 或 `ClassLoader` 相关异常。

**解决**：
- 使用 `feign-hc5` 替代默认的 `HttpURLConnection`
- 确保 Spring Cloud 版本兼容（Spring Cloud 2025.0.x + Spring Boot 3.5.x 已验证）

---

## 5.17 面试问答

### Q1：PAID 状态的订单能不能直接取消？你们的系统是怎么设计的？

**不能直接取消。** PAID 状态的订单已经完成支付，资金已经经过结算流程，直接取消会导致财务对账问题。

在我们的系统中，PAID 订单要走**退款流程**：

1. 用户或后端发起退款申请 → 状态变为 `REFUNDING`（退款中）
2. 退款处理（调用支付网关退款接口、异步回调）
3. 退款完成 → 状态变为 `REFUNDED`（已退款）

WAIT_PAY 状态的订单走**取消流程**（释放冻结库存即可），而已经支付的订单必须走退款流程。这在状态机中严格区分：
- WAIT_PAY → CANCELLED：释放冻结库存
- PAID → REFUNDING → REFUNDED：退款 + 释放扣减库存

### Q2：为什么不用 CANCELLED 直接表示退款，而是引入 REFUNDING/REFUNDED？

防止业务语义混淆：

- **CANCELLED** 代表"未支付取消"——只需释放冻结库存，无需资金操作
- **REFUNDING/REFUNDED** 代表"支付后退款"——涉及资金结算、支付网关交互、税务冲抵

区分后，财务审计可以快速统计退款金额，业务方也可以区分"用户主动放弃"和"售后退款"两种不同的场景。State 枚举中 `FINAL_STATES` 包含了 `REFUNDED`，确保退款终态不可再变更。

### Q3：你们的订单状态机有没有用状态模式（State Pattern）来实现？

没有。当前使用**表驱动法**（`isValidTransition` 中的 switch + 常量集合），原因：

- 状态数目前可控（10 个状态），switch 足够清晰
- 状态模式会引入大量状态子类，过度设计
- 业务规则中状态转移的**触发条件是外部事件**（支付回调、定时任务、用户操作），不是状态内部行为

如果未来状态增加（如退款审批流程拆分多个子状态），会考虑引入状态机框架如 **Spring Statemachine** 或 **Cola StateMachine**。

---

**下一步**：完成订单核心链路后，进入第6章 — Kafka 事件驱动与 outbox 模式，实现订单事件的消息通知。
