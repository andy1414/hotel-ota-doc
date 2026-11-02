# 第4章：Inventory 价库服务

## 4.1 本章目标

完成 `ota-inventory`（端口 8085）价库服务，实现：

- **价库合一表**：统一管理价格和库存
- **库存流水表**：记录每一次库存变更
- **核心接口**：价库查询、冻结、释放、扣减、流水查询
- **乐观锁**：通过 `version` 字段防止并发更新
- **库存状态机**：AVAILABLE → FROZEN → DEDUCTED / RELEASED
- **Redis 缓存**：Cache-Aside 模式，TTL 5min

## 4.2 数据库表设计

### 4.2.1 inventory_price_stock（价库合一表）

> 为什么用"价库合一"？因为在酒店预订场景中，价格和库存是高度耦合的——同一个日期同一个房型，价格和库存必须一起查询和更新。分开两张表会导致频繁的 JOIN 和一致性问题。

```sql
CREATE TABLE `inventory_price_stock` (
  `id`         BIGINT       NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `hotel_id`   BIGINT       NOT NULL COMMENT '酒店ID',
  `room_type`  VARCHAR(32)  NOT NULL COMMENT '房型（如：king_double、standard_twin）',
  `biz_date`   DATE         NOT NULL COMMENT '业务日期',
  `sell_price` DECIMAL(10,2) NOT NULL DEFAULT 0.00 COMMENT '售价',
  `cost_price` DECIMAL(10,2) NOT NULL DEFAULT 0.00 COMMENT '成本价',
  `currency`   VARCHAR(3)   NOT NULL DEFAULT 'CNY' COMMENT '币种',
  `total`      INT          NOT NULL DEFAULT 0 COMMENT '总库存',
  `frozen`     INT          NOT NULL DEFAULT 0 COMMENT '已冻结库存',
  `booked`     INT          NOT NULL DEFAULT 0 COMMENT '已预订库存',
  `available`  INT          NOT NULL DEFAULT 0 COMMENT '可用库存（冗余计算列）',
  `version`    INT          NOT NULL DEFAULT 0 COMMENT '乐观锁版本号',
  `updated_at` DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_hotel_room_date` (`hotel_id`, `room_type`, `biz_date`),
  KEY `idx_hotel_date` (`hotel_id`, `biz_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='价库合一表';
```

**字段说明**：

| 字段 | 含义 | 计算逻辑 |
|------|------|---------|
| total | 总库存（房间总数） | 不变，除非人工调整 |
| frozen | 已冻结（下单锁定） | freeze +1 → release -1 / deduct 不变 |
| booked | 已预订（最终确认） | deduct +1 |
| available | 可用库存 | **available = total - frozen - booked** |

价格与库存的关系：`available` 是一个冗余字段，每次操作后应保证 `available = total - frozen - booked`。

### 4.2.2 inventory_record（库存流水表）

```sql
CREATE TABLE `inventory_record` (
  `id`         BIGINT       NOT NULL AUTO_INCREMENT COMMENT '流水ID',
  `order_id`   VARCHAR(64)  NOT NULL COMMENT '订单ID',
  `hotel_id`   BIGINT       NOT NULL COMMENT '酒店ID',
  `room_type`  VARCHAR(32)  NOT NULL COMMENT '房型',
  `start_date` DATE         NOT NULL COMMENT '入住日期',
  `end_date`   DATE         NOT NULL COMMENT '离店日期',
  `quantity`   INT          NOT NULL COMMENT '数量（间数）',
  `status`     VARCHAR(16)  NOT NULL COMMENT '状态：FROZEN/DEDUCTED/RELEASED',
  `remark`     VARCHAR(255) NOT NULL DEFAULT '' COMMENT '备注',
  `created_at` DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_order_id` (`order_id`),
  KEY `idx_hotel_date` (`hotel_id`, `start_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='库存流水表';
```

### 4.2.3 初始化数据

```sql
-- 为酒店1（深圳湾万丽）的部分房型初始化30天库存
INSERT INTO inventory_price_stock (hotel_id, room_type, biz_date, sell_price, cost_price, total, frozen, booked, available, version)
SELECT
  1 AS hotel_id,
  rt.name AS room_type,
  date_add(CURDATE(), INTERVAL seq.day) AS biz_date,
  CASE rt.id
    WHEN 1 THEN 888.00   -- 高级大床房
    WHEN 2 THEN 1088.00  -- 豪华双床房
    WHEN 3 THEN 1888.00  -- 行政套房
  END AS sell_price,
  CASE rt.id
    WHEN 1 THEN 600.00
    WHEN 2 THEN 700.00
    WHEN 3 THEN 1200.00
  END AS cost_price,
  'CNY' AS currency,
  CASE rt.id
    WHEN 1 THEN 30   -- 大床房30间
    WHEN 2 THEN 20   -- 双床房20间
    WHEN 3 THEN 5    -- 套房5间
  END AS total,
  0 AS frozen,
  0 AS booked,
  CASE rt.id
    WHEN 1 THEN 30
    WHEN 2 THEN 20
    WHEN 3 THEN 5
  END AS available,
  0 AS version
FROM room_type rt
CROSS JOIN (
  SELECT 0 AS day UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
  UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9
  UNION SELECT 10 UNION SELECT 11 UNION SELECT 12 UNION SELECT 13 UNION SELECT 14
  UNION SELECT 15 UNION SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19
  UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23 UNION SELECT 24
  UNION SELECT 25 UNION SELECT 26 UNION SELECT 27 UNION SELECT 28 UNION SELECT 29
) seq
WHERE rt.hotel_id = 1;
```

> **注意**：P0 阶段使用单表。P2 阶段会基于 `hotel_id % 100` 做 ShardingSphere 分表，此处留好扩展。

## 4.3 项目依赖与配置

### 4.3.1 pom.xml

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

    <artifactId>ota-inventory</artifactId>
    <version>1.0.0-SNAPSHOT</version>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-redis</artifactId>
        </dependency>
        <dependency>
            <groupId>com.baomidou</groupId>
            <artifactId>mybatis-plus-spring-boot3-starter</artifactId>
            <version>3.5.16</version>
        </dependency>
        <dependency>
            <groupId>com.mysql</groupId>
            <artifactId>mysql-connector-j</artifactId>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>com.alibaba.cloud</groupId>
            <artifactId>spring-cloud-starter-alibaba-nacos-discovery</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.commons</groupId>
            <artifactId>commons-pool2</artifactId>
        </dependency>
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <optional>true</optional>
        </dependency>
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

### 4.3.2 application.yml

```yaml
server:
  port: 8085

spring:
  application:
    name: ota-inventory

  threads:
    virtual:
      enabled: true

  datasource:
    url: jdbc:mysql://localhost:3306/ota_inventory?useUnicode=true&characterEncoding=utf8mb4&useSSL=false&serverTimezone=Asia/Shanghai
    username: root
    password: root123456
    driver-class-name: com.mysql.cj.jdbc.Driver
    hikari:
      maximum-pool-size: 20
      minimum-idle: 5

  data:
    redis:
      host: localhost
      port: 6379
      password: redis123456
      database: 1          # 价库服务使用 DB1，与酒店服务隔离
      timeout: 3000ms
      lettuce:
        pool:
          max-active: 16
          max-idle: 8
          min-idle: 4

  cloud:
    nacos:
      discovery:
        server-addr: localhost:8848

mybatis-plus:
  configuration:
    map-underscore-to-camel-case: true
    log-impl: org.apache.ibatis.logging.stdout.StdOutImpl
  global-config:
    db-config:
      id-type: auto
```

## 4.4 实体类

### 4.4.1 InventoryPriceStock

```java
package com.ota.inventory.entity;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Data
@TableName("inventory_price_stock")
public class InventoryPriceStock {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long hotelId;

    private String roomType;

    private LocalDate bizDate;

    private BigDecimal sellPrice;

    private BigDecimal costPrice;

    private String currency;

    private Integer total;

    private Integer frozen;

    private Integer booked;

    private Integer available;

    /**
     * 乐观锁版本号
     */
    @Version
    private Integer version;

    @TableField(fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updatedAt;
}
```

### 4.4.2 InventoryRecord

```java
package com.ota.inventory.entity;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Data
@TableName("inventory_record")
public class InventoryRecord {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String orderId;

    private Long hotelId;

    private String roomType;

    private LocalDate startDate;

    private LocalDate endDate;

    private Integer quantity;

    /**
     * 状态：FROZEN / DEDUCTED / RELEASED
     */
    private String status;

    private String remark;

    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createdAt;

    @TableField(fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updatedAt;
}
```

### 4.4.3 VO 与 DTO

```java
package com.ota.inventory.vo;

import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * 价库查询响应VO
 */
@Data
public class InventoryPriceVO {
    private Long id;
    private Long hotelId;
    private String roomType;
    private LocalDate bizDate;
    private BigDecimal sellPrice;
    private String currency;
    private Integer total;
    private Integer frozen;
    private Integer booked;
    private Integer available;
}
```

```java
package com.ota.inventory.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import java.time.LocalDate;

/**
 * 库存操作（冻结/释放/扣减）请求DTO
 */
@Data
public class InventoryOperateDTO {

    @NotNull(message = "酒店ID不能为空")
    private Long hotelId;

    @NotBlank(message = "房型不能为空")
    private String roomType;

    @NotNull(message = "入住日期不能为空")
    private LocalDate startDate;

    @NotNull(message = "离店日期不能为空")
    private LocalDate endDate;

    @Min(value = 1, message = "数量必须大于0")
    private Integer quantity;

    /** 订单ID（冻结时由调用方生成） */
    @NotBlank(message = "订单ID不能为空")
    private String orderId;
}
```

```java
package com.ota.inventory.vo;

import lombok.Builder;
import lombok.Data;
import java.util.List;

/**
 * 库存操作结果
 */
@Data
@Builder
public class InventoryResultVO {

    private boolean success;

    private String message;

    /** 成功冻结/释放/扣减的日期明细 */
    private List<DateResult> details;

    @Data
    @Builder
    public static class DateResult {
        private LocalDate bizDate;
        private boolean success;
        private String message;
        private Integer version;
    }
}
```

## 4.5 库存状态机

```
                     ┌──────────┐
                     │ AVAILABLE │  (数据库中的 available 字段 > 0)
                     └─────┬─────┘
                           │ 冻结 (freeze)
                           ▼
                     ┌──────────┐
                     │  FROZEN   │  (frozen+1, available-1, 写流水记录)
                     └─────┬─────┘
                      ╱         ╲
                     ╱           ╲
                    ▼             ▼
              ┌──────────┐  ┌──────────┐
              │ DEDUCTED  │  │ RELEASED  │
              │(booked+1) │  │(frozen-1) │
              │(frozen-1) │  │(avail+1)  │
              └──────────┘  └──────────┘
```

| 操作 | 前置条件 | 字段变更 | 流水状态 |
|------|---------|---------|---------|
| freeze（冻结） | available >= quantity | frozen += qty, available -= qty | FROZEN |
| deduct（扣减） | frozen >= quantity | booked += qty, frozen -= qty | DEDUCTED |
| release（释放） | frozen >= quantity | frozen -= qty, available += qty | RELEASED |

## 4.6 Mapper 层

### 4.6.1 InventoryPriceStockMapper

```java
package com.ota.inventory.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.ota.inventory.entity.InventoryPriceStock;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.time.LocalDate;
import java.util.List;

@Mapper
public interface InventoryPriceStockMapper extends BaseMapper<InventoryPriceStock> {

    /**
     * 查询指定酒店房型日期范围的价库
     */
    @Select("SELECT * FROM inventory_price_stock " +
            "WHERE hotel_id = #{hotelId} AND room_type = #{roomType} " +
            "AND biz_date >= #{startDate} AND biz_date < #{endDate} " +
            "ORDER BY biz_date ASC")
    List<InventoryPriceStock> selectByHotelAndDateRange(
            @Param("hotelId") Long hotelId,
            @Param("roomType") String roomType,
            @Param("startDate") LocalDate startDate,
            @Param("endDate") LocalDate endDate);

    /**
     * 查询单天价库
     */
    @Select("SELECT * FROM inventory_price_stock " +
            "WHERE hotel_id = #{hotelId} AND room_type = #{roomType} " +
            "AND biz_date = #{bizDate}")
    InventoryPriceStock selectByUniqueKey(
            @Param("hotelId") Long hotelId,
            @Param("roomType") String roomType,
            @Param("bizDate") LocalDate bizDate);

    /**
     * 乐观锁冻结库存
     * UPDATE ... SET frozen = frozen + #{quantity}, available = available - #{quantity},
     *               version = version + 1
     * WHERE id = #{id} AND version = #{version} AND available >= #{quantity}
     */
    @Update("UPDATE inventory_price_stock SET " +
            "frozen = frozen + #{quantity}, " +
            "available = available - #{quantity}, " +
            "version = version + 1 " +
            "WHERE id = #{id} AND version = #{version} AND available >= #{quantity}")
    int freezeById(@Param("id") Long id,
                   @Param("version") Integer version,
                   @Param("quantity") Integer quantity);

    /**
     * 乐观锁释放库存
     */
    @Update("UPDATE inventory_price_stock SET " +
            "frozen = frozen - #{quantity}, " +
            "available = available + #{quantity}, " +
            "version = version + 1 " +
            "WHERE id = #{id} AND version = #{version} AND frozen >= #{quantity}")
    int releaseById(@Param("id") Long id,
                    @Param("version") Integer version,
                    @Param("quantity") Integer quantity);

    /**
     * 乐观锁扣减库存（扣减已冻结部分）
     */
    @Update("UPDATE inventory_price_stock SET " +
            "booked = booked + #{quantity}, " +
            "frozen = frozen - #{quantity}, " +
            "version = version + 1 " +
            "WHERE id = #{id} AND version = #{version} AND frozen >= #{quantity}")
    int deductById(@Param("id") Long id,
                   @Param("version") Integer version,
                   @Param("quantity") Integer quantity);
}
```

### 4.6.2 InventoryRecordMapper

```java
package com.ota.inventory.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.ota.inventory.entity.InventoryRecord;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface InventoryRecordMapper extends BaseMapper<InventoryRecord> {

    @Select("SELECT * FROM inventory_record WHERE order_id = #{orderId} ORDER BY created_at ASC")
    List<InventoryRecord> selectByOrderId(@Param("orderId") String orderId);
}
```

### 4.6.3 MyBatis-Plus 乐观锁插件配置

```java
package com.ota.inventory.config;

import com.baomidou.mybatisplus.annotation.DbType;
import com.baomidou.mybatisplus.extension.plugins.MybatisPlusInterceptor;
import com.baomidou.mybatisplus.extension.plugins.inner.OptimisticLockerInnerInterceptor;
import com.baomidou.mybatisplus.extension.plugins.inner.PaginationInnerInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MyBatisPlusConfig {

    @Bean
    public MybatisPlusInterceptor mybatisPlusInterceptor() {
        MybatisPlusInterceptor interceptor = new MybatisPlusInterceptor();
        // 分页插件
        interceptor.addInnerInterceptor(new PaginationInnerInterceptor(DbType.MYSQL));
        // 乐观锁插件
        interceptor.addInnerInterceptor(new OptimisticLockerInnerInterceptor());
        return interceptor;
    }
}
```

> **关键点**：`OptimisticLockerInnerInterceptor` 会自动拦截带有 `@Version` 注解字段的实体更新，在 UPDATE 时自动加上 `WHERE version = oldVersion` 和 `SET version = version + 1`。对于自定义 SQL（如 `freezeById`），需要手动写版本判断逻辑。

## 4.7 Service 层

### 4.7.1 Redis 缓存服务（复用第3章模式）

```java
package com.ota.inventory.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 库存缓存服务，轻量版（价库数据TTL短、更新频繁）
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryCacheService {

    private final RedisTemplate<String, Object> redisTemplate;

    private static final String NULL_CACHE = "NULL";
    private static final long DEFAULT_TTL_SECONDS = 300;  // 5分钟

    @SuppressWarnings("unchecked")
    public <T> T queryWithCache(String key, Class<T> type, Supplier<T> dbLoader) {
        Object cached = redisTemplate.opsForValue().get(key);
        if (cached != null) {
            if (NULL_CACHE.equals(cached)) {
                return null;
            }
            return (T) cached;
        }

        // 这里用简单模式（价库场景不需要互斥锁，乐观锁在DB层保证一致）
        T result = dbLoader.get();

        if (result != null) {
            redisTemplate.opsForValue()
                    .set(key, result, DEFAULT_TTL_SECONDS, TimeUnit.SECONDS);
        } else {
            redisTemplate.opsForValue()
                    .set(key, NULL_CACHE, 30, TimeUnit.SECONDS);
        }
        return result;
    }

    public void evict(String key) {
        redisTemplate.delete(key);
    }
}
```

> **为什么库存缓存不需要互斥锁？**
> 库存数据对一致性要求极高。缓存只是"加速读"，真正的一致性由数据库乐观锁保证。即使缓存击穿打到DB，DB也能通过乐观锁防止超卖。没必要为了"防击穿"增加复杂度反而可能引入锁超时等新问题。

### 4.7.2 InventoryService

```java
package com.ota.inventory.service;

import com.ota.inventory.dto.InventoryOperateDTO;
import com.ota.inventory.entity.InventoryPriceStock;
import com.ota.inventory.entity.InventoryRecord;
import com.ota.inventory.mapper.InventoryPriceStockMapper;
import com.ota.inventory.mapper.InventoryRecordMapper;
import com.ota.inventory.vo.InventoryPriceVO;
import com.ota.inventory.vo.InventoryResultVO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryService {

    private static final String CACHE_KEY_PREFIX = "ps:";

    private final InventoryPriceStockMapper priceStockMapper;
    private final InventoryRecordMapper recordMapper;
    private final InventoryCacheService cacheService;

    // ==================== 查询接口 ====================

    /**
     * 查询指定酒店房型日期范围的价库（带Redis缓存）
     */
    public List<InventoryPriceVO> queryPriceAndStock(
            Long hotelId, String roomType,
            LocalDate startDate, LocalDate endDate) {

        List<InventoryPriceStock> records = priceStockMapper
                .selectByHotelAndDateRange(hotelId, roomType, startDate, endDate);

        if (records == null || records.isEmpty()) {
            return new ArrayList<>();
        }

        return records.stream().map(this::toVO).collect(Collectors.toList());
    }

    /**
     * 查询单天价库（走缓存）
     */
    public InventoryPriceVO querySingleDay(
            Long hotelId, String roomType, LocalDate bizDate) {

        String cacheKey = buildCacheKey(hotelId, roomType, bizDate);

        InventoryPriceStock stock = cacheService.queryWithCache(
                cacheKey,
                InventoryPriceStock.class,
                () -> priceStockMapper.selectByUniqueKey(hotelId, roomType, bizDate)
        );

        return stock != null ? toVO(stock) : null;
    }

    // ==================== 冻结接口 ====================

    /**
     * 冻结库存（下单锁定）
     * 对日期范围内的每一天逐天冻结
     */
    @Transactional(rollbackFor = Exception.class)
    public InventoryResultVO freeze(InventoryOperateDTO dto) {
        List<InventoryResultVO.DateResult> details = new ArrayList<>();

        for (LocalDate date = dto.getStartDate();
             date.isBefore(dto.getEndDate());
             date = date.plusDays(1)) {

            InventoryPriceStock stock = priceStockMapper
                    .selectByUniqueKey(dto.getHotelId(), dto.getRoomType(), date);

            if (stock == null) {
                details.add(InventoryResultVO.DateResult.builder()
                        .bizDate(date).success(false)
                        .message("未找到库存记录").build());
                continue;
            }

            if (stock.getAvailable() < dto.getQuantity()) {
                details.add(InventoryResultVO.DateResult.builder()
                        .bizDate(date).success(false)
                        .message("库存不足，可用库存: " + stock.getAvailable()
                                + "，需要: " + dto.getQuantity()).build());
                continue;
            }

            // 乐观锁冻结
            int affected = priceStockMapper.freezeById(
                    stock.getId(), stock.getVersion(), dto.getQuantity());

            if (affected == 0) {
                // 版本冲突或库存不足，重试一次
                InventoryPriceStock refreshed = priceStockMapper
                        .selectById(stock.getId());
                affected = priceStockMapper.freezeById(
                        refreshed.getId(), refreshed.getVersion(), dto.getQuantity());
            }

            if (affected > 0) {
                // 写入流水
                InventoryRecord record = new InventoryRecord();
                record.setOrderId(dto.getOrderId());
                record.setHotelId(dto.getHotelId());
                record.setRoomType(dto.getRoomType());
                record.setStartDate(dto.getStartDate());
                record.setEndDate(dto.getEndDate());
                record.setQuantity(dto.getQuantity());
                record.setStatus("FROZEN");
                record.setRemark("库存冻结");
                recordMapper.insert(record);

                // 删缓存
                evictCache(dto.getHotelId(), dto.getRoomType(), date);

                details.add(InventoryResultVO.DateResult.builder()
                        .bizDate(date).success(true)
                        .message("冻结成功").build());
            } else {
                details.add(InventoryResultVO.DateResult.builder()
                        .bizDate(date).success(false)
                        .message("冻结失败（并发冲突）").build());
            }
        }

        boolean allSuccess = details.stream().allMatch(InventoryResultVO.DateResult::isSuccess);
        return InventoryResultVO.builder()
                .success(allSuccess)
                .message(allSuccess ? "全部冻结成功" : "部分日期冻结失败，详见details")
                .details(details)
                .build();
    }

    // ==================== 释放接口 ====================

    /**
     * 释放库存（取消订单）
     */
    @Transactional(rollbackFor = Exception.class)
    public InventoryResultVO release(InventoryOperateDTO dto) {
        List<InventoryResultVO.DateResult> details = new ArrayList<>();

        for (LocalDate date = dto.getStartDate();
             date.isBefore(dto.getEndDate());
             date = date.plusDays(1)) {

            InventoryPriceStock stock = priceStockMapper
                    .selectByUniqueKey(dto.getHotelId(), dto.getRoomType(), date);

            if (stock == null) {
                details.add(InventoryResultVO.DateResult.builder()
                        .bizDate(date).success(false)
                        .message("未找到库存记录").build());
                continue;
            }

            if (stock.getFrozen() < dto.getQuantity()) {
                details.add(InventoryResultVO.DateResult.builder()
                        .bizDate(date).success(false)
                        .message("冻结库存不足，已冻结: " + stock.getFrozen()).build());
                continue;
            }

            int affected = priceStockMapper.releaseById(
                    stock.getId(), stock.getVersion(), dto.getQuantity());

            if (affected == 0) {
                InventoryPriceStock refreshed = priceStockMapper
                        .selectById(stock.getId());
                affected = priceStockMapper.releaseById(
                        refreshed.getId(), refreshed.getVersion(), dto.getQuantity());
            }

            if (affected > 0) {
                // 写入流水
                InventoryRecord record = new InventoryRecord();
                record.setOrderId(dto.getOrderId());
                record.setHotelId(dto.getHotelId());
                record.setRoomType(dto.getRoomType());
                record.setStartDate(dto.getStartDate());
                record.setEndDate(dto.getEndDate());
                record.setQuantity(dto.getQuantity());
                record.setStatus("RELEASED");
                record.setRemark("库存释放");
                recordMapper.insert(record);

                evictCache(dto.getHotelId(), dto.getRoomType(), date);

                details.add(InventoryResultVO.DateResult.builder()
                        .bizDate(date).success(true)
                        .message("释放成功").build());
            } else {
                details.add(InventoryResultVO.DateResult.builder()
                        .bizDate(date).success(false)
                        .message("释放失败（并发冲突）").build());
            }
        }

        boolean allSuccess = details.stream().allMatch(InventoryResultVO.DateResult::isSuccess);
        return InventoryResultVO.builder()
                .success(allSuccess)
                .message(allSuccess ? "全部释放成功" : "部分释放失败")
                .details(details)
                .build();
    }

    // ==================== 扣减接口 ====================

    /**
     * 扣减库存（确认入住/已消费）
     */
    @Transactional(rollbackFor = Exception.class)
    public InventoryResultVO deduct(InventoryOperateDTO dto) {
        List<InventoryResultVO.DateResult> details = new ArrayList<>();

        for (LocalDate date = dto.getStartDate();
             date.isBefore(dto.getEndDate());
             date = date.plusDays(1)) {

            InventoryPriceStock stock = priceStockMapper
                    .selectByUniqueKey(dto.getHotelId(), dto.getRoomType(), date);

            if (stock == null) {
                details.add(InventoryResultVO.DateResult.builder()
                        .bizDate(date).success(false)
                        .message("未找到库存记录").build());
                continue;
            }

            if (stock.getFrozen() < dto.getQuantity()) {
                details.add(InventoryResultVO.DateResult.builder()
                        .bizDate(date).success(false)
                        .message("冻结库存不足，已冻结: " + stock.getFrozen()).build());
                continue;
            }

            int affected = priceStockMapper.deductById(
                    stock.getId(), stock.getVersion(), dto.getQuantity());

            if (affected == 0) {
                InventoryPriceStock refreshed = priceStockMapper
                        .selectById(stock.getId());
                affected = priceStockMapper.deductById(
                        refreshed.getId(), refreshed.getVersion(), dto.getQuantity());
            }

            if (affected > 0) {
                InventoryRecord record = new InventoryRecord();
                record.setOrderId(dto.getOrderId());
                record.setHotelId(dto.getHotelId());
                record.setRoomType(dto.getRoomType());
                record.setStartDate(dto.getStartDate());
                record.setEndDate(dto.getEndDate());
                record.setQuantity(dto.getQuantity());
                record.setStatus("DEDUCTED");
                record.setRemark("库存扣减");
                recordMapper.insert(record);

                evictCache(dto.getHotelId(), dto.getRoomType(), date);

                details.add(InventoryResultVO.DateResult.builder()
                        .bizDate(date).success(true)
                        .message("扣减成功").build());
            } else {
                details.add(InventoryResultVO.DateResult.builder()
                        .bizDate(date).success(false)
                        .message("扣减失败（并发冲突）").build());
            }
        }

        boolean allSuccess = details.stream().allMatch(InventoryResultVO.DateResult::isSuccess);
        return InventoryResultVO.builder()
                .success(allSuccess)
                .message(allSuccess ? "全部扣减成功" : "部分扣减失败")
                .details(details)
                .build();
    }

    // ==================== 流水查询 ====================

    /**
     * 查询订单的库存流水
     */
    public List<InventoryRecord> getRecordsByOrderId(String orderId) {
        return recordMapper.selectByOrderId(orderId);
    }

    // ==================== 内部方法 ====================

    private InventoryPriceVO toVO(InventoryPriceStock stock) {
        InventoryPriceVO vo = new InventoryPriceVO();
        vo.setId(stock.getId());
        vo.setHotelId(stock.getHotelId());
        vo.setRoomType(stock.getRoomType());
        vo.setBizDate(stock.getBizDate());
        vo.setSellPrice(stock.getSellPrice());
        vo.setCurrency(stock.getCurrency());
        vo.setTotal(stock.getTotal());
        vo.setFrozen(stock.getFrozen());
        vo.setBooked(stock.getBooked());
        vo.setAvailable(stock.getAvailable());
        return vo;
    }

    private String buildCacheKey(Long hotelId, String roomType, LocalDate date) {
        return CACHE_KEY_PREFIX + hotelId + ":" + roomType + ":" + date;
    }

    private void evictCache(Long hotelId, String roomType, LocalDate date) {
        String key = buildCacheKey(hotelId, roomType, date);
        cacheService.evict(key);
    }
}
```

## 4.8 Controller 层

```java
package com.ota.inventory.controller;

import com.ota.inventory.dto.InventoryOperateDTO;
import com.ota.inventory.entity.InventoryRecord;
import com.ota.inventory.service.InventoryService;
import com.ota.inventory.vo.InventoryPriceVO;
import com.ota.inventory.vo.InventoryResultVO;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.List;

@RestController
@RequestMapping("/inventory")
@RequiredArgsConstructor
public class InventoryController {

    private final InventoryService inventoryService;

    /**
     * 查询价库
     * GET /inventory/price/query?hotelId=1&roomType=king_double&startDate=2026-06-01&endDate=2026-06-05
     */
    @GetMapping("/price/query")
    public ResponseEntity<List<InventoryPriceVO>> queryPrice(
            @RequestParam Long hotelId,
            @RequestParam String roomType,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate startDate,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate endDate) {

        List<InventoryPriceVO> result = inventoryService
                .queryPriceAndStock(hotelId, roomType, startDate, endDate);
        return ResponseEntity.ok(result);
    }

    /**
     * 查询单天价库（走缓存）
     * GET /inventory/price/single?hotelId=1&roomType=king_double&bizDate=2026-06-01
     */
    @GetMapping("/price/single")
    public ResponseEntity<InventoryPriceVO> querySingle(
            @RequestParam Long hotelId,
            @RequestParam String roomType,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate bizDate) {

        InventoryPriceVO result = inventoryService
                .querySingleDay(hotelId, roomType, bizDate);
        if (result == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(result);
    }

    /**
     * 冻结库存
     * POST /inventory/freeze
     */
    @PostMapping("/freeze")
    public ResponseEntity<InventoryResultVO> freeze(
            @Valid @RequestBody InventoryOperateDTO dto) {
        return ResponseEntity.ok(inventoryService.freeze(dto));
    }

    /**
     * 释放库存
     * POST /inventory/release
     */
    @PostMapping("/release")
    public ResponseEntity<InventoryResultVO> release(
            @Valid @RequestBody InventoryOperateDTO dto) {
        return ResponseEntity.ok(inventoryService.release(dto));
    }

    /**
     * 扣减库存
     * POST /inventory/deduct
     */
    @PostMapping("/deduct")
    public ResponseEntity<InventoryResultVO> deduct(
            @Valid @RequestBody InventoryOperateDTO dto) {
        return ResponseEntity.ok(inventoryService.deduct(dto));
    }

    /**
     * 查询订单库存流水
     * GET /inventory/record/ORDER001
     */
    @GetMapping("/record/{orderId}")
    public ResponseEntity<List<InventoryRecord>> getRecord(
            @PathVariable String orderId) {
        return ResponseEntity.ok(inventoryService.getRecordsByOrderId(orderId));
    }
}
```

## 4.9 乐观锁原理详解

### 4.9.1 为什么用乐观锁而不是悲观锁？

| 对比 | 乐观锁 | 悲观锁（SELECT ... FOR UPDATE） |
|------|--------|-------------------------------|
| 性能 | 高，无锁等待 | 低，锁等待阻塞其他事务 |
| 死锁风险 | 无 | 有，尤其跨天操作时 |
| 并发度 | 高，版本冲突自动重试 | 低，同一行只能一个事务操作 |
| 适用场景 | 读多写少，冲突概率低 | 写冲突频繁的场景 |

酒店库存场景：同一个日期房型的并发修改概率不高（同一订单只会冻结一次），适合乐观锁。

### 4.9.2 乐观锁工作原理

**第1次请求（成功）**：
```sql
-- 查询：version=0
SELECT * FROM inventory_price_stock WHERE id = 100;
-- version = 0

-- 更新：自动检查版本，version+1
UPDATE inventory_price_stock
SET frozen = frozen + 1, available = available - 1, version = version + 1
WHERE id = 100 AND version = 0 AND available >= 1;
-- affected rows = 1（成功）
```

**第2次请求（并发冲突）**：
```sql
-- 查询：但此时version已经变为1
SELECT * FROM inventory_price_stock WHERE id = 100;
-- version = 1

-- 更新：用旧版本号更新，WHERE条件不满足
UPDATE inventory_price_stock
SET frozen = frozen + 1, available = available - 1, version = version + 1
WHERE id = 100 AND version = 0 AND available >= 1;
-- affected rows = 0（失败，版本不一致）
```

### 4.9.3 代码中的重试机制

```java
// 第1次尝试
int affected = priceStockMapper.freezeById(
        stock.getId(), stock.getVersion(), dto.getQuantity());

if (affected == 0) {
    // 第2次尝试：重新读取最新版本
    InventoryPriceStock refreshed = priceStockMapper.selectById(stock.getId());
    affected = priceStockMapper.freezeById(
            refreshed.getId(), refreshed.getVersion(), dto.getQuantity());
}
```

这里只重试一次而不是循环重试，因为：
1. 库存操作通常是短事务，冲突概率低
2. 连续两次冲突说明并发极高，应该快速失败让调用方处理
3. 重试时重新查询数据库，确保读到最新版本

## 4.10 验证步骤

### 4.10.1 查询价库

```bash
# 查询酒店1大床房2026-06-01~2026-06-03的价库
curl -s "http://localhost:8085/inventory/price/query?hotelId=1&roomType=king_double&startDate=2026-06-01&endDate=2026-06-03" | python3 -m json.tool
```

返回：
```json
[
  {
    "id": 1,
    "hotelId": 1,
    "roomType": "king_double",
    "bizDate": "2026-06-01",
    "sellPrice": 888.00,
    "total": 30,
    "frozen": 0,
    "booked": 0,
    "available": 30
  },
  {
    "id": 2,
    "hotelId": 1,
    "roomType": "king_double",
    "bizDate": "2026-06-02",
    "sellPrice": 888.00,
    "total": 30,
    "frozen": 0,
    "booked": 0,
    "available": 30
  }
]
```

### 4.10.2 查询单天（验证缓存）

```bash
# 第一次查询（慢，读DB+写入缓存）
time curl -s "http://localhost:8085/inventory/price/single?hotelId=1&roomType=king_double&bizDate=2026-06-01"

# 第二次查询（快，走缓存）
time curl -s "http://localhost:8085/inventory/price/single?hotelId=1&roomType=king_double&bizDate=2026-06-01"
```

### 4.10.3 冻结库存

```bash
# 冻结1间大床房，6月1日~6月3日入住
curl -s -X POST http://localhost:8085/inventory/freeze \
  -H "Content-Type: application/json" \
  -d '{
    "hotelId": 1,
    "roomType": "king_double",
    "startDate": "2026-06-01",
    "endDate": "2026-06-03",
    "quantity": 1,
    "orderId": "ORDER001"
  }' | python3 -m json.tool
```

返回：
```json
{
  "success": true,
  "message": "全部冻结成功",
  "details": [
    {"bizDate": "2026-06-01", "success": true, "message": "冻结成功"},
    {"bizDate": "2026-06-02", "success": true, "message": "冻结成功"}
  ]
}
```

### 4.10.4 确认冻结已生效

```bash
# 再次查询，确认frozen从0变为1，available从30变为29
curl -s "http://localhost:8085/inventory/price/single?hotelId=1&roomType=king_double&bizDate=2026-06-01" | python3 -m json.tool
```

```json
{
  "frozen": 1,
  "available": 29,
  "total": 30
}
```

### 4.10.5 释放库存

```bash
# 释放刚才冻结的库存
curl -s -X POST http://localhost:8085/inventory/release \
  -H "Content-Type: application/json" \
  -d '{
    "hotelId": 1,
    "roomType": "king_double",
    "startDate": "2026-06-01",
    "endDate": "2026-06-03",
    "quantity": 1,
    "orderId": "ORDER001"
  }' | python3 -m json.tool
```

确认释放后 frozen=0, available=30 恢复原状。

### 4.10.6 扣减库存

```bash
# 先冻结
curl -s -X POST http://localhost:8085/inventory/freeze \
  -H "Content-Type: application/json" \
  -d '{
    "hotelId": 1, "roomType": "king_double",
    "startDate": "2026-06-01", "endDate": "2026-06-03",
    "quantity": 1, "orderId": "ORDER002"
  }'

# 再扣减
curl -s -X POST http://localhost:8085/inventory/deduct \
  -H "Content-Type: application/json" \
  -d '{
    "hotelId": 1, "roomType": "king_double",
    "startDate": "2026-06-01", "endDate": "2026-06-03",
    "quantity": 1, "orderId": "ORDER002"
  }' | python3 -m json.tool
```

确认扣减后 frozen=0, booked=1, available=29。

### 4.10.7 查询流水

```bash
# 查询 ORDER001 的库存流水
curl -s http://localhost:8085/inventory/record/ORDER001 | python3 -m json.tool
```

返回：
```json
[
  {
    "id": 1,
    "orderId": "ORDER001",
    "hotelId": 1,
    "roomType": "king_double",
    "startDate": "2026-06-01",
    "endDate": "2026-06-03",
    "quantity": 1,
    "status": "FROZEN",
    "remark": "库存冻结"
  },
  {
    "id": 2,
    "orderId": "ORDER001",
    "hotelId": 1,
    "roomType": "king_double",
    "startDate": "2026-06-01",
    "endDate": "2026-06-03",
    "quantity": 1,
    "status": "RELEASED",
    "remark": "库存释放"
  }
]
```

### 4.10.8 验证乐观锁并发冲突

```bash
# 模拟并发：同时发送两个冻结请求
# 终端1：先触发，但不释放
curl -s -X POST http://localhost:8085/inventory/freeze \
  -H "Content-Type: application/json" \
  -d '{
    "hotelId": 1, "roomType": "king_double",
    "startDate": "2026-06-05", "endDate": "2026-06-06",
    "quantity": 30, "orderId": "ORDER003"
  }'

# 终端2：也请求同样30间（已无库存）
curl -s -X POST http://localhost:8085/inventory/freeze \
  -H "Content-Type: application/json" \
  -d '{
    "hotelId": 1, "roomType": "king_double",
    "startDate": "2026-06-05", "endDate": "2026-06-06",
    "quantity": 30, "orderId": "ORDER004"
  }'
```

第2个请求应该返回 `success=false`，`message="库存不足，可用库存: 0"`。

## 4.11 常见坑

### ⚠️ 坑1：乐观锁 UPDATE 返回 0 时的处理

```java
// ❌ 错误写法：不管 affected == 0，认为操作成功
priceStockMapper.freezeById(stock.getId(), stock.getVersion(), qty);
// 实际上可能因为版本冲突而失败

// ✅ 正确写法：检查 affected rows
int affected = priceStockMapper.freezeById(stock.getId(), stock.getVersion(), qty);
if (affected == 0) {
    // 重新读取最新版本重试
    // 或抛出异常让调用方处理
    throw new OptimisticLockException("库存已被其他操作修改");
}
```

### ⚠️ 坑2：跨天操作的原子性

冻结/释放/扣减都是对日期范围内的**每一天**逐天操作。如果第1天成功、第2天失败，事务回滚但有些天的 `available` 已经减少：

```java
// ✅ 使用 @Transactional 保证全部成功或全部回滚
@Transactional(rollbackFor = Exception.class)
public InventoryResultVO freeze(InventoryOperateDTO dto) {
    // 循环中任何一个操作失败，整个事务回滚
}
```

### ⚠️ 坑3：库存不足假象（缓存一致性问题）

如果缓存 TTL 期间更新了库存，缓存还是旧值：

```java
// 每次写操作后立即删缓存
private void evictCache(Long hotelId, String roomType, LocalDate date) {
    String key = buildCacheKey(hotelId, roomType, date);
    cacheService.evict(key);  // 删缓存，下次查询强制读DB
}
```

**缓存 vs DB 的不一致窗口**：删缓存后 → 下一个查询之前，如果另一个线程更新了DB，缓存不存在所以会读DB，获取最新数据。这个窗口非常短（毫秒级），业务上可接受。

### ⚠️ 坑4：Redis DB 隔离

酒店和价库服务使用不同的 Redis Database：

```yaml
# ota-hotel: database 0
spring.data.redis.database: 0

# ota-inventory: database 1
spring.data.redis.database: 1
```

防止 key 冲突。生产环境应使用独立的 Redis 实例或 key 前缀隔离。

### ⚠️ 坑5：使用 MyBatis-Plus @Version 的注意事项

如果使用 MyBatis-Plus 自动乐观锁（实体上标注 @Version），**自定义 SQL（如 freezeById）不会自动拼接版本条件**。需要手动写：

```sql
-- 必须手动加 WHERE version = #{version}
UPDATE inventory_price_stock SET ...
WHERE id = #{id} AND version = #{version} AND available >= #{quantity}
```

乐观锁插件只对 MyBatis-Plus 内置的 `updateById` 等自动方法生效。

### ⚠️ 坑6：BigDecimal 精度

价格字段使用 `DECIMAL(10,2)`，对应的 Java 类型必须用 `BigDecimal`：

```java
// ❌ 错误：用 Double 或 Float 存储价格
private Double sellPrice;  // 精度丢失！

// ✅ 正确：用 BigDecimal
private BigDecimal sellPrice;  // 精确十进制
```

### ⚠️ 坑7：库存扣减的幂等性

如果扣减接口被重复调用（比如网络重试），会导致重复扣减。解决方案：

```java
// 方案1：调用方保证 orderId 唯一，每次扣减前检查流水
@Transactional
public InventoryResultVO deduct(InventoryOperateDTO dto) {
    // 检查是否已经扣减过
    List<InventoryRecord> existing = recordMapper
            .selectByOrderId(dto.getOrderId());
    boolean alreadyDeducted = existing.stream()
            .anyMatch(r -> "DEDUCTED".equals(r.getStatus()));
    if (alreadyDeducted) {
        return InventoryResultVO.builder()
                .success(true)
                .message("已扣减，无需重复操作")
                .build();
    }
    // ... 执行扣减
}
```

## 4.12 分表预留设计（P2 阶段）

P0 阶段使用单表。P2 阶段用 ShardingSphere 做分表，提前预留：

```sql
-- P2 分表策略：
-- 逻辑表: inventory_price_stock
-- 实际表: inventory_price_stock_00 ~ inventory_price_stock_99
-- 分片键: hotel_id % 100
-- 分片算法: MOD(hotel_id, 100)
```

Service 层已经做了面向实体的编程，切换到分表时只需：
1. 引入 ShardingSphere 依赖
2. 配置分片规则（`actual-data-nodes`、`sharding-column`、`algorithm-expression`）
3. **Mapper 和 Service 代码不需要改**

## 4.13 本章总结

| 知识点 | 实现方式 |
|--------|---------|
| 价库合一表 | inventory_price_stock，UK(hotel_id, room_type, biz_date) |
| 乐观锁 | version 字段 + WHERE version = oldVersion |
| 库存状态机 | AVAILABLE → FROZEN → DEDUCTED / RELEASED |
| 查询缓存 | Cache-Aside，key=ps:{hotelId}:{roomType}:{date}，TTL 5min |
| 写后删缓存 | 每次 freeze/release/deduct 后立即 evict cache |
| 库存流水 | inventory_record，记录完整的库存变更历史 |
| 跨天操作 | @Transactional 逐天操作，全部成功或回滚 |
| 分表预留 | hotel_id % 100，P2 接入 ShardingSphere |

---

**补充变更**：订单服务前追加 [第4章补充：Admin 中台配置后端一期](./chapter04b-admin-service.md)，新增 `ota-admin`，管理下游渠道、可售范围/加价系数、酒店来源候选映射，并给 `inventory_price_stock` 补 `source_type/source_code/source_ref_id`。

**下一步**：进入第5章，实现 **ota-order 订单服务**，通过 Feign 调用 inventory 接口，完成完整的预订下单流程。
