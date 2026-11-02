# 第3章：Hotel 酒店服务与 Redis 缓存策略

## 3.1 本章目标

完成 `ota-hotel`（端口 8083）服务的核心酒店数据接口开发，实现：

- 酒店列表分页查询 + 城市筛选
- 酒店详情（含房型列表）查询
- 多条件酒店搜索
- **Redis Cache-Aside 缓存策略**：穿透防护、击穿防护（互斥锁）、缓存更新
- JDK 21 虚拟线程配置

## 3.2 数据库表设计

### 3.2.1 hotel（酒店表）

```sql
CREATE TABLE `hotel` (
  `id`          BIGINT       NOT NULL AUTO_INCREMENT COMMENT '酒店ID',
  `name`        VARCHAR(128) NOT NULL COMMENT '酒店名称',
  `city_id`     INT          NOT NULL COMMENT '所在城市ID',
  `city_name`   VARCHAR(64)  NOT NULL COMMENT '城市名称',
  `address`     VARCHAR(256) NOT NULL DEFAULT '' COMMENT '详细地址',
  `star_rating` TINYINT      NOT NULL DEFAULT 0 COMMENT '星级：1-5',
  `phone`       VARCHAR(32)  NOT NULL DEFAULT '' COMMENT '联系电话',
  `description` VARCHAR(512) NOT NULL DEFAULT '' COMMENT '酒店描述',
  `status`      TINYINT      NOT NULL DEFAULT 1 COMMENT '状态：0下架 1上架',
  `created_at`  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_city_id` (`city_id`),
  KEY `idx_star_rating` (`star_rating`),
  KEY `idx_status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='酒店表';
```

### 3.2.2 room_type（房型表）

```sql
CREATE TABLE `room_type` (
  `id`           BIGINT       NOT NULL AUTO_INCREMENT COMMENT '房型ID',
  `hotel_id`     BIGINT       NOT NULL COMMENT '所属酒店ID',
  `name`         VARCHAR(64)  NOT NULL COMMENT '房型名称（如：大床房、双床房）',
  `max_occupancy` TINYINT     NOT NULL DEFAULT 2 COMMENT '最大入住人数',
  `bed_type`     VARCHAR(32)  NOT NULL DEFAULT '' COMMENT '床型（1.8m大床/1.2m双床）',
  `area`         INT          NOT NULL DEFAULT 0 COMMENT '房间面积（平方米）',
  `window`       TINYINT      NOT NULL DEFAULT 1 COMMENT '有无窗户：0无 1有',
  `breakfast`    TINYINT      NOT NULL DEFAULT 0 COMMENT '含早餐：0无 1有',
  `sort_order`   INT          NOT NULL DEFAULT 0 COMMENT '排序号',
  `created_at`   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_hotel_id` (`hotel_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='房型表';
```

### 3.2.3 初始化数据

```sql
-- 酒店数据
INSERT INTO hotel (id, name, city_id, city_name, address, star_rating, phone, description) VALUES
(1, '深圳湾万丽酒店', 4403, '深圳', '南山区科技南路18号', 5, '0755-88888888', '深圳湾畔高端商务酒店，毗邻科技园'),
(2, '深圳南山希尔顿花园', 4403, '深圳', '南山区登良路22号', 4, '0755-87777777', '南山核心区商务酒店'),
(3, '北京王府井希尔顿', 1101, '北京', '东城区王府井东街8号', 5, '010-88886666', '王府井核心商圈豪华酒店'),
(4, '上海浦东香格里拉', 3101, '上海', '浦东新区富城路33号', 5, '021-88889999', '陆家嘴金融区江景酒店');

-- 房型数据
INSERT INTO room_type (hotel_id, name, max_occupancy, bed_type, area, window, breakfast, sort_order) VALUES
(1, '高级大床房', 2, '1.8m大床', 38, 1, 1, 1),
(1, '豪华双床房', 2, '1.2m双床', 45, 1, 1, 2),
(1, '行政套房', 3, '2.0m大床', 65, 1, 1, 3),
(2, '标准大床房', 2, '1.8m大床', 30, 1, 0, 1),
(2, '标准双床房', 2, '1.2m双床', 35, 1, 0, 2),
(3, '豪华大床房', 2, '1.8m大床', 45, 1, 1, 1),
(3, '豪华套房', 3, '2.0m大床', 70, 1, 1, 2),
(4, '江景大床房', 2, '1.8m大床', 50, 1, 1, 1),
(4, '江景双床房', 2, '1.2m双床', 55, 1, 1, 2),
(4, '总统套房', 4, '2.0m大床+1.5m双床', 120, 1, 1, 3);
```

## 3.3 项目结构与依赖

### 3.3.1 pom.xml（关键依赖）

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

    <artifactId>ota-hotel</artifactId>
    <version>1.0.0-SNAPSHOT</version>

    <dependencies>
        <!-- Spring Boot Web -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>

        <!-- Spring Data Redis -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-redis</artifactId>
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

        <!-- Nacos 服务发现 -->
        <dependency>
            <groupId>com.alibaba.cloud</groupId>
            <artifactId>spring-cloud-starter-alibaba-nacos-discovery</artifactId>
        </dependency>

        <!-- Redis连接池 -->
        <dependency>
            <groupId>org.apache.commons</groupId>
            <artifactId>commons-pool2</artifactId>
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

### 3.3.2 application.yml

```yaml
server:
  port: 8083

spring:
  application:
    name: ota-hotel

  # JDK 21 虚拟线程
  threads:
    virtual:
      enabled: true

  datasource:
    url: jdbc:mysql://localhost:3306/ota_hotel?useUnicode=true&characterEncoding=utf8mb4&useSSL=false&serverTimezone=Asia/Shanghai
    username: root
    password: root123456
    driver-class-name: com.mysql.cj.jdbc.Driver
    hikari:
      maximum-pool-size: 20
      minimum-idle: 5
      # 虚拟线程模式下，设置更大的超时时间
      connection-timeout: 10000

  data:
    redis:
      host: localhost
      port: 6379
      password: redis123456
      database: 0
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
        namespace: public

mybatis-plus:
  configuration:
    # 开启驼峰命名映射
    map-underscore-to-camel-case: true
    # 打印SQL日志（开发环境）
    log-impl: org.apache.ibatis.logging.stdout.StdOutImpl
  global-config:
    db-config:
      id-type: auto
      logic-delete-field: deleted
      logic-delete-value: 1
      logic-not-delete-value: 0

logging:
  level:
    com.ota.hotel: debug
```

### 3.3.3 启动类

```java
package com.ota.hotel;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class HotelApplication {

    public static void main(String[] args) {
        SpringApplication.run(HotelApplication.class, args);
    }
}
```

## 3.4 实体类

### 3.4.1 Hotel 实体

```java
package com.ota.hotel.entity;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@TableName("hotel")
public class Hotel {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String name;

    private Integer cityId;

    private String cityName;

    private String address;

    /**
     * 星级：1-5
     */
    @TableField("star_rating")
    private Integer starRating;

    private String phone;

    private String description;

    /**
     * 状态：0下架 1上架
     */
    private Integer status;

    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createdAt;

    @TableField(fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updatedAt;
}
```

### 3.4.2 RoomType 实体

```java
package com.ota.hotel.entity;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@TableName("room_type")
public class RoomType {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long hotelId;

    private String name;

    private Integer maxOccupancy;

    private String bedType;

    private Integer area;

    /**
     * 有无窗户：0无 1有
     */
    private Integer window;

    /**
     * 含早餐：0无 1有
     */
    private Integer breakfast;

    private Integer sortOrder;

    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createdAt;

    @TableField(fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updatedAt;
}
```

### 3.4.3 VO（视图对象）

```java
package com.ota.hotel.vo;

import lombok.Data;
import java.util.List;

/**
 * 酒店详情VO（含房型列表）
 */
@Data
public class HotelDetailVO {
    private Long id;
    private String name;
    private Integer cityId;
    private String cityName;
    private String address;
    private Integer starRating;
    private String phone;
    private String description;
    private List<RoomTypeVO> roomTypes;
}

@Data
class RoomTypeVO {
    private Long id;
    private String name;
    private Integer maxOccupancy;
    private String bedType;
    private Integer area;
    private Integer window;
    private Integer breakfast;
}
```

```java
package com.ota.hotel.vo;

import lombok.Data;

/**
 * 酒店查询请求VO
 */
@Data
public class HotelSearchVO {
    private String keyword;       // 关键字（匹配酒店名和地址）
    private Integer cityId;       // 城市ID
    private Integer minStar;      // 最低星级
    private Integer maxStar;      // 最高星级
    private Integer page = 1;
    private Integer size = 10;
}
```

## 3.5 Mapper 层

### 3.5.1 HotelMapper

```java
package com.ota.hotel.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.ota.hotel.entity.Hotel;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface HotelMapper extends BaseMapper<Hotel> {

    /**
     * 分页 + 城市筛选（自定义SQL示例，复杂查询时可以用XML）
     */
    IPage<Hotel> selectPageByCity(Page<Hotel> page, @Param("cityId") Integer cityId);

    /**
     * 多条件搜索
     */
    IPage<Hotel> searchHotels(Page<Hotel> page,
                              @Param("keyword") String keyword,
                              @Param("cityId") Integer cityId,
                              @Param("minStar") Integer minStar,
                              @Param("maxStar") Integer maxStar);
}
```

### 3.5.2 HotelMapper.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE mapper PUBLIC "-//mybatis.org//DTD Mapper 3.0//EN"
        "http://mybatis.org/dtd/mybatis-3-mapper.dtd">
<mapper namespace="com.ota.hotel.mapper.HotelMapper">

    <select id="selectPageByCity" resultType="com.ota.hotel.entity.Hotel">
        SELECT * FROM hotel
        WHERE status = 1
        <if test="cityId != null and cityId > 0">
            AND city_id = #{cityId}
        </if>
        ORDER BY star_rating DESC, id ASC
    </select>

    <select id="searchHotels" resultType="com.ota.hotel.entity.Hotel">
        SELECT * FROM hotel
        WHERE status = 1
        <if test="keyword != null and keyword != ''">
            AND (name LIKE CONCAT('%', #{keyword}, '%')
                 OR address LIKE CONCAT('%', #{keyword}, '%'))
        </if>
        <if test="cityId != null and cityId > 0">
            AND city_id = #{cityId}
        </if>
        <if test="minStar != null">
            AND star_rating >= #{minStar}
        </if>
        <if test="maxStar != null">
            AND star_rating &lt;= #{maxStar}
        </if>
        ORDER BY star_rating DESC, id ASC
    </select>

</mapper>
```

### 3.5.3 RoomTypeMapper

```java
package com.ota.hotel.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.ota.hotel.entity.RoomType;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface RoomTypeMapper extends BaseMapper<RoomType> {

    @Select("SELECT * FROM room_type WHERE hotel_id = #{hotelId} ORDER BY sort_order ASC")
    List<RoomType> selectByHotelId(Long hotelId);
}
```

### 3.5.4 MyBatis-Plus 分页插件配置

```java
package com.ota.hotel.config;

import com.baomidou.mybatisplus.annotation.DbType;
import com.baomidou.mybatisplus.extension.plugins.MybatisPlusInterceptor;
import com.baomidou.mybatisplus.extension.plugins.inner.PaginationInnerInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MyBatisPlusConfig {

    @Bean
    public MybatisPlusInterceptor mybatisPlusInterceptor() {
        MybatisPlusInterceptor interceptor = new MybatisPlusInterceptor();
        PaginationInnerInterceptor pagination = new PaginationInnerInterceptor(DbType.MYSQL);
        pagination.setMaxLimit(500L);
        interceptor.addInnerInterceptor(pagination);
        return interceptor;
    }
}
```

## 3.6 Redis 缓存服务

这是本章的核心 —— 实现健壮的缓存层。

### 3.6.1 Redis 配置类

```java
package com.ota.hotel.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfig {

    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory factory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(factory);

        // key 使用 String 序列化
        StringRedisSerializer keySerializer = new StringRedisSerializer();
        template.setKeySerializer(keySerializer);
        template.setHashKeySerializer(keySerializer);

        // value 使用 JSON 序列化（支持 Java 8 时间类型）
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        GenericJackson2JsonRedisSerializer valueSerializer =
                new GenericJackson2JsonRedisSerializer(objectMapper);
        template.setValueSerializer(valueSerializer);
        template.setHashValueSerializer(valueSerializer);

        template.afterPropertiesSet();
        return template;
    }
}
```

### 3.6.2 Redis 缓存服务（核心）

```java
package com.ota.hotel.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 通用Redis缓存服务
 * <p>
 * 实现 Cache-Aside 模式，包含：
 * - 缓存穿透防护（空值缓存）
 * - 缓存击穿防护（互斥锁）
 * - 缓存雪崩防护（TTL随机抖动）
 * - 先写DB再删缓存（更新策略）
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CacheService {

    private final RedisTemplate<String, Object> redisTemplate;

    /**
     * 缓存空值的特殊标记
     */
    private static final String NULL_CACHE = "NULL";

    /**
     * 互斥锁前缀
     */
    private static final String LOCK_PREFIX = "lock:";

    /**
     * 默认TTL：30分钟
     */
    private static final long DEFAULT_TTL_MINUTES = 30;

    /**
     * 最大随机抖动：5分钟
     */
    private static final long MAX_JITTER_SECONDS = 300;

    /**
     * 获取锁的超时时间（毫秒）
     */
    private static final long LOCK_TIMEOUT_MS = 2000;

    /**
     * 核心查询方法：Cache-Aside 模式 + 互斥锁防击穿
     *
     * @param key       Redis key
     * @param type      返回类型
     * @param dbLoader  数据库加载逻辑（Supplier，缓存miss时调用）
     * @param ttlMinutes TTL（分钟），null则使用默认30min
     * @param <T>       泛型类型
     * @return 查询结果，可能为null（数据库中也没有）
     */
    @SuppressWarnings("unchecked")
    public <T> T queryWithCacheAside(String key, Class<T> type,
                                     Supplier<T> dbLoader, Long ttlMinutes) {
        // 第1步：查缓存
        Object cached = redisTemplate.opsForValue().get(key);
        if (cached != null) {
            // 命中缓存空值 → 直接返回null，避免穿透到DB
            if (NULL_CACHE.equals(cached)) {
                log.debug("[Cache] 命中空值缓存: key={}", key);
                return null;
            }
            log.debug("[Cache] 命中缓存: key={}", key);
            return (T) cached;
        }

        log.debug("[Cache] 缓存未命中: key={}，尝试获取互斥锁", key);

        // 第2步：缓存未命中，尝试获取互斥锁（防击穿）
        String lockKey = LOCK_PREFIX + key;
        String lockValue = String.valueOf(System.nanoTime());

        try {
            Boolean locked = redisTemplate.opsForValue()
                    .setIfAbsent(lockKey, lockValue, LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            if (Boolean.TRUE.equals(locked)) {
                // 拿到锁 → 加载数据
                try {
                    // 双重检查：拿到锁后再次检查缓存（防止等待锁的期间数据已被加载）
                    cached = redisTemplate.opsForValue().get(key);
                    if (cached != null) {
                        if (NULL_CACHE.equals(cached)) {
                            return null;
                        }
                        return (T) cached;
                    }

                    // 从DB加载
                    T result = dbLoader.get();
                    if (result != null) {
                        // 写入缓存（带随机TTL防雪崩）
                        long ttl = (ttlMinutes != null ? ttlMinutes : DEFAULT_TTL_MINUTES);
                        long ttlWithJitter = ttl * 60 + (long) (Math.random() * MAX_JITTER_SECONDS);
                        redisTemplate.opsForValue()
                                .set(key, result, ttlWithJitter, TimeUnit.SECONDS);
                        log.debug("[Cache] 写入缓存: key={}, TTL={}s", key, ttlWithJitter);
                    } else {
                        // 空值缓存：防止缓存穿透
                        long nullTtl = 60L; // 空值缓存60秒
                        redisTemplate.opsForValue()
                                .set(key, NULL_CACHE, nullTtl, TimeUnit.SECONDS);
                        log.debug("[Cache] 写入空值缓存: key={}, TTL={}s", key, nullTtl);
                    }
                    return result;
                } finally {
                    // 释放锁
                    String currentValue = (String) redisTemplate.opsForValue().get(lockKey);
                    if (lockValue.equals(currentValue)) {
                        redisTemplate.delete(lockKey);
                    }
                }
            } else {
                // 没拿到锁 → 自旋等待（最多重试3次）
                log.debug("[Cache] 未获取到锁，自旋等待: key={}", key);
                for (int i = 0; i < 3; i++) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(200);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    cached = redisTemplate.opsForValue().get(key);
                    if (cached != null) {
                        if (NULL_CACHE.equals(cached)) {
                            return null;
                        }
                        return (T) cached;
                    }
                }
                // 自旋超时，降级查DB（不做缓存）
                log.warn("[Cache] 自旋超时降级查DB: key={}", key);
                return dbLoader.get();
            }
        } finally {
            // 清理锁（确保不残留）
            String currentValue = (String) redisTemplate.opsForValue().get(lockKey);
            if (lockValue.equals(currentValue)) {
                redisTemplate.delete(lockKey);
            }
        }
    }

    /**
     * 更新缓存策略：先写DB，再删缓存
     */
    public void evictCache(String key) {
        redisTemplate.delete(key);
        log.debug("[Cache] 删除缓存: key={}", key);
    }
}
```

### 3.6.3 缓存常量

```java
package com.ota.hotel.constant;

/**
 * Redis Key 常量
 */
public interface CacheConstants {

    /**
     * 酒店详情缓存
     * key格式：hotel:detail:{hotelId}
     * TTL：30分钟 + 随机抖动
     */
    String HOTEL_DETAIL_KEY = "hotel:detail:";
}
```

## 3.7 Service 层

### 3.7.1 HotelService

```java
package com.ota.hotel.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.ota.hotel.constant.CacheConstants;
import com.ota.hotel.entity.Hotel;
import com.ota.hotel.entity.RoomType;
import com.ota.hotel.mapper.HotelMapper;
import com.ota.hotel.vo.HotelDetailVO;
import com.ota.hotel.vo.HotelSearchVO;
import com.ota.hotel.vo.RoomTypeVO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class HotelService extends ServiceImpl<HotelMapper, Hotel> {

    private final CacheService cacheService;
    private final RoomTypeMapper roomTypeMapper;

    /**
     * 分页查询酒店列表（按城市筛选）
     */
    public IPage<Hotel> listByCity(Integer cityId, int page, int size) {
        Page<Hotel> pageParam = new Page<>(page, size);
        return baseMapper.selectPageByCity(pageParam, cityId);
    }

    /**
     * 查询酒店详情（含房型）+ Redis缓存
     */
    public HotelDetailVO getDetailWithCache(Long hotelId) {
        String cacheKey = CacheConstants.HOTEL_DETAIL_KEY + hotelId;

        return cacheService.queryWithCacheAside(
                cacheKey,
                HotelDetailVO.class,
                () -> loadHotelDetailFromDB(hotelId),
                30L  // TTL 30分钟
        );
    }

    /**
     * 从数据库加载酒店详情
     */
    private HotelDetailVO loadHotelDetailFromDB(Long hotelId) {
        log.info("[DB] 查询酒店详情: hotelId={}", hotelId);

        Hotel hotel = getById(hotelId);
        if (hotel == null) {
            return null;
        }

        List<RoomType> roomTypes = roomTypeMapper.selectByHotelId(hotelId);

        HotelDetailVO detail = new HotelDetailVO();
        BeanUtils.copyProperties(hotel, detail);

        if (roomTypes != null && !roomTypes.isEmpty()) {
            detail.setRoomTypes(roomTypes.stream().map(rt -> {
                RoomTypeVO vo = new RoomTypeVO();
                BeanUtils.copyProperties(rt, vo);
                return vo;
            }).collect(Collectors.toList()));
        } else {
            detail.setRoomTypes(Collections.emptyList());
        }

        return detail;
    }

    /**
     * 更新酒店信息（先写DB，再删缓存）
     */
    public boolean updateAndEvictCache(Hotel hotel) {
        boolean updated = updateById(hotel);
        if (updated) {
            cacheService.evictCache(CacheConstants.HOTEL_DETAIL_KEY + hotel.getId());
        }
        return updated;
    }

    /**
     * 多条件酒店搜索
     */
    public IPage<Hotel> search(HotelSearchVO searchVO) {
        Page<Hotel> page = new Page<>(searchVO.getPage(), searchVO.getSize());
        return baseMapper.searchHotels(
                page,
                searchVO.getKeyword(),
                searchVO.getCityId(),
                searchVO.getMinStar(),
                searchVO.getMaxStar()
        );
    }
}
```

## 3.8 Controller 层

```java
package com.ota.hotel.controller;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.ota.hotel.entity.Hotel;
import com.ota.hotel.service.HotelService;
import com.ota.hotel.vo.HotelDetailVO;
import com.ota.hotel.vo.HotelSearchVO;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/hotel")
@RequiredArgsConstructor
public class HotelController {

    private final HotelService hotelService;

    /**
     * 酒店列表（分页 + 城市筛选）
     * GET /hotel/list?cityId=4403&page=1&size=10
     */
    @GetMapping("/list")
    public ResponseEntity<IPage<Hotel>> list(
            @RequestParam(required = false) Integer cityId,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "10") int size) {
        return ResponseEntity.ok(hotelService.listByCity(cityId, page, size));
    }

    /**
     * 酒店详情（含房型列表 + Redis缓存）
     * GET /hotel/{id}
     */
    @GetMapping("/{id}")
    public ResponseEntity<HotelDetailVO> detail(@PathVariable Long id) {
        HotelDetailVO detail = hotelService.getDetailWithCache(id);
        if (detail == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(detail);
    }

    /**
     * 多条件酒店搜索
     * POST /hotel/search
     */
    @PostMapping("/search")
    public ResponseEntity<IPage<Hotel>> search(@RequestBody HotelSearchVO searchVO) {
        return ResponseEntity.ok(hotelService.search(searchVO));
    }
}
```

## 3.9 虚拟线程详解

### 3.9.1 配置说明

`spring.threads.virtual.enabled=true` 是 Spring Boot 3.2+ 引入的配置。启用后：

1. **Tomcat 请求处理线程** → 虚拟线程（之前是平台线程池）
2. **@Async 异步方法** → 虚拟线程
3. **@Scheduled 定时任务** → 虚拟线程

这意味着每个请求不再需要从 200 线程池中抢一个平台线程，而是可以轻松创建成千上万虚拟线程。

### 3.9.2 虚拟线程下的注意事项

```java
// ❌ 错误：虚拟线程中避免 synchronized
public synchronized void doSomething() {
    // synchronized 会钉住虚拟线程的载体线程
}

// ✅ 正确：改用 ReentrantLock
private final Lock lock = new ReentrantLock();
public void doSomething() {
    lock.lock();
    try {
        // ...
    } finally {
        lock.unlock();
    }
}
```

```properties
# ❌ 错误：虚拟线程中避免 ThreadPoolExecutor 做 IO 任务
# 如果确实需要线程池，使用虚拟线程工厂
spring.task.execution.pool.max-size=10  # 虚拟线程下此配置失效

# ✅ 虚拟线程的线程池配置使用虚拟线程工厂
```

### 3.9.3 验证虚拟线程是否生效

```java
// 在任意 Controller 中加端点
@GetMapping("/thread-info")
public String threadInfo() {
    Thread thread = Thread.currentThread();
    return "Thread: " + thread.getName()
         + ", isVirtual: " + thread.isVirtual()
         + ", threadGroup: " + thread.getThreadGroup();
}
```

启动后访问 `/hotel/thread-info`，返回：
```
Thread: http-nio-8083-exec-1, isVirtual: true, threadGroup: java.lang.VirtualThread$VThreadGroup
```

## 3.10 缓存策略深度解析

### 3.10.1 Cache-Aside 模式流程图

```
请求查询酒店详情
       │
       ▼
  ┌────────────┐  Yes   ┌──────────┐
  │ Redis中有？ │──────→ │ 直接返回  │
  └─────┬──────┘        └──────────┘
        │ No
        ▼
  ┌────────────────┐
  │ 获取互斥锁      │
  │ (setnx lock:xxx)│
  └───────┬────────┘
          │
     ┌────┴────┐
     │ 成功获取 │
     └────┬────┘
          │
          ▼
  ┌──────────────────┐  Yes   ┌──────────┐
  │ 双重检查Redis？  │──────→ │ 直接返回  │
  └───────┬──────────┘        └──────────┘
          │ No
          ▼
  ┌──────────────┐
  │ 查询数据库    │
  └───────┬──────┘
          │
     ┌────┴────┐
     │ 查到数据 │──── No ──→ 写入空值缓存(60s)
     └────┬────┘              返回 null
          │ Yes
          ▼
  ┌──────────────────┐
  │ 写入Redis缓存     │
  │ TTL=30min+随机抖动 │
  └──────────────────┘
          │
          ▼
       返回结果
```

### 3.10.2 三种缓存异常的防护

| 问题 | 原因 | 本项目的防护措施 |
|------|------|-----------------|
| **缓存穿透** | 查询不存在的数据，每次都穿过缓存打到DB | 空值缓存（NULL标记，TTL 60s） |
| **缓存击穿** | 热点key过期，大量并发同时打到DB | 互斥锁（setnx），只有一个线程查DB |
| **缓存雪崩** | 大量key同时过期，突发流量打到DB | TTL随机抖动（基础TTL±5min范围内随机） |

### 3.10.3 缓存更新策略

**先写DB，再删缓存**（Cache-Aside 更新策略）：

```java
@Transactional
public boolean updateAndEvictCache(Hotel hotel) {
    // 1. 先写数据库
    boolean updated = updateById(hotel);
    if (updated) {
        // 2. 再删缓存
        cacheService.evictCache(CacheConstants.HOTEL_DETAIL_KEY + hotel.getId());
    }
    return updated;
}
```

为什么不是先删缓存再写DB？
- 并发场景：线程A删缓存 → 线程B查缓存miss → 线程B查DB读到旧数据 → 线程B把旧数据写入缓存 → 线程A写DB，导致**缓存脏数据**
- 先写DB再删缓存：即使有并发，下次查询会重新加载最新数据

## 3.11 验证步骤

### 3.11.1 第一次查询（慢，查DB）

```bash
# 启动 MySQL、Redis、Nacos
# 启动 ota-hotel 服务

# 查询酒店详情（第一次，应该走DB+写缓存）
curl -v http://localhost:8083/hotel/1
```

预期结果：
```json
{
  "id": 1,
  "name": "深圳湾万丽酒店",
  "cityId": 4403,
  "starRating": 5,
  "roomTypes": [
    {"id": 1, "name": "高级大床房", "maxOccupancy": 2},
    {"id": 2, "name": "豪华双床房", "maxOccupancy": 2},
    {"id": 3, "name": "行政套房", "maxOccupancy": 3}
  ]
}
```

控制台日志：
```
[DB] 查询酒店详情: hotelId=1
[Cache] 缓存未命中: key=hotel:detail:1，尝试获取互斥锁
[Cache] 写入缓存: key=hotel:detail:1, TTL=1850s
```

### 3.11.2 第二次查询（快，走缓存）

```bash
# 再次查询，应该走缓存，速度明显提升
time curl -s http://localhost:8083/hotel/1 | head -c 100
```

控制台日志：
```
[Cache] 命中缓存: key=hotel:detail:1
```

### 3.11.3 验证缓存穿透防护

```bash
# 查询不存在的酒店（应该写空值缓存，不穿透DB）
curl -v http://localhost:8083/hotel/99999

# 再次查询，不会查DB
curl -v http://localhost:8083/hotel/99999
```

### 3.11.4 验证列表分页

```bash
# 查询深圳的酒店
curl "http://localhost:8083/hotel/list?cityId=4403&page=1&size=10"

# 多条件搜索
curl -X POST http://localhost:8083/hotel/search \
  -H "Content-Type: application/json" \
  -d '{"keyword":"万丽","cityId":4403,"page":1,"size":10}'
```

### 3.11.5 用Redis客户端查看缓存

```bash
# 连接 Redis 查看缓存
redis-cli -a redis123456

127.0.0.1:6379> keys hotel:detail:*
1) "hotel:detail:1"
2) "hotel:detail:2"

127.0.0.1:6379> TTL hotel:detail:1
(integer) 1550

127.0.0.1:6379> TYPE hotel:detail:1
string
```

## 3.12 常见坑

### ⚠️ 坑1：Lettuce连接池与虚拟线程兼容

虚拟线程下，Lettuce 连接池默认行为可能导致连接泄露。确保配置：

```yaml
spring:
  data:
    redis:
      timeout: 3000ms    # 必须设置超时，否则虚拟线程可能永久阻塞
      lettuce:
        pool:
          max-active: 16  # 虚拟线程也需要限制连接数
```

### ⚠️ 坑2：互斥锁的死锁风险

如果查DB抛异常（如数据库连接断开），锁不会自动释放。在 `finally` 块中释放锁是**必须的**：

```java
try {
    // 查DB
} finally {
    // 必须释放锁
    redisTemplate.delete(lockKey);
}
```

同时给锁设置自动过期时间（`LOCK_TIMEOUT_MS = 2000ms`），双重保障。

### ⚠️ 坑3：JSON序列化 LocalDateTime

Spring Boot 默认的 Jackson 配置在序列化 `LocalDateTime` 时可能报错。需要注册 `JavaTimeModule`：

```java
ObjectMapper objectMapper = new ObjectMapper();
objectMapper.registerModule(new JavaTimeModule());
objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
```

### ⚠️ 坑4：MyBatis-Plus 分页不生效

如果分页一直返回全部数据，检查：
1. 是否配置了 `MybatisPlusInterceptor`
2. 分页参数必须使用 `Page` 对象传递
3. Mapper 方法参数顺序：`Page` 必须在第一位

### ⚠️ 坑5：Nacos 服务注册失败

确保 Nacos 3.1.1 服务端已启动。`application.yml` 中配置：
```yaml
spring.cloud.nacos.discovery:
  server-addr: localhost:8848
  namespace: public  # 默认命名空间
```

如果 Nacos 不在本地，修改 `server-addr` 指向实际地址。

### ⚠️ 坑6：虚拟线程+MySQL连接池

虚拟线程场景下，HikariCP 连接池大小不需要太大（20足够）。因为虚拟线程对 IO 等待的开销极低，即使少量连接也能高效处理大量请求。如果设置了 `connection-timeout` 太短，高并发时会出现获取连接超时。

---

## 3.13 本章总结

| 知识点 | 实现方式 |
|--------|---------|
| 酒店列表分页 | MyBatis-Plus Page + 自定义XML |
| 酒店详情（含房型） | Service 层组合查询 |
| Cache-Aside 模式 | 先查Redis → miss查DB → 写入Redis |
| 缓存穿透防护 | 查询结果为null时，缓存空值（60s TTL） |
| 缓存击穿防护 | Redis setnx 互斥锁，双重检查 |
| 缓存雪崩防护 | TTL = 基础值 + 随机抖动（0~5min） |
| 缓存更新 | 先写DB → 再删缓存 |
| 虚拟线程 | `spring.threads.virtual.enabled=true` |

下一章我们将实现 **Inventory 价库服务**，包含价库合一表设计、乐观锁库存操作、和库存状态机。
