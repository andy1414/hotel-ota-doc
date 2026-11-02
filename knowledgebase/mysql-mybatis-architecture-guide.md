# ota-cloud MySQL 8.0 与 MyBatis-Plus 架构与生产实践指南

> **定位**：系统性梳理 `ota-cloud` 中基于 MySQL 8.0 与 MyBatis-Plus 3.5.x 的**数据持久层工程架构规范**。深入剖析“价库合一”核心表设计、基于 CAS 乐观锁版本号（`version`）的库存防超卖机制、库存流水幂等设计、事务发件箱（`outbox_event`）以及严格的 XML 外部化 SQL 规范。

---

## 一、架构定位与设计模式：Optimistic Locking & Outbox Pattern

在分布式酒店预订系统中，高并发并发抢房引发的数据竞争是最核心的技术风险：
- **拒绝大事务行排他锁（`SELECT ... FOR UPDATE`）**：悲观锁会导致数据库线程排队阻塞、死锁率飙升、TPS 暴跌；
- **CAS 乐观锁防超卖**：采用“业务自检 + 乐观版本号递增”机制（`version = version + 1 WHERE id = #{id} AND version = #{version} AND available >= #{quantity}`），扣减影响行数为 0 即代表竞争冲突，通过轻量级应用重试解决；
- **库存流水与严格幂等**：所有库存变更必须伴随写入唯一约束的流水记录（`order_id` + `biz_date` + `operation_type`），防止重复扣减；
- **规范治理**：禁止在 Mapper 接口使用注解 SQL；禁止在 Service 层通过 `LambdaQueryWrapper` 组装复杂查询，所有业务 SQL 统一集中管理在 `src/main/resources/mapper/*Mapper.xml`。

```
              【预占库存写操作 (Freeze)】
                          │
                          ▼
        ┌───────────────────────────────────┐
        │ 校验并构造 CAS 条件                │
        │ available >= quantity             │
        │ version = currentVersion          │
        └─────────────────┬─────────────────┘
                          │ 执行原子 UPDATE
                          ▼
┌───────────────────────────────────────────────────┐
│        UPDATE inventory_price_stock               │
│        SET frozen = frozen + #{quantity},         │
│            available = available - #{quantity},   │
│            version = version + 1                  │
│        WHERE id = #{id} AND version = #{version}  │
│          AND available >= #{quantity}             │
└─────────────────────────┬─────────────────────────┘
                          │
            ┌─────────────┴─────────────┐
            ▼ (affected rows == 1)      ▼ (affected rows == 0)
    【预占成功，记录幂等流水】      【发生并发竞争/库存不足，快速失败/重试】
```

---

## 二、微服务拓扑与角色分工

1. **`ota-inventory`（价库与库存中心，端口 8084）**：
   - 核心表：`inventory_price_stock`（价库合一表，管理总房量、已冻结、已订、可用房量）；
   - `inventory_record`：库存操作流水表，实现防重操作。
2. **`ota-order`（订单中心，端口 8085）**：
   - 核心表：`orders`（主订单状态机表）、`order_item`（明细表）、`outbox_event`（事务发件箱表）。
3. **`ota-hotel`（酒店中心，端口 8083）**：
   - 核心表：`hotel`（酒店主表）、`room_type`（物理房型与售卖房型表）。

---

## 三、核心表结构设计矩阵

### 1. 价库合一表（`inventory_price_stock`）

| 字段名 | 类型 | 说明 | 约束与索引 |
| :--- | :--- | :--- | :--- |
| **`id`** | BIGINT | 主键自增 | PRIMARY KEY |
| **`hotel_id`** | BIGINT | 酒店主键 ID | 联合索引 `uk_hotel_room_date` |
| **`room_type_code`** | VARCHAR(32) | 房型编码 | 联合索引 `uk_hotel_room_date` |
| **`biz_date`** | DATE | 业务售卖日期 | 联合索引 `uk_hotel_room_date` |
| **`sell_price`** | DECIMAL(10,2)| 门市售卖价 | 非空 |
| **`cost_price`** | DECIMAL(10,2)| 底价成本价 | 非空 |
| **`total`** | INT | 物理总房量 | 非空 |
| **`frozen`** | INT | 冻结预占房量（下单未付） | 默认 0 |
| **`booked`** | INT | 确认已售房量（已支付出票） | 默认 0 |
| **`available`** | INT | 实时真实可用房量 | 约束: `total - frozen - booked` |
| **`version`** | INT | 乐观锁版本号 | CAS 更新自增 |

---

## 四、关键落地技术细节

### 1. 乐观锁原子变更（`InventoryPriceStockMapper.xml`）

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE mapper PUBLIC "-//mybatis.org//DTD Mapper 3.0//EN"
        "http://mybatis.org/dtd/mybatis-3-mapper.dtd">
<mapper namespace="com.ota.cloud.inventory.mapper.InventoryPriceStockMapper">

    <sql id="BaseColumns">
        id, hotel_id, room_type_code AS room_type, biz_date, sell_price, cost_price, currency,
        source_type, source_code, source_ref_id, total, frozen, booked, available,
        version, updated_at
    </sql>

    <!-- 1. 冻结库存：检查 available >= #{quantity} 与 version 严格匹配 -->
    <update id="freezeById">
        UPDATE inventory_price_stock
        SET frozen = frozen + #{quantity},
            available = available - #{quantity},
            version = version + 1
        WHERE id = #{id}
          AND version = #{version}
          AND available &gt;= #{quantity}
    </update>

    <!-- 2. 释放库存：取消订单时回退冻结房量 -->
    <update id="releaseById">
        UPDATE inventory_price_stock
        SET frozen = frozen - #{quantity},
            available = available + #{quantity},
            version = version + 1
        WHERE id = #{id}
          AND version = #{version}
          AND frozen &gt;= #{quantity}
    </update>

    <!-- 3. 实扣库存：支付成功时将 frozen 转化为 booked -->
    <update id="deductById">
        UPDATE inventory_price_stock
        SET booked = booked + #{quantity},
            frozen = frozen - #{quantity},
            version = version + 1
        WHERE id = #{id}
          AND version = #{version}
          AND frozen &gt;= #{quantity}
    </update>
</mapper>
```

### 2. Java Mapper 规范约定

严格遵循工程规范，Java Mapper 接口仅维护抽象方法定义，杜绝注解 SQL 与内联拼接：

```java
package com.ota.cloud.inventory.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.ota.cloud.inventory.entity.InventoryPriceStock;
import org.apache.ibatis.annotations.Param;
import java.time.LocalDate;
import java.util.List;

public interface InventoryPriceStockMapper extends BaseMapper<InventoryPriceStock> {

    List<InventoryPriceStock> selectByHotelAndDateRange(
            @Param("hotelId") Long hotelId,
            @Param("roomType") String roomType,
            @Param("startDate") LocalDate startDate,
            @Param("endDate") LocalDate endDate);

    int freezeById(
            @Param("id") Long id,
            @Param("quantity") Integer quantity,
            @Param("version") Integer version);

    int releaseById(
            @Param("id") Long id,
            @Param("quantity") Integer quantity,
            @Param("version") Integer version);

    int deductById(
            @Param("id") Long id,
            @Param("quantity") Integer quantity,
            @Param("version") Integer version);
}
```

---

## 五、监控指标与运维排查

### 1. Druid 连接池核心指标
- `druid.active.count`：当前活跃使用的数据库物理连接数；
- `druid.wait.thread.count`：等待连接的排队线程数（如大于 0 代表需要调大 `max-active`）；
- `druid.slow.sql`：慢 SQL 监控（阈值设定为 > 500ms 自动记录全字段）。

### 2. 索引与性能优化
- 关键联合索引：`CREATE UNIQUE INDEX uk_hotel_room_date ON inventory_price_stock(hotel_id, room_type_code, biz_date);` 杜绝重复初始化。

---

## 六、基础设施与运维部署

- **镜像规范**：`mysql:8.0`（启用 `utf8mb4_unicode_ci` 与连接池容量 `max_connections=1000`）。
- **Docker 运行参数**：
  ```yaml
  mysql:
    image: mysql:8.0
    container_name: ota-mysql
    restart: unless-stopped
    environment:
      MYSQL_ROOT_PASSWORD: root123456
      MYSQL_DATABASE: ota_cloud
      TZ: Asia/Shanghai
    ports:
      - "3306:3306"
    command:
      --default-authentication-plugin=mysql_native_password
      --character-set-server=utf8mb4
      --collation-server=utf8mb4_unicode_ci
      --max_connections=1000
      --innodb_buffer_pool_size=1G
  ```
