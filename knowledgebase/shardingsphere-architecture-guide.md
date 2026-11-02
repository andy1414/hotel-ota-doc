# ota-cloud ShardingSphere 分库分表全景架构指南

> **定位**：系统性梳理 `ota-cloud` 中针对海量订单与日历房态数据基于 Apache ShardingSphere 5.x 的**水平分库分表（Horizontal Sharding）架构方案**。解析分片键选型、哈希取模算法、老系统 `hotel_id % 100` 兼容策略、跨分片分页查询归并以及透明数据访问层设计。

---

## 一、架构定位与设计模式：Data Sharding Pattern

在 OTA 平台的生命周期中，订单和日历库存属于数据增长最快的核心业务表：
- **单表性能拐点**：当单表行数超过 2000 万行或 B+ 树层级达到 4 层时，索引体积过大导致磁盘 I/O 激增，慢查询率飙升；
- **分片键（Sharding Key）核心选型**：
  - **订单中心**：采用 `user_id` 或基于分布式雪花算法（Snowflake）生成的 `order_id`（将分片信息内置在 ID 低位），保证用户高频查询落入单分片；
  - **价库与库存中心**：沿用老系统成熟的 `hotel_id % 100` 算法，将同一家酒店的所有日期房态集中存放在同张物理分表中，杜绝跨分片聚合；
- **透明代理与改写**：业务代码依然执行 `SELECT * FROM orders WHERE ...`，由 ShardingSphere 驱动内部的 SQL 解析引擎、路由引擎与结果归并引擎自动改写为针对物理分表的精准路由。

```
              【逻辑业务 SQL: SELECT ... FROM orders WHERE user_id = 10086】
                                          │
                                          ▼
                      ┌───────────────────────────────────────┐
                      │    ShardingSphere 核心分片引擎        │
                      │                                       │
                      │  1. SQL 解析 (提取 ShardingKey)       │
                      │  2. 路由计算 (10086 % 16 = 6)         │
                      │  3. SQL 改写 (orders -> order_6)      │
                      └───────────────────┬───────────────────┘
                                          │
                                          ▼
                      ┌───────────────────────────────────────┐
                      │    真实物理表: order_6                │
                      │    精准单分片索引扫描 (无需广播)       │
                      └───────────────────────────────────────┘
```

---

## 二、微服务拓扑与角色分工

1. **`ota-order`（订单中心，端口 8085）**：
   - 订单物理分表：`order_0` ~ `order_15`（16 张分表，支撑亿级订单沉淀）；
2. **`ota-inventory`（价库与库存中心，端口 8084）**：
   - 库存流水表：`inventory_record_0` ~ `inventory_record_15`（16 张分表）；
   - 房态价库表：`price_stock_00` ~ `price_stock_99`（100 张分表，严格按酒店主键隔离）。

---

## 三、核心分片规则矩阵

| 逻辑表名 (Logic Table) | 真实物理分表 (Actual Data Nodes) | 分片键 (Sharding Key) | 分片算法与策略 | 业务特性与考量 |
| :--- | :--- | :--- | :--- | :--- |
| **`orders`** | `order_0` ~ `order_15` | `user_id` / `order_id` | 哈希取模 (`% 16`) | 聚合单用户所有历史订单，避免跨表 Join |
| **`inventory_record`** | `inventory_record_0` ~ `15` | `order_id` | 哈希取模 (`% 16`) | 订单粒度幂等去重 |
| **`inventory_price_stock`** | `price_stock_00` ~ `99` | `hotel_id` | 格式化补零取模 (`LPAD(hotel_id % 100, 2, '0')`) | **继承老系统经典上线方案**，单酒店房态集中查询 |

---

## 四、关键落地技术细节

### 1. 物理分表初始化存储过程（`init-sql/001-init.sql`）

```sql
DELIMITER $$
CREATE PROCEDURE `create_chapter07_sharding_demo_tables`()
BEGIN
    DECLARE i INT DEFAULT 0;
    DECLARE suffix_name VARCHAR(8);

    -- 1. 创建 16 张订单与库存流水演示分表
    WHILE i < 16 DO
        SET @sql = CONCAT('CREATE TABLE IF NOT EXISTS `order_', i, '` LIKE `orders`');
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        SET @sql = CONCAT('CREATE TABLE IF NOT EXISTS `inventory_record_', i, '` LIKE `inventory_record`');
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        SET i = i + 1;
    END WHILE;

    -- 2. 创建 100 张房价库存物理分表 (price_stock_00 ~ price_stock_99)
    SET i = 0;
    WHILE i < 100 DO
        SET suffix_name = LPAD(i, 2, '0');
        SET @sql = CONCAT('CREATE TABLE IF NOT EXISTS `price_stock_', suffix_name, '` LIKE `inventory_price_stock`');
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        SET i = i + 1;
    END WHILE;
END$$
DELIMITER ;

CALL `create_chapter07_sharding_demo_tables`();
DROP PROCEDURE IF EXISTS `create_chapter07_sharding_demo_tables`;
```

### 2. ShardingSphere 规则声明模板（YAML 配置）

```yaml
spring:
  shardingsphere:
    rules:
      sharding:
        tables:
          orders:
            actual-data-nodes: ds_0.order_${0..15}
            table-strategy:
              standard:
                sharding-column: user_id
                sharding-algorithm-name: order-inline
          inventory_price_stock:
            actual-data-nodes: ds_0.price_stock_${0..99}
            table-strategy:
              standard:
                sharding-column: hotel_id
                sharding-algorithm-name: hotel-price-inline
        sharding-algorithms:
          order-inline:
            type: INLINE
            props:
              algorithm-expression: order_${user_id % 16}
          hotel-price-inline:
            type: INLINE
            props:
              algorithm-expression: price_stock_${hotel_id % 100}
```

---

## 五、跨分片分页与非分片键查询方案

1. **非分片键查询（如管理员按商户后台查询）**：
   - 方案一：采用异步双写构建 Elasticsearch 订单全文检索索引，中台全量检索走 ES，不打崩底层 MySQL 分片；
   - 方案二：建立“买家订单表（按 `user_id` 分片）”与“卖家订单表（按 `merchant_id` 分片）”的异构索引表。
2. **全局唯一主键设计**：
   - 禁止依赖 MySQL 自增 ID，统一采用 64 位雪花算法（Snowflake ID），保证全部分片趋势递增且全局不重复。
