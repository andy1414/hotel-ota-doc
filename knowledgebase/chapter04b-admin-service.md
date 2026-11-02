# 第4章补充：Admin 中台配置后端一期

> 本节是第 4 章之后追加的业务设计变更。先做后端配置闭环，不做前端页面，也不让 `ota-inventory` 立即按规则变价。

## 4B.1 本节目标

新增 `ota-admin`（端口 8086）中台配置服务，用来管理：

- 下游渠道：谁可以从我们系统拿酒店和价格。
- 渠道可售范围：哪些集团、品牌、门店、价格码可以卖给哪个下游。
- 渠道加价系数：按旧系统 `ChanCoeTool` 思路累加有效系数。
- 酒店来源映射：第三方酒店可能和集团酒店重复，先生成候选并人工确认。
- 价库来源字段：`inventory_price_stock` 记录当前价格库存来自哪里，给后续比价系统预留入口。

一期边界：

- 不做前端。
- 不做 RBAC/菜单权限，复用 Gateway JWT 登录态即可。
- 不让库存查询接口按 `channelCode` 返回下游价。
- 不自动合并重复酒店，不自动改价，只保存候选和确认结果。
- 比价系统不进入一期，只通过来源字段和映射表预留。

## 4B.2 旧系统参考点

字段和业务规则优先参考已上线老系统：

| 新能力 | 老系统来源 | 迁移说明 |
| --- | --- | --- |
| 下游渠道 | `/Users/fuxinwei/work/db/api_hotel.txt` 中 `hoa_channel` | 迁移为 `downstream_channel`，保留 `channel_code/name/status/secret/share_rate/remark` |
| 可售范围和加价规则 | `/Users/fuxinwei/work/db/api_hotel.txt` 中 `hoa_channel_data_range` | 迁移为 `channel_data_range`，保留 `partner_code/brand_code/hotel_id/sale_code/rule_type/coe/status` |
| 重复规则校验 | `/Users/fuxinwei/work/java-background/hoa-background` 的 `ChannelDataRangeService` | 同一渠道、规则类型和规则维度不允许重复 |
| 系数累加 | `/Users/fuxinwei/work/otaapi` 中 `ChanCoeTool` | 渠道级、集团级、品牌级、门店级、价格码级、门店+价格码级累加 |
| 多来源价库追溯 | 本期新业务需求 | 给 `inventory_price_stock` 增加 `source_type/source_code/source_ref_id` |

## 4B.3 模块和路由

父 POM 增加模块：

```xml
<module>ota-admin</module>
```

服务配置：

```yaml
server:
  port: 8086

spring:
  application:
    name: ota-admin
```

Gateway 路由：

```yaml
- id: ota-admin
  uri: lb://ota-admin
  predicates:
    - Path=/api/admin/**
  filters:
    - StripPrefix=1
```

外部访问路径统一为：

```text
/api/admin/**
```

Gateway 已经对非白名单接口做 JWT 校验，所以中台一期只要登录即可访问。

## 4B.4 数据库设计

### 4B.4.1 downstream_channel

```sql
CREATE TABLE `downstream_channel` (
  `id`           BIGINT       NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `channel_code` VARCHAR(64)  NOT NULL COMMENT '下游渠道编码，来源：hoa_channel.channel_code',
  `name`         VARCHAR(128) NOT NULL COMMENT '渠道名称，来源：hoa_channel.name',
  `status`       TINYINT      NOT NULL DEFAULT 1 COMMENT '状态：1启用，0停用；口径延续 hoa_channel.status',
  `secret`       VARCHAR(128) NULL COMMENT '渠道密钥，来源：hoa_channel.secret；一期只保存配置',
  `share_rate`   DECIMAL(8,4) NOT NULL DEFAULT 0.0000 COMMENT '分享比例，来源：hoa_channel.share_rate',
  `remark`       VARCHAR(255) NOT NULL DEFAULT '' COMMENT '备注',
  `created_at`   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_downstream_channel_code` (`channel_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='中台下游渠道配置表';
```

### 4B.4.2 channel_data_range

```sql
CREATE TABLE `channel_data_range` (
  `id`           BIGINT       NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `channel_code` VARCHAR(64)  NOT NULL COMMENT '下游渠道编码，来源：hoa_channel_data_range.hoa_channel_code',
  `partner_code` VARCHAR(64)  NULL COMMENT '集团编码',
  `brand_code`   VARCHAR(64)  NULL COMMENT '品牌编码',
  `hotel_id`     VARCHAR(64)  NULL COMMENT '门店编号',
  `sale_code`    VARCHAR(64)  NULL COMMENT '价格码',
  `rule_type`    TINYINT      NOT NULL COMMENT '1集团、2品牌、3门店、4价格码、5渠道、6门店+价格码',
  `coe`          DECIMAL(8,4) NOT NULL DEFAULT 0.0000 COMMENT '加价系数；0.01 表示加价1%',
  `status`       TINYINT      NOT NULL DEFAULT 1 COMMENT '状态：1启用，0停用',
  `created_at`   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_channel_rule_scope` (`channel_code`, `rule_type`, `partner_code`, `brand_code`, `hotel_id`, `sale_code`),
  KEY `idx_channel_enabled_rule` (`channel_code`, `status`, `rule_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='中台渠道可售范围与加价规则表';
```

规则类型延续老系统 `hoa_channel_data_range.rule_type`：

| rule_type | 含义 | 匹配字段 |
| --- | --- | --- |
| 1 | 集团 | `channel_code + partner_code` |
| 2 | 品牌 | `channel_code + partner_code + brand_code` |
| 3 | 门店 | `channel_code + partner_code + brand_code + hotel_id` |
| 4 | 价格码 | `channel_code + sale_code` |
| 5 | 渠道 | `channel_code` |
| 6 | 门店+价格码 | `channel_code + partner_code + brand_code + hotel_id + sale_code` |

### 4B.4.3 hotel_source_mapping

```sql
CREATE TABLE `hotel_source_mapping` (
  `id`                 BIGINT       NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `source_type`        VARCHAR(32)  NOT NULL COMMENT '来源类型，例如 GROUP/OTA/THIRD_PARTY',
  `source_code`        VARCHAR(64)  NOT NULL COMMENT '来源编码，例如 huazhu/xc',
  `source_hotel_id`    VARCHAR(128) NOT NULL COMMENT '来源酒店ID',
  `source_hotel_name`  VARCHAR(255) NULL COMMENT '来源酒店名称',
  `source_address`     VARCHAR(255) NULL COMMENT '来源酒店地址',
  `source_city_code`   VARCHAR(32)  NULL COMMENT '来源城市编码',
  `matched_hotel_id`   BIGINT       NOT NULL COMMENT '匹配到的新系统酒店主键，对应 hotel.id',
  `matched_hotel_code` VARCHAR(64)  NULL COMMENT '匹配到的新系统酒店业务编号，对应 hotel.hotel_id',
  `matched_hotel_name` VARCHAR(255) NULL COMMENT '匹配到的新系统酒店名称',
  `confidence_score`   DECIMAL(6,4) NOT NULL DEFAULT 0.0000 COMMENT '候选置信度',
  `match_reason`       VARCHAR(512) NOT NULL DEFAULT '' COMMENT '候选生成理由',
  `status`             VARCHAR(16)  NOT NULL DEFAULT 'CANDIDATE' COMMENT 'CANDIDATE/CONFIRMED/REJECTED',
  `remark`             VARCHAR(255) NOT NULL DEFAULT '' COMMENT '备注',
  `created_at`         DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`         DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_source_matched_hotel` (`source_type`, `source_code`, `source_hotel_id`, `matched_hotel_id`),
  KEY `idx_source_status` (`source_type`, `source_code`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='酒店来源/重复候选映射表';
```

## 4B.5 价库来源字段

`inventory_price_stock` 增加：

```sql
ALTER TABLE inventory_price_stock
  ADD COLUMN `source_type`   VARCHAR(32)  NOT NULL DEFAULT 'GROUP' COMMENT '价库来源类型',
  ADD COLUMN `source_code`   VARCHAR(64)  NOT NULL DEFAULT 'huazhu' COMMENT '价库来源编码',
  ADD COLUMN `source_ref_id` VARCHAR(128) NOT NULL DEFAULT 'seed' COMMENT '来源原始记录ID';
```

初始化数据默认：

```text
source_type=GROUP
source_code=huazhu
source_ref_id=seed
```

查询 VO 同步返回这三个字段。这样后续如果接入携程、飞猪、高德等第三方价库，就能知道当前价格库存来自哪条来源记录。

## 4B.6 API 设计

### 下游渠道

```text
GET    /admin/channels
GET    /admin/channels/{id}
POST   /admin/channels
DELETE /admin/channels/{id}
```

### 渠道可售范围和加价系数

```text
GET    /admin/channel-data-ranges
GET    /admin/channel-data-ranges/{id}
POST   /admin/channel-data-ranges
DELETE /admin/channel-data-ranges/{id}
GET    /admin/channel-data-ranges/effective-coe
```

有效系数预览示例：

```bash
curl "http://localhost:8080/api/admin/channel-data-ranges/effective-coe?channelCode=fx&partnerCode=huazhu&brandCode=1&hotelId=HZ0001&saleCode=BAR" \
  -H "Authorization: Bearer $TOKEN"
```

累加顺序：

```text
渠道级(rule_type=5)
+ 集团级(rule_type=1)
+ 品牌级(rule_type=2)
+ 门店级(rule_type=3)
+ 价格码级(rule_type=4)
+ 门店+价格码级(rule_type=6)
= effectiveCoe
```

只统计 `status=1` 的规则。

### 酒店来源映射

```text
GET    /admin/hotel-source-mappings
GET    /admin/hotel-source-mappings/{id}
POST   /admin/hotel-source-mappings
DELETE /admin/hotel-source-mappings/{id}
POST   /admin/hotel-source-mappings/generate-candidates
```

候选生成示例：

```bash
curl -X POST http://localhost:8080/api/admin/hotel-source-mappings/generate-candidates \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "sourceType": "OTA",
    "sourceCode": "xc",
    "sourceHotelId": "xc-10001",
    "sourceHotelName": "深圳湾万丽酒店",
    "sourceAddress": "深圳市南山区科技南路",
    "cityCode": "440300",
    "latitude": 22.5331,
    "longitude": 113.9420
  }'
```

候选算法一期只使用静态信息：

- 同城市优先，不同城市不生成候选。
- 酒店名归一化后相似度。
- 地址归一化后相似度。
- 经纬度距离。
- 已存在 `CANDIDATE/CONFIRMED/REJECTED` 映射不重复生成。

## 4B.7 测试重点

`ota-admin`：

- 渠道规则新增、更新、删除。
- 重复规则唯一性校验。
- 有效系数累加结果。
- 仅启用 `status=1` 规则。
- 同名近距离酒店生成高分候选。
- 不同城市不生成候选。
- 已确认/已拒绝映射不重复生成。

`ota-inventory` 回归：

- 冻结、释放、扣减测试保持不变。
- 价库查询返回 `source_type/source_code/source_ref_id`。

验证命令：

```bash
/Users/fuxinwei/work/apache-maven-3.9.8/bin/mvn -pl ota-admin -am test
/Users/fuxinwei/work/apache-maven-3.9.8/bin/mvn -pl ota-inventory -am test
/Users/fuxinwei/work/apache-maven-3.9.8/bin/mvn test
```

## 4B.8 后续前端建议

中台页面后续再做，技术选型可以按产品气质选择：

| 方向 | 推荐 |
| --- | --- |
| 稳定企业中台 | React + Ant Design |
| 更现代可定制 | Next.js + shadcn/ui |
| Vue 生态 | Nuxt + Nuxt UI Dashboard |

当前优先级是后端规则、数据口径和测试闭环。

## 4B.9 下一步

进入第 5 章订单链路前，需要记住两个边界：

- 下单仍直接读取 `ota-inventory` 当前集团价，不按下游渠道加价。
- 后续如果订单要区分下游渠道，需要在订单创建 DTO 中追加 `channelCode`，并在价格确认阶段读取 `ota-admin` 的有效系数。
