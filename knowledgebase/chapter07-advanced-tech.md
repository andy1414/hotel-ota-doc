# 第7章：进阶技术大杂烩

> 本章将之前章节的核心链路"注入"进阶技术，包括链路追踪、限流降级、分布式事务、分库分表、AI 推荐、监控告警、日志分析。
>
> 每个技术点**独立可读**，按需选学，互不依赖（仅 Zipkin + Sentinel 可同时生效）。

---

## 7.1 Zipkin 链路追踪（P1-2）

> **目标**：通过 Micrometer Tracing + Zipkin，在分布式调用中透传 TraceId，实现全链路可视化。

### 7.1.1 背景

在微服务架构中，一个请求可能经过 Gateway → Auth → Order → Inventory 多个服务。当出现慢请求或错误时，需要知道：

- 哪一步耗时最长？
- 哪个服务返回了错误？
- 请求的完整调用链路是怎样的？

Zipkin 配合 Micrometer Tracing 可以完美解决这个问题。

### 7.1.2 添加依赖

在所有服务（gateway、auth、user、hotel、order、inventory）的 `pom.xml` 中添加：

```xml
<!-- Micrometer Tracing + Zipkin -->
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-tracing-bridge-brave</artifactId>
</dependency>
<dependency>
    <groupId>io.zipkin.reporter2</groupId>
    <artifactId>zipkin-reporter-brave</artifactId>
</dependency>
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-actuator</artifactId>
</dependency>
```

Gateway 还需要额外添加：

```xml
<!-- Gateway 的 TraceId 透传 -->
<dependency>
    <groupId>org.springframework.cloud</groupId>
    <artifactId>spring-cloud-starter-gateway</artifactId>
</dependency>
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-tracing-bridge-brave</artifactId>
</dependency>
```

### 7.1.3 配置（每个服务）

在每个服务的 `application.yml` 中添加：

```yaml
# Micrometer Tracing 配置
management:
  tracing:
    sampling:
      probability: 1.0       # 采样率 100%（开发环境全部采样，生产可设为 0.1）
    propagation:
      type: w3c              # 使用 W3C trace context 标准（兼容性好）

  zipkin:
    tracing:
      endpoint: http://localhost:9411/api/v2/spans

  endpoints:
    web:
      exposure:
        include: health,info,prometheus

logging:
  pattern:
    level: "%5p [%X{traceId:-},%X{spanId:-}]"   # 日志输出 TraceId
```

### 7.1.4 Gateway 的 TraceId 透传

Spring Cloud Gateway 默认会透传 TraceId，但需要确保过滤器顺序正确：

```yaml
spring.cloud.gateway:
  default-filters:
    - DedupeResponseHeader=Access-Control-Allow-Origin

# Micrometer 在 Gateway 中默认启用，无需额外代码
```

### 7.1.5 Feign 的 TraceId 透传

使用 Micrometer Tracing 后，OpenFeign 客户端会自动携带 tracing context，**无需额外代码**。项目可以通过 `micrometer-tracing-bridge-brave` 桥接到 Zipkin Reporter，但学习口径以 Micrometer Tracing 为主，不把 Brave API 作为业务代码入口。

### 7.1.6 RestTemplate 的 TraceId 透传

```java
@Bean
public RestTemplate restTemplate(RestTemplateBuilder builder) {
    return builder.build();
}
// RestTemplateBuilder 自动集成 Micrometer Observation/Tracing，开箱即用
```

### 7.1.7 Docker Compose 配置

```yaml
# 在 docker-compose.yml 中增加
  zipkin:
    image: openzipkin/zipkin:3
    container_name: ota-zipkin
    ports:
      - "9411:9411"
    environment:
      - STORAGE_TYPE=mem      # 内存存储（开发用），生产推荐 Elasticsearch
      - JAVA_OPTS=-Dzipkin.ui.enabled=true
```

### 7.1.8 验证步骤

```bash
# 1. 启动 Zipkin
cd chapter-01 && docker compose up -d zipkin

# 2. 确认 Zipkin 启动
curl http://localhost:9411/health

# 3. 发起一次完整的调用：登录 → 查询酒店 → 创建订单 → 支付
# 建议使用 chapter-02 的 test-auth-flow.sh 获取 token，然后依次调用

# 4. 打开 Zipkin UI
# 浏览器访问 http://localhost:9411
# 点击 "Run Query" 查看最近的调用链
# 可以看到 Gateway → Auth 校验 → Order → Inventory 的完整链路（含每个调用的耗时）
```

**Zipkin UI 查看要点**：

1. **搜索**：按服务名 `ota-order`、`ota-inventory` 或标签过滤
2. **详情**：点击一个 Trace 查看 Span 列表，每个 Span 展示服务名、耗时、标签
3. **依赖图**：切换到 "Dependencies" 标签，可以看到服务间的调用关系图
4. **慢调用**：按耗时降序排列，快速找到性能瓶颈

### 7.1.9 日志集成 TraceId

配置 `logback-spring.xml` 或直接在 `application.yml` 中设置 pattern：

```yaml
logging.pattern.level: "%5p [%X{traceId:-},%X{spanId:-}]"
```

日志输出示例：

```
2026-05-24T10:30:00.123+08:00  INFO [traceId=abc123,spanId=def456] com.ota.order.OrderService - 创建订单成功
```

### 7.1.10 常见坑

**🕳️ 采样率设置**：生产环境采样率设为 0.01~0.1（1%~10%），不需要全量采集。开发环境务必设为 1.0，否则看不到链路。推荐通过 Nacos 配置中心动态控制采样率，排查问题时临时调高，排查完恢复。

```yaml
# 生产推荐：通过 Nacos 动态配置
management:
  tracing:
    sampling:
      probability: ${ZIPKIN_SAMPLE_RATE:0.01}
```

**🕳️ Zipkin 性能开销**：Micrometer Tracing 埋点开销很小，每个 Span 约几微秒的开销。即使采样率 1.0，对业务性能的影响也几乎可以忽略。生产环境设低采样率主要是控制 Zipkin 自身的存储和展示压力。

**🕳️ Zipkin 内存溢出**：内存模式存储有限，生产环境务必使用 Elasticsearch 存储。

**🕳️ Gateway 链路断开**：如果 Gateway 使用 `webflux`，需要确保依赖正确，不要在 Gateway 中添加 `spring-boot-starter-web`（与 WebFlux 冲突）。

---

## 7.2 Sentinel 限流（P1-2）

> **目标**：通过 Sentinel 1.8.10 保护服务不被突发流量冲垮，支持热点参数限流、熔断降级。

### 7.2.1 背景

OTA 场景中，热门酒店（如节假日）可能被高频查询，需要：

- 对指定 `hotelId` 的查询限流（热点限流）
- 对整个接口做 QPS 限流
- 对慢调用做熔断降级

### 7.2.2 添加依赖

在所有需要限流的服务（hotel、order、inventory）中添加：

```xml
<dependency>
    <groupId>com.alibaba.cloud</groupId>
    <artifactId>spring-cloud-starter-alibaba-sentinel</artifactId>
</dependency>

<!-- Sentinel 配合 Gateway（仅 gateway 需要） -->
<dependency>
    <groupId>com.alibaba.cloud</groupId>
    <artifactId>spring-cloud-alibaba-sentinel-gateway</artifactId>
</dependency>

<!-- Sentinel 控制台 -->
<dependency>
    <groupId>com.alibaba.csp</groupId>
    <artifactId>sentinel-transport-simple-http</artifactId>
</dependency>
```

### 7.2.3 配置

```yaml
spring:
  cloud:
    sentinel:
      transport:
        dashboard: localhost:8088   # Sentinel 控制台地址
        port: 8719                  # 与控制台通信的端口
      eager: true                   # 启动时立即注册到控制台
      filter:
        enabled: true
      datasource:
        # 支持 Nacos 动态规则配置（生产推荐）
        ds1:
          nacos:
            server-addr: localhost:8848
            namespace: public
            dataId: ${spring.application.name}-sentinel-rules
            groupId: SENTINEL_GROUP
            rule-type: flow
```

### 7.2.4 热点参数限流（酒店查询）

```java
package com.ota.hotel.controller;

import com.alibaba.csp.sentinel.annotation.SentinelResource;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import com.ota.common.result.Result;
import com.ota.hotel.service.HotelService;
import com.ota.hotel.vo.HotelDetailVO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/hotel")
@RequiredArgsConstructor
public class HotelController {

    private final HotelService hotelService;

    /**
     * 酒店详情（热点限流：对 hotelId 做限流）
     *
     * 规则在 Sentinel 控制台配置：
     * - 资源名: hotelDetail
     * - 参数索引: 0（hotelId）
     * - 单机阈值: 5
     * - 统计窗口时长: 1s
     */
    @GetMapping("/detail/{hotelId}")
    @SentinelResource(
            value = "hotelDetail",
            blockHandler = "hotelDetailBlockHandler",
            fallback = "hotelDetailFallback"
    )
    public Result<HotelDetailVO> getHotelDetail(@PathVariable Long hotelId) {
        HotelDetailVO detail = hotelService.getHotelDetail(hotelId);
        return Result.success(detail);
    }

    /**
     * 限流降级处理（BlockException 异常时触发）
     */
    public Result<HotelDetailVO> hotelDetailBlockHandler(
            Long hotelId, BlockException ex) {
        log.warn("酒店详情接口被限流，hotelId={}", hotelId);
        return Result.error(429, "该酒店查询过于频繁，请稍后再试");
    }

    /**
     * 服务降级处理（业务异常时触发）
     */
    public Result<HotelDetailVO> hotelDetailFallback(
            Long hotelId, Throwable ex) {
        log.error("酒店详情接口异常，hotelId={}", hotelId, ex);
        return Result.error(500, "服务异常，请稍后重试");
    }
}
```

### 7.2.5 Sentinel 控制台启动

```yaml
# docker-compose.yml 中添加
  sentinel:
    image: bladex/sentinel-dashboard:1.8.10
    container_name: ota-sentinel
    ports:
      - "8088:8858"
    environment:
      - JAVA_OPTS=-Dserver.port=8858
```

访问 [http://localhost:8088](http://localhost:8088)，默认账号密码 `sentinel / sentinel`。

### 7.2.6 流控规则配置（控制台）

**在 Sentinel 控制台中配置以下规则**：

**热点规则（hotelDetail 资源）**：

| 参数 | 值 |
|------|-----|
| 资源名 | hotelDetail |
| 参数索引 | 0（hotelId） |
| 单机阈值 | 5 |
| 统计窗口时长 | 1s |

**QPS 流控规则（order/create 接口）**：

| 参数 | 值 |
|------|-----|
| 资源名 | POST:/order/create |
| 阈值类型 | QPS |
| 单机阈值 | 100 |
| 是否集群 | 否 |
| 流控模式 | 直接 |

### 7.2.7 验证步骤

```bash
# 1. 启动 Sentinel 控制台
cd chapter-01 && docker compose up -d sentinel

# 2. 启动 hotel 服务（或其他被保护服务）

# 3. 快速多次查询同一个 hotelId
for i in $(seq 1 20); do
  curl -s "http://localhost:8083/hotel/detail/1" | head -c 100
  echo ""
done

# 4. 观察结果——前几次返回正常数据，之后返回限流提示
#    {"code":429,"message":"该酒店查询过于频繁，请稍后再试"}
```

### 7.2.8 Sentinel vs Hystrix 对比

| 维度 | Sentinel | Hystrix（已停维） |
|------|----------|-----------------|
| **维护状态** | 活跃维护，持续更新 | 2018 年进入维护模式，已停止更新 |
| **流控模式** | QPS、线程数、慢调用比例、异常比例、系统负载 | 线程池隔离、信号量隔离 |
| **降级策略** | 慢调用比例、异常比例、异常数 | 熔断器（短路器模式） |
| **规则持久化** | Nacos/APOLLO/ZK/File 多种数据源 | 无持久化支持，代码配置 |
| **实时监控** | 内置面板，实时流量/拒绝统计 | 依赖 Metrics + Hystrix Dashboard |
| **Spring Cloud 集成** | Gateway/Feign/RestTemplate 原生支持 | 需额外适配 |
| **动态规则** | 支持运行时热更新 | 重启生效 |
| **高级特性** | 热点参数限流、系统自适应限流、流量效果（Warm Up/排队等待） | 无 |

### 7.2.9 降级后恢复方式

Sentinel 降级后支持两种恢复模式：

**1. 半开恢复（Half-Open）**：熔断后定时允许少量请求通过探测。如果探测请求成功，自动关闭熔断器恢复；如果仍失败，维持熔断。推荐在核心链路中使用，恢复更精准。
**2. 时间窗口恢复**：熔断指定时长（如 30-60 秒）后自动恢复，无论下游是否正常。适用于非关键路径，实现简单。

> 在我们的项目中，order 创建接口使用**时间窗口模式**（30 秒熔断时长），因为熔断期间用户请求直接返回降级提示比等待探测更合适。热点参数限流（如酒店详情）则不需要熔断，直接拒绝超限请求。

### 7.2.10 常见坑

**🕳️ Sentinel 控制台无法连接**：检查 `spring.cloud.sentinel.transport.port=8719` 是否被占用，Sentinel 会尝试从 8719 开始递增寻找可用端口。

**🕳️ 热点规则不生效**：确保 `@SentinelResource` 的 `value` 与规则中的资源名一致，且方法必须是 `public` 的。

**🕳️ Sentinel + Gateway 冲突**：gateway 项目中不要添加 `spring-boot-starter-web` 依赖，只使用 `spring-cloud-starter-gateway` 和 `spring-cloud-alibaba-sentinel-gateway`。

---

## 7.3 Seata AT 分布式事务（P1-3）

> **目标**：在 order 创建（冻结库存）和支付（扣减库存）场景中，通过 Seata AT 模式演示分布式事务能力。
>
> ⚠️ **重要说明**：Seata **仅用于 P1 技术演示**，不侵入核心链路。P0 核心链路（订单 + 库存）使用**本地事务 + 幂等接口 + 定时补偿**方案。Seata AT 有额外的部署复杂度和性能开销，不是所有场景都需要强一致性。

### 7.3.1 AT 模式原理

Seata AT（Auto Transaction）模式的工作原理：

```
TM（事务管理器）     TC（事务协调器）     RM（资源管理器）
    │                    │                    │
    │ 1. 开启全局事务     │                    │
    │───────────────────▶│                    │
    │                    │                    │
    │ 2. 执行业务SQL      │                    │
    │─────────────────────────────▶ RM(order) │
    │                    │  3. 注册分支事务    │
    │                    │◀───────────────────│
    │                    │                    │
    │ 4. 调用 inventory   │                    │
    │─────────────────────────────▶ RM(inv)   │
    │                    │  5. 注册分支事务    │
    │                    │◀───────────────────│
    │                    │                    │
    │ 6. 全局提交/回滚    │                    │
    │───────────────────▶│                    │
    │                    │ 7. 通知所有RM提交   │
    │                    │──────────────────▶│
    │                    │  或回滚（UNDO LOG） │
```

**关键点**：

- AT 模式对业务代码**几乎无侵入**——只需要 `@GlobalTransactional` 注解
- 在事务期间，Seata 会记录"前后镜像"，提交时删除，回滚时用前镜像恢复
- 需要每张业务表都有对应的 `undo_log` 表

### 7.3.1b AT 模式 vs TCC 模式

| 维度 | AT 模式 | TCC 模式 |
|------|---------|----------|
| **侵入性** | 几乎无侵入，@GlobalTransactional 注解 | 高侵入，需手动实现 Try/Confirm/Cancel 三接口 |
| **回滚方式** | 自动生成补偿 SQL（基于前后镜像） | 手动编写 Cancel 逻辑 |
| **适用场景** | 简单 CRUD 操作，单服务内多表事务 | 复杂资源管理，如库存精确回滚、第三方接口 |
| **性能开销** | 额外 SQL（查询前后镜像）+ undo_log 写入 | 无额外 SQL，但需业务代码保障 |
| **学习成本** | 低 | 高 |
| **一致性保证** | 最终一致性（二阶段异步提交） | 最终一致性 |

> 我们的订单+库存场景理论上更适合 TCC（库存扣减需要精确回滚），但 TCC 需要大量额外代码。因此 P1 只演示 AT 模式，TCC 留作知识储备。P0 核心链路不引入任何 Seata。

### 7.3.2 Seata Server (TC) 部署

```yaml
# docker-compose.yml 中添加
  seata:
    image: seataio/seata-server:2.6.0
    container_name: ota-seata
    ports:
      - "7091:7091"    # 控制台
      - "8091:8091"    # 服务端口
    environment:
      - SEATA_PORT=8091
      - STORE_MODE=db
      - SEATA_CONFIG_DATA_TYPE=nacos
    volumes:
      - ./seata/application.yml:/seata-server/resources/application.yml
    depends_on:
      - nacos
      - mysql
```

### 7.3.3 添加依赖

在需要参与分布式事务的服务（order、inventory）中添加：

```xml
<dependency>
    <groupId>com.alibaba.cloud</groupId>
    <artifactId>spring-cloud-starter-alibaba-seata</artifactId>
</dependency>
```

### 7.3.4 配置

```yaml
# 每个参与分布式事务的服务都需要
seata:
  enabled: true
  application-id: ${spring.application.name}
  tx-service-group: ota-tx-group
  service:
    vgroup-mapping:
      ota-tx-group: default
    grouplist:
      default: localhost:8091
  config:
    type: nacos
    nacos:
      server-addr: localhost:8848
      namespace: public
      group: SEATA_GROUP
  registry:
    type: nacos
    nacos:
      server-addr: localhost:8848
      namespace: public
      group: SEATA_GROUP
      application: seata-server
```

### 7.3.5 undo_log 表

每个参与 Seata 的数据库（ota_order、ota_inventory）都需要创建：

```sql
-- 在 ota_order 和 ota_inventory 库中分别执行
CREATE TABLE IF NOT EXISTS `undo_log` (
  `branch_id`     BIGINT       NOT NULL COMMENT '分支事务ID',
  `xid`           VARCHAR(128) NOT NULL COMMENT '全局事务ID',
  `context`       VARCHAR(128) NOT NULL COMMENT '上下文',
  `rollback_info` LONGBLOB     NOT NULL COMMENT '回滚信息',
  `log_status`    INT          NOT NULL COMMENT '状态：0正常 1已清理',
  `log_created`   DATETIME     NOT NULL COMMENT '创建时间',
  `log_modified`  DATETIME     NOT NULL COMMENT '修改时间',
  PRIMARY KEY (`branch_id`),
  KEY `idx_xid` (`xid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='AT事务模式UNDO日志表';
```

### 7.3.6 代码集成（P1 演示：在 OrderService 中使用 @GlobalTransactional）

```java
import io.seata.spring.annotation.GlobalTransactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements OrderService {

    /**
     * 创建订单（Seata 分布式事务版本）
     *
     * P1 演示用：使用 @GlobalTransactional 展示 Seata AT。
     * P0 默认不依赖 Seata，仍采用订单状态 + 库存幂等接口 + 补偿任务。
     */
    @Override
    @GlobalTransactional(name = "create-order-tx", rollbackFor = Exception.class)
    public OrderVO createOrder(OrderCreateDTO dto, Long userId) {
        // ... 创建订单 INIT 逻辑 ...

        // Feign 调用 inventory 冻结库存
        // Seata 演示版本中，Feign 会自动传递 XID
        Result<InventoryResultDTO> freezeResult = inventoryFeignClient.freeze(freezeDTO);

        if (freezeResult != null && freezeResult.isSuccess()) {
            // 更新订单状态为 WAIT_PAY
        } else {
            // 抛出异常，Seata 会自动回滚 order 的本地事务
            throw new RuntimeException("库存冻结失败，全局事务回滚");
        }
    }
}
```

### 7.3.7 Feign 传递 XID

Seata 会自动通过 Feign 的请求头传递 XID（`TX_XID`），**无需额外代码**。确保 Seata 的 `seata-spring-boot-starter` 依赖在 order 和 inventory 服务中都存在。

### 7.3.8 验证步骤

```bash
# 1. 启动 Seata Server
cd chapter-01 && docker compose up -d seata

# 2. 确认 Seata 控制台可用
curl http://localhost:7091

# 3. 确认 undo_log 表已创建
mysql -h127.0.0.1 -uroot -proot123456 ota_order -e "SHOW TABLES LIKE 'undo_log'"
mysql -h127.0.0.1 -uroot -proot123456 ota_inventory -e "SHOW TABLES LIKE 'undo_log'"

# 4. 启动 inventory 和 order 服务（Seata 版本）

# 5. 创建一个库存不足的订单（如数量远大于库存）
curl -X POST http://localhost:8080/api/order \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"hotelId":1,"roomType":"高级大床房","checkIn":"2026-06-01","checkOut":"2026-06-03","quantity":999}'

# 6. 验证事务回滚
# - order 表中没有新增的 INIT 状态订单（被回滚了）
# - undo_log 表中有一条记录（记录了回滚操作）
# - inventory 的库存没有变化

# 7. 查看 Seata 控制台（http://localhost:7091）上的全局事务记录
```

### 7.3.9 常见坑

**🕳️ Seata 全局事务不回滚**：检查 Feign 接口是否传了 `TX_XID`，可以打印请求头验证。如果 Feign 使用 `@RequestLine` 注解，需要手动配置 RequestInterceptor。

**🕳️ undo_log 表缺失**：每个参与的数据库都必须要创建 `undo_log` 表，否则 Seata 无法启用 AT 模式，启动时会报 `no available service` 错误。

**🕳️ 虚拟线程兼容问题**：Seata 依赖 ThreadLocal 做 XID 传递。JDK 21 虚拟线程下 ThreadLocal 性能下降（虚拟线程数多导致扩容成本高），需要使用 Seata 的虚拟线程兼容方案——替代为 ScopedValue 或复制 TransmittableThreadLocal。建议在 Seata 2.6.0+ 测试验证后使用。如果使用旧版本，需配置：

```yaml
seata.client.tm.enable-auto-data-source-proxy: false
```

**🕳️ 性能开销**：AT 模式会记录前后镜像，对每行数据都有额外的 SQL 查询。如果表数据量大或写频繁，考虑使用 TCC 模式。

---

## 7.4 ShardingSphere 分库分表（P2-2）

> **目标**：将订单表按 order_id 取模分表，库存表按 hotel_id 取模 100 分表，为海量数据做准备。

### 7.4.1 分片策略

| 表 | 分片键 | 策略 | 分片数量 |
|-----|--------|------|---------|
| orders | order_id | 按 ID 取模 16 | order_0 ~ order_15 |
| inventory_price_stock | hotel_id % 100 | 按 hotel_id 取模 100 | price_stock_00 ~ price_stock_99 |

> 为什么分片数不同？订单表按 ID 分片保证均匀分布；库存表按酒店分片保证同一个酒店的库存查询只命中一个分片。

### 7.4.2 添加依赖

```xml
<dependency>
    <groupId>org.apache.shardingsphere</groupId>
    <artifactId>shardingsphere-jdbc-core-spring-boot-starter</artifactId>
    <version>5.5.2</version>
</dependency>
```

### 7.4.3 Order 服务分表配置

```yaml
# ota-order 的 application.yml
spring:
  shardingsphere:
    datasource:
      names: ds0
      ds0:
        type: com.zaxxer.hikari.HikariDataSource
        driver-class-name: com.mysql.cj.jdbc.Driver
        jdbc-url: jdbc:mysql://localhost:3306/ota_order?useUnicode=true&characterEncoding=utf8mb4&useSSL=false&serverTimezone=Asia/Shanghai
        username: root
        password: root123456

    rules:
      sharding:
        tables:
          orders:
            actual-data-nodes: ds0.order_$->{0..15}
            table-strategy:
              standard:
                sharding-column: id
                sharding-algorithm-name: order-mod
            key-generate-strategy:
              column: id
              key-generator-name: snowflake   # 使用雪花算法有序ID

        sharding-algorithms:
          order-mod:
            type: MOD
            props:
              sharding-count: 16

        key-generators:
          snowflake:
            type: SNOWFLAKE

    props:
      sql-show: true    # 打印分片后的真实 SQL（开发用）
```

### 7.4.4 Inventory 服务分表配置

```yaml
# ota-inventory 的 application.yml
spring:
  shardingsphere:
    datasource:
      names: ds0
      ds0:
        type: com.zaxxer.hikari.HikariDataSource
        driver-class-name: com.mysql.cj.jdbc.Driver
        jdbc-url: jdbc:mysql://localhost:3306/ota_inventory?useUnicode=true&characterEncoding=utf8mb4&useSSL=false&serverTimezone=Asia/Shanghai
        username: root
        password: root123456

    rules:
      sharding:
        tables:
          inventory_price_stock:
            actual-data-nodes: ds0.price_stock_$->{0..99}
            table-strategy:
              standard:
                sharding-column: hotel_id
                sharding-algorithm-name: hotel-mod
          inventory_record:
            actual-data-nodes: ds0.inventory_record_$->{0..15}
            table-strategy:
              standard:
                sharding-column: order_id
                sharding-algorithm-name: record-mod

        sharding-algorithms:
          hotel-mod:
            type: MOD
            props:
              sharding-count: 100
          record-mod:
            type: MOD
            props:
              sharding-count: 16

    props:
      sql-show: true
```

### 7.4.5 分表 DDL（示例）

需要为每个分表执行 DDL：

```sql
-- 创建 16 张订单分表
CREATE TABLE `order_0` LIKE `orders`;
CREATE TABLE `order_1` LIKE `orders`;
-- ... order_2 ~ order_15

-- 创建 100 张库存分表
CREATE TABLE `price_stock_00` LIKE `inventory_price_stock`;
CREATE TABLE `price_stock_01` LIKE `inventory_price_stock`;
-- ... price_stock_02 ~ price_stock_99

-- 建议使用脚本批量创建，不要手动执行 116 条 DDL
```

批量创建脚本（Python）：

```python
#!/usr/bin/env python3
import pymysql

conn = pymysql.connect(host='127.0.0.1', user='root', password='root123456')
cursor = conn.cursor()

# 订单分表
for i in range(16):
    cursor.execute(f"CREATE TABLE IF NOT EXISTS ota_order.order_{i} LIKE ota_order.orders")
    print(f"order_{i} created")

# 库存分表
for i in range(100):
    cursor.execute(f"CREATE TABLE IF NOT EXISTS ota_inventory.price_stock_{i:02d} LIKE ota_inventory.inventory_price_stock")
    print(f"price_stock_{i:02d} created")

cursor.close()
conn.close()
```

### 7.4.6 验证步骤

```bash
# 1. 启动含 ShardingSphere 配置的服务

# 2. 创建订单
curl -X POST http://localhost:8080/api/order \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"hotelId":1,"roomType":"高级大床房","checkIn":"2026-06-10","checkOut":"2026-06-12","quantity":1}'

# 3. 在日志中查看分片后的实际 SQL
# 例如：INSERT INTO order_7  (id, ...) VALUES (...)
# 可以看到 ShardingSphere 自动路由到了 order_7 表

# 4. 直接查询数据库验证
mysql -h127.0.0.1 -uroot -proot123456 -e \
  "SELECT 'order_0' as tbl, COUNT(*) as cnt FROM ota_order.order_0 \
   UNION ALL SELECT 'order_1', COUNT(*) FROM ota_order.order_1 \
   UNION ALL SELECT 'order_7', COUNT(*) FROM ota_order.order_7"

# 5. 分片后的查询（按分片键查询不走全分片）
mysql -h127.0.0.1 -uroot -proot123456 ota_inventory \
  -e "EXPLAIN SELECT * FROM price_stock_03 WHERE hotel_id = 3"
```

### 7.4.7 常见坑

**🕳️ 跨分片查询性能差**：如果不带分片键（如 `SELECT * FROM orders WHERE status = 'WAIT_PAY'`），ShardingSphere 会广播到所有分片，性能极差。一定要使用分片键。

**🕳️ 分片键变更**：一旦数据分片后，分片键的值不能修改（修改 hotel_id 会导致数据无法找到）。如果必须变更，需要重新分片。

**🕳️ 分布式主键冲突**：使用雪花算法时，不同服务实例的 `worker-id` 不能重复，否则可能生成相同 ID。配置方式：

```yaml
spring.shardingsphere.rules.sharding.key-generators.snowflake.props:
  worker-id: ${random.int(1024)}
```

**🕳️ ShardingSphere + MyBatis-Plus 兼容性**：ShardingSphere 5.x 和 MyBatis-Plus 3.5.x 配合良好，但注意 MyBatis-Plus 的分页插件需要 ShardingSphere 的 `PaginationInnerInterceptor` 支持。

---

## 7.5 Spring AI 推荐（P2-1）

> **目标**：通过 Spring AI 1.1.7 集成 DeepSeek API，提供智能推荐和酒店问答能力。未配 Key 时不阻塞主链路。

### 7.5.1 添加依赖

```xml
<dependency>
    <groupId>org.springframework.ai</groupId>
    <artifactId>spring-ai-openai-spring-boot-starter</artifactId>
    <version>1.1.7</version>
</dependency>
```

### 7.5.2 配置

```yaml
# ota-ai 的 application.yml
spring:
  ai:
    openai:
      api-key: ${AI_API_KEY:}          # 从环境变量读取，留空则不启用
      base-url: https://api.deepseek.com/v1
      chat:
        options:
          model: deepseek-chat
          temperature: 0.7
          max-tokens: 2048

# 是否启用 AI 功能（控制开关，不配 Key 时不阻塞）
ai:
  enabled: ${AI_ENABLED:false}
```

### 7.5.3 判断 AI 是否可用

```java
package com.ota.ai.config;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AiConfig {

    @Value("${spring.ai.openai.api-key:}")
    private String apiKey;

    @Value("${ai.enabled:false}")
    private boolean aiEnabled;

    @Getter
    private boolean available = false;

    @PostConstruct
    public void init() {
        available = aiEnabled && apiKey != null && !apiKey.isBlank();
        if (available) {
            log.info("🔥 AI 功能已启用");
        } else {
            log.warn("⚠️ AI 功能未启用（未配置 API Key 或 ai.enabled=false）");
        }
    }
}
```

### 7.5.4 AI Service

```java
package com.ota.ai.service;

import com.ota.ai.config.AiConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.ChatResponse;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.chat.prompt.SystemPromptTemplate;
import org.springframework.ai.openai.OpenAiChatClient;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class AiRecommendService {

    private final OpenAiChatClient chatClient;
    private final AiConfig aiConfig;

    /**
     * 酒店推荐
     */
    public String recommend(String city, String budget, String preferences) {
        if (!aiConfig.isAvailable()) {
            return "AI 推荐功能暂不可用，请联系管理员配置 API Key";
        }

        String systemPrompt = """
                你是一个专业的酒店推荐助手。请根据用户的需求推荐合适的酒店。
                城市：{city}
                预算：{budget}
                偏好：{preferences}
                
                请推荐 3-5 家酒店，包含以下信息：
                - 酒店名称
                - 推荐理由（50字以内）
                - 建议预订的房型
                - 预估价格
                """;

        SystemPromptTemplate promptTemplate = new SystemPromptTemplate(systemPrompt);
        Prompt prompt = promptTemplate.create(Map.of(
                "city", city,
                "budget", budget,
                "preferences", preferences
        ));

        try {
            ChatResponse response = chatClient.call(prompt);
            return response.getResult().getOutput().getContent();
        } catch (Exception e) {
            log.error("AI 推荐失败", e);
            return "AI 推荐暂时无法使用，请稍后再试";
        }
    }

    /**
     * 酒店问答助手
     */
    public String ask(String question) {
        if (!aiConfig.isAvailable()) {
            return "AI 助手暂不可用，请联系管理员配置 API Key";
        }

        try {
            return chatClient.call(question);
        } catch (Exception e) {
            log.error("AI 问答失败", e);
            return "AI 助手暂时无法回答，请稍后再试";
        }
    }
}
```

### 7.5.5 Controller

```java
package com.ota.ai.controller;

import com.ota.ai.service.AiRecommendService;
import com.ota.common.result.Result;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/ai")
@RequiredArgsConstructor
public class AiController {

    private final AiRecommendService aiService;

    /**
     * 酒店推荐
     */
    @GetMapping("/recommend")
    public Result<String> recommend(
            @RequestParam String city,
            @RequestParam(required = false, defaultValue = "500-1000") String budget,
            @RequestParam(required = false, defaultValue = "") String preferences) {
        String result = aiService.recommend(city, budget, preferences);
        return Result.success(result);
    }

    /**
     * 酒店预订问答助手
     */
    @PostMapping("/assistant")
    public Result<String> assistant(@RequestBody String question) {
        String result = aiService.ask(question);
        return Result.success(result);
    }
}
```

### 7.5.6 验证步骤

```bash
# 1. 配置 DeepSeek API Key
export AI_API_KEY=sk-xxxxxxxxxxxxxxxx
export AI_ENABLED=true

# 2. 启动 ota-ai 服务
cd chapter-07/ota-ai
mvn spring-boot:run

# 3. 测试推荐（外部访问走 Gateway，不直连 8087）
curl -X POST http://localhost:8080/api/ai/recommend \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"cityId":1,"preferences":["亲子","泳池","早餐"],"budget":"1000-2000"}'

# 4. 测试问答（同样通过 Gateway）
curl -X POST http://localhost:8080/api/ai/qa \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"hotelId":101,"question":"是否有接机服务？退房时间是几点？"}'

# 5. 不配 Key 时验证
# 注释掉 API Key，重启服务，调用接口应返回"AI 推荐功能暂不可用"
```

> ⚠️ 外部请求统一通过 Gateway `/api/ai/` 路径访问，禁止直连 ota-ai 的 8087 端口。Gateway 路由配置已在第2章中定义。

### 7.5.7 常见坑

**🕳️ Spring AI vs 直接调 OpenAI API**：Spring AI 是 Spring 生态原生集成——可以用 Bean 管理和配置方式。它提供了 ChatClient 声明式接口、OutputParser 结构化输出、Prompt Template、向量数据库抽象等。最大的好处是切换提供商只需要改配置文件，无需改业务代码。

**🕳️ 如何保证 LLM 回答的准确性**：1）RAG 模式——只基于提供的酒店数据回答，不依赖 LLM 内部知识；2）提示词中明确约束「如果信息中找不到，如实告知」；3）输出解析为结构化 JSON，业务侧做字段校验。关键信息（如价格、库存）仍然从业务 API 获取，不依赖 LLM 回答。

**🕳️ OpenAI API 兼容性问题**：DeepSeek 兼容 OpenAI API，但如果改用了其他模型（如千问、文心），需要确认 API 格式是否完全兼容。

**🕳️ 超时阻塞**：AI 调用可能耗时较长，建议配置超时：

```yaml
spring.ai.openai.chat.options:
  timeout: 30s
```

并在 Feign/Gateway 层面设置合理的调用超时。

**🕳️ Key 泄露**：API Key 不要硬编码在配置文件中，使用环境变量或 Nacos 加密配置。

---

## 7.6 Prometheus/Grafana 监控（P2-3）

> **目标**：通过 Micrometer 暴露指标，Prometheus 采集，Grafana 展示 JVM、接口延迟、DB 连接、缓存命中率等面板。

### 7.6.1 添加依赖

在每个服务中添加：

```xml
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>
```

### 7.6.2 配置暴露指标

```yaml
# 在每个服务的 application.yml 中
management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus
  metrics:
    tags:
      application: ${spring.application.name}   # 为所有指标增加 app 标签
    export:
      prometheus:
        enabled: true
```

### 7.6.3 Docker Compose 配置

```yaml
# docker-compose.yml 中添加
  prometheus:
    image: prom/prometheus:v3.2.1
    container_name: ota-prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus-data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--web.console.libraries=/etc/prometheus/console_libraries'
      - '--web.console.templates=/etc/prometheus/consoles'
      - '--web.enable-lifecycle'

  grafana:
    image: grafana/grafana:11.6.0
    container_name: ota-grafana
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_USER=admin
      - GF_SECURITY_ADMIN_PASSWORD=admin123
      - GF_INSTALL_PLUGINS=
    volumes:
      - grafana-data:/var/lib/grafana
      - ./grafana/dashboards:/etc/grafana/provisioning/dashboards
      - ./grafana/datasources:/etc/grafana/provisioning/datasources
    depends_on:
      - prometheus

volumes:
  prometheus-data:
  grafana-data:
```

### 7.6.4 Prometheus 配置文件

`chapter-01/prometheus/prometheus.yml`：

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'ota-gateway'
    metrics_path: '/actuator/prometheus'
    static_configs:
      - targets: ['host.docker.internal:8080']
        labels:
          application: ota-gateway

  - job_name: 'ota-auth'
    metrics_path: '/actuator/prometheus'
    static_configs:
      - targets: ['host.docker.internal:8081']
        labels:
          application: ota-auth

  - job_name: 'ota-order'
    metrics_path: '/actuator/prometheus'
    static_configs:
      - targets: ['host.docker.internal:8084']
        labels:
          application: ota-order

  - job_name: 'ota-inventory'
    metrics_path: '/actuator/prometheus'
    static_configs:
      - targets: ['host.docker.internal:8085']
        labels:
          application: ota-inventory

  - job_name: 'ota-hotel'
    metrics_path: '/actuator/prometheus'
    static_configs:
      - targets: ['host.docker.internal:8083']
        labels:
          application: ota-hotel

  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']
```

### 7.6.5 Grafana 数据源配置

`chapter-01/grafana/datasources/datasource.yml`：

```yaml
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: false
```

### 7.6.6 关键监控指标

| 指标 | 含义 | PromQL 示例 |
|------|------|-------------|
| `jvm_memory_used_bytes` | JVM 内存使用 | `jvm_memory_used_bytes{area="heap"}` |
| `jvm_gc_pause_seconds` | GC 暂停时间 | `rate(jvm_gc_pause_seconds_sum[1m])` |
| `http_server_requests_seconds` | 接口响应时间 | `rate(http_server_requests_seconds_sum[1m])` |
| `hikaricp_connections_active` | 活跃连接数 | `hikaricp_connections_active` |
| `cache_gets_total` | 缓存命中/未命中 | `rate(cache_gets_total{result="hit"}[1m])` |
| `logback_events_total` | 日志事件计数 | `rate(logback_events_total{level="error"}[5m])` |

### 7.6.7 常用 Grafana Dashboard

在 Grafana 中导入以下 Dashboard：

1. **JVM (Micrometer)** — Dashboard ID: `4701`
2. **Spring Boot 2.1 System** — Dashboard ID: `12900`
3. **Golang / Java 应用监控** — 自行创建，包含以下面板：

**推荐面板布局**：

| 行 | 面板 | 指标 |
|----|------|------|
| 1 | JVM 堆内存 | `jvm_memory_used_bytes{area="heap"}` |
| 1 | JVM 非堆内存 | `jvm_memory_used_bytes{area="nonheap"}` |
| 1 | GC 次数 | `rate(jvm_gc_pause_seconds_count[1m])` |
| 2 | 接口 QPS | `rate(http_server_requests_seconds_count[1m])` |
| 2 | P99 延迟 | `histogram_quantile(0.99, ...)` |
| 2 | 错误率 | `sum(rate(http_server_requests_seconds_count{status=~"5.."}[1m]))` |
| 3 | DB 活跃连接 | `hikaricp_connections_active` |
| 3 | DB 连接等待 | `hikaricp_connections_pending` |
| 3 | 线程数 | `jvm_threads_live_threads` |

### 7.6.8 验证步骤

```bash
# 1. 启动 Prometheus + Grafana
cd chapter-01 && docker compose up -d prometheus grafana

# 2. 确认 Prometheus 已抓取到指标
curl http://localhost:9090/api/v1/targets | python3 -m json.tool | grep "health"
# 应显示所有服务为 UP

# 3. 触发一些业务请求产生指标
curl http://localhost:8084/order/list?page=1&size=10 -H "X-User-Id: 1"

# 4. 在 Prometheus 中查询指标
curl "http://localhost:9090/api/v1/query?query=http_server_requests_seconds_count"

# 5. 访问 Grafana
# 浏览器访问 http://localhost:3000
# 账号：admin / admin123
# 进入 Explore → 选择 Prometheus 数据源 → 输入查询: jvm_memory_used_bytes
# 创建 Dashboard 并添加面板

# 6. 在 Prometheus 内置 UI 中查看
# http://localhost:9090 → Graph → 输入: rate(http_server_requests_seconds_count[1m])
```

### 7.6.9 常见坑

**🕳️ Prometheus 无法抓取 Windows/WSL 指标**：在 WSL2 中，服务监听在 localhost，但 Prometheus 通过 `host.docker.internal` 访问宿主机。如果 `host.docker.internal` 不可用，使用 `host-gateway`：

```yaml
extra_hosts:
  - "host.docker.internal:host-gateway"
```

**🕳️ 指标过多导致存储膨胀**：默认每 15s 收集所有指标，一天下来数据量不小。配置保留策略：

```yaml
prometheus:
  command:
    - '--storage.tsdb.retention.time=15d'
    - '--storage.tsdb.retention.size=10GB'
```

**🕳️ Grafana 面板不显示数据**：检查 Grafana 数据源的 Prometheus URL。在 Docker 网络中，Grafana 通过容器名访问 Prometheus（`http://prometheus:9090`），而不是 `localhost:9090`。

---

## 7.7 ELK 日志分析（P2-3）

> **目标**：通过 Elasticsearch + Logstash + Kibana 集中收集各微服务日志，按 `traceId`、`orderId`、异常堆栈快速定位问题。

### 7.7.1 背景

Zipkin 解决“调用链路和耗时”问题，Prometheus/Grafana 解决“指标和趋势”问题。ELK 补齐日志维度：当订单失败、库存冻结异常、Kafka 消费重试时，可以不用逐个服务翻日志，而是在 Kibana 中按关键字段检索完整上下文。

### 7.7.2 日志字段建议

所有服务建议输出结构化 JSON 日志，至少包含以下字段：

| 字段 | 来源 | 用途 |
|------|------|------|
| `timestamp` | Logback | 时间排序 |
| `service` | `${spring.application.name}` | 区分 Gateway/Auth/Order/Inventory |
| `level` | Logback | 按 ERROR/WARN 过滤 |
| `traceId` | MDC | 串起一次跨服务请求 |
| `spanId` | MDC | 定位单个调用片段 |
| `orderId` | 业务日志 | 定位订单生命周期 |
| `hotelId` | 业务日志 | 定位酒店/库存查询问题 |
| `message` | 业务日志 | 人可读说明 |

### 7.7.3 Docker Compose 配置

```yaml
# docker-compose.yml 中添加
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.15.0
    container_name: ota-elasticsearch
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
      - ES_JAVA_OPTS=-Xms512m -Xmx512m
    ports:
      - "9200:9200"
    volumes:
      - elasticsearch-data:/usr/share/elasticsearch/data

  logstash:
    image: docker.elastic.co/logstash/logstash:8.15.0
    container_name: ota-logstash
    ports:
      - "5044:5044"
    volumes:
      - ./logstash/pipeline:/usr/share/logstash/pipeline
    depends_on:
      - elasticsearch

  kibana:
    image: docker.elastic.co/kibana/kibana:8.15.0
    container_name: ota-kibana
    environment:
      - ELASTICSEARCH_HOSTS=http://elasticsearch:9200
    ports:
      - "5601:5601"
    depends_on:
      - elasticsearch

volumes:
  elasticsearch-data:
```

### 7.7.4 Logstash Pipeline

`chapter-01/logstash/pipeline/ota-log.conf`：

```conf
input {
  tcp {
    port => 5044
    codec => json_lines
  }
}

filter {
  mutate {
    add_field => { "project" => "ota-cloud" }
  }
}

output {
  elasticsearch {
    hosts => ["http://elasticsearch:9200"]
    index => "ota-cloud-%{+YYYY.MM.dd}"
  }
  stdout { codec => rubydebug }
}
```

### 7.7.5 验证步骤

```bash
# 1. 启动 ELK
cd chapter-01 && docker compose up -d elasticsearch logstash kibana

# 2. 确认 Elasticsearch 正常
curl http://localhost:9200

# 3. 访问 Kibana
# 浏览器访问 http://localhost:5601

# 4. 发起一次完整业务请求
# 登录 → 查酒店 → 查价库 → 创建订单 → 支付

# 5. 在 Kibana Discover 中按 traceId/orderId 检索
# 示例查询：traceId : "abc123" or orderId : "202605270001"
```

### 7.7.6 面试口径

> 「Zipkin 看调用链，Prometheus/Grafana 看指标，ELK 查日志细节。比如订单支付失败时，我先在 Grafana 看错误率是否突增，再用 Zipkin 找到失败链路，最后用 Kibana 按 traceId/orderId 查 Gateway、Order、Inventory、Kafka Consumer 的完整日志。」

---

## 7.8 本章总结

| 技术 | 用途 | 依赖组件 | 学习重点 |
|------|------|---------|---------|
| Zipkin | 链路追踪 | Zipkin 3.x Server | TraceId 透传、Span 理解 |
| Sentinel | 限流熔断 | Sentinel Dashboard 1.8.10 | 热点限流、@SentinelResource |
| Seata | 分布式事务 | Seata Server 2.6.0 | AT 模式、undo_log |
| ShardingSphere | 分库分表 | ShardingSphere-JDBC 5.x | 分片策略、分片键 |
| Spring AI | AI 推荐 | DeepSeek API / 其他 LLM | ChatClient、Prompt Template、RAG |
| Prometheus/Grafana | 监控告警 | Prometheus + Grafana | Micrometer 指标暴露 |
| ELK | 日志分析 | Elasticsearch + Logstash + Kibana | traceId/orderId 检索 |

**生产环境建议**：

1. Zipkin 使用 Elasticsearch 存储，而非内存
2. Sentinel 规则通过 Nacos 动态推送，而非控制台手动配置
3. Seata AT 模式仅用于 P1 技术演示，P0 核心链路使用本地事务 + 幂等接口 + 定时补偿
4. ShardingSphere 的分片键一旦选定，后续不能变更，前期要做好规划
5. AI 功能添加熔断和限流，防止大模型调用拖垮主服务，所有外部请求走 Gateway `/api/ai/`，禁止直连 8087
6. 监控使用 Grafana Alerting 配置告警规则，结合钉钉/企微通知
7. ELK 放在 P2，不阻塞 P0/P1；日志字段要和 TraceId、订单号等业务上下文对齐
