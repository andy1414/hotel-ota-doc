# ota-cloud 全链路学习指南

> 目标读者：19 年 Java 经验开发者，从全链路视角理解 ota-cloud 微服务项目
> 技术栈：JDK 21 (虚拟线程) + Spring Boot 3.5.14 + Spring Cloud 2025.0.x + Spring Cloud Alibaba 2025.0.x + ELK(P2)

---

## 目录

1. [一次请求的全链路追踪](#1-一次请求的全链路追踪)
2. [学习优先级与路线](#2-学习优先级与路线)
3. [各模块关键技术拆解](#3-各模块关键技术拆解)
4. [开发路线图（13天版本规划）](#4-开发路线图13天版本规划)
5. [面试演示路径](#5-面试演示路径)

---

## 1. 一次请求的全链路追踪

### 1.1 完整请求路径（文字流程图）

以下是一次「供应同步 → 用户搜索酒店 → 查看价库 → 集团下单 → 支付/取消」完整业务的请求链路。外部请求统一从 Gateway 进入，Gateway 做白名单、TraceId、限流和认证过滤；需要登录态的请求会使用 `ota-auth` 的认证中心能力校验 Token、黑名单和权限上下文，然后再转发到业务服务。

```
┌─────────────┐
│   浏览器     │  http://api.ota.com/api/hotel/search?cityId=...
└──────┬──────┘
       │ GET /api/hotel/search
       ▼
┌──────────────────────────────────────────────────┐
│              ota-gateway (8080)                   │
│                                                    │
│  ① GatewayHandlerMapping 路由匹配                  │
│  ② TraceIdFilter 生成/透传 TraceId                 │
│  ③ WhiteListFilter 检查白名单                       │
│     /api/auth/login、/api/auth/refresh 等放行       │
│  ④ 非白名单 → JwtAuthGlobalFilter                  │
│     解析 Authorization: Bearer <token>             │
│     必要时调用 ota-auth /auth/validate              │
│     校验签名、有效期、黑名单、权限上下文              │
│     通过 → 注入 X-User-Id / X-User-Name / TraceId  │
│  ⑤ Sentinel / RequestRateLimiter 限流               │
│  ⑥ 路由转发 → Nacos 负载均衡 → 业务服务              │
└──────────────────┬───────────────────────────────┘
                   │ X-User-Id: 1001, X-Trace-Id: abc-123
                   ▼
┌──────────────────────────────────────────────────┐
│              ota-supplier (8086)                  │
│                                                    │
│  ① 定时或手动触发供应同步                         │
│  ② mock/华住风格 adapter 拉集团 API                │
│     酒店列表/详情/房型/价库/库存/取消规则           │
│  ③ 转换为标准 HotelDTO / PriceStockDTO             │
│  ④ 写入 ota-hotel 和 ota-inventory 标准表           │
│     保存 supplierCode + supplierHotelId 映射       │
└──────────────────┬───────────────────────────────┘
                   │ 用户查询标准酒店和标准价库
                   ▼
┌──────────────────────────────────────────────────┐
│              ota-hotel (8083)                     │
│                                                    │
│  ① 接收 /api/hotel/search 请求                    │
│  ② Redis 查询缓存 hotel:detail:{id}                │
│     Cache-Aside：缓存命中直接返回                   │
│     缓存未命中 → 查 MySQL hotel/room_type 表        │
│     结果写入 Redis，设置 TTL（600s + 随机抖动 60s） │
│  ③ 空值缓存：DB 无数据时缓存空值 30s 防穿透         │
│  ④ 返回 HotelDetailDTO（含酒店信息 + 房型列表）     │
└──────────────────┬───────────────────────────────┘
                   │ 浏览器拿到酒店列表后，继续请求价库
                   ▼
┌──────────────────────────────────────────────────┐
│            ota-inventory (8085)                   │
│                                                    │
│  ① 查询价库：POST /api/inventory/price/query       │
│     {hotelId:101, roomType:"DBL",                  │
│      startDate:"2026-06-01", endDate:"2026-06-03"} │
│  ② Redis 缓存 ps:{supplierCode}:{supplierHotelId}: │
│     {roomType}:{date}                              │
│     批量获取 → 拼装返回值                          │
│  ③ 缓存未命中 → MySQL 查询 inventory_price_stock 表 │
│     （价库快照：supplierCode, supplierHotelId,      │
│       sell_price, total, held, booked, available） │
│  ④ available 作为本地快照，最终以集团试单为准       │
│  ⑤ 返回 PriceStockDTO 列表                        │
└──────────────────┬───────────────────────────────┘
                   │ 用户选择日期、房型 → 点击下单
                   ▼
┌──────────────────────────────────────────────────┐
│            ota-gateway (8080) 再次拦截             │
│  复用认证过滤器 + ota-auth 校验能力，路由到 order    │
└──────────────────┬───────────────────────────────┘
                   │ POST /api/order/create (X-User-Id: 1001)
                   ▼
┌──────────────────────────────────────────────────┐
│            ota-order (8084)                       │
│                                                    │
│  ① OrderController.create()                       │
│  ② 参数校验：日期、房型、数量                      │
│  ③ 订单本地事务：创建平台订单 INIT                 │
│  ④ 调用 ota-inventory/precheck 校验本地价库快照     │
│  ⑤ 调用 ota-inventory/hold 做短时本地预占           │
│     预占只防重复提交，不作为最终库存依据             │
│  ⑥ 调用 ota-supplier/prebook 集团试单/验价          │
│  ⑦ 调用 ota-supplier/book 创建集团订单              │
│     成功 → 保存 supplierOrderNo                    │
│     订单 INIT→PRECHECKED→SUPPLIER_BOOKING→WAIT_PAY │
│     失败 → 释放本地 hold，订单 CLOSED               │
│  ⑧ 定时补偿扫描 supplierOrderNo 和异常订单          │
│     调用 supplier/order/query 或 supplier/cancel    │
└──────────────────┬───────────────────────────────┘
                   │ 用户支付（模拟回调）
                   ▼
┌──────────────────────────────────────────────────┐
│         ota-order 支付回调处理                     │
│                                                    │
│  ① 接收支付成功回调（或模拟回调）                   │
│  ② 订单本地事务：WAIT_PAY → PAID                   │
│  ③ 本地订单状态 WAIT_PAY → PAID                    │
│  ④ 写入 outbox 表：order-paid PENDING              │
│  ⑤ 事务提交后 → Outbox 调度器发送 Kafka             │
│  ⑥ Kafka 消费者通知渠道/自有分销 API                │
│  ⑦ 若发送失败 → outbox 定时任务重试，失败告警        │
└──────────────────┬───────────────────────────────┘
                   │ 5分钟后... 查看订单
                   ▼
┌──────────────────────────────────────────────────┐
│         ota-order 订单查询                         │
│  ① 根据 order_id 查 order 表                      │
│  ② 返回订单完整状态                               │
│  ③ 支持订单取消（状态校验 → 调集团取消 → 释放 hold）│
└──────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────┐
│            ota-ai (8087, P2)                      │
│  AI 与 user/hotel/order/inventory/supplier 并列，是独立业务服务│
│  外部访问仍走 Gateway：/api/ai/recommend、/api/ai/assistant│
└──────────────────────────────────────────────────┘
```

### 1.2 跨服务 TraceId 传递

```
Gateway 生成或接收 TraceId → Micrometer Tracing 写入 tracing context + MDC
         ↓
    Gateway 调用 ota-auth /auth/validate 时自动透传 tracing context
         ↓
    认证通过后路由到业务服务，HTTP Header 继续携带 traceparent / X-Trace-Id
         ↓
    OpenFeign 自动透传 tracing context，日志通过 MDC 输出 traceId/spanId
         ↓
    Zipkin (P1)：Micrometer Tracing + Zipkin Reporter 上报 Span → 展示完整拓扑
         ↓
    ELK (P2)：集中检索带 traceId/orderId 的跨服务日志，补齐日志维度排障
```

> 说明：Spring Boot 3.x 主口径使用 Micrometer Tracing；底层可以桥接 Brave/Zipkin Reporter，但学习和面试时不要把 Brave 当成唯一入口。

### 1.3 完整数据流（从请求到持久化）

```
HTTP Request
  → Gateway: 白名单/认证过滤/限流/TraceId
  → Auth Service: 签发、校验、刷新 Token，检查 Redis 黑名单和权限上下文
  → Supplier Service: 同步集团酒店静态/价库，或执行试单/下单/取消
  → Hotel Service: 查询标准酒店 → Redis 未命中 → MySQL 查询 → Cache-Aside 写入 Redis
  → Inventory Service: Redis 查询标准价库快照 → 未命中 → MySQL 查询 → 本地预校验/hold
  → Order Service:
      订单本地事务: 创建平台订单 INIT
      远程调用 Inventory: 本地价库预校验 + 短时 hold
      远程调用 Supplier: 集团试单/验价 + 集团下单，保存 supplierOrderNo
      支付回调: 平台订单状态 WAIT_PAY → PAID
      Outbox: 订单事件与订单库本地事务绑定 → Kafka 异步发送
      Compensation: 集团下单本地落库失败、集团取消失败、渠道通知失败都进入补偿任务
```

---

## 2. 学习优先级与路线

### 2.1 总体原则

| 优先级 | 时间 | 目标 |
|--------|------|------|
| **P0** | 第 1-7 天 | 跑通核心链路：登录 → 搜索 → 下单 → 支付/取消 |
| **P1** | 第 8-10 天 | 增强可靠性：Kafka异步、链路追踪、流控、API文档、Seata 演示 |
| **P2** | 第 11-13 天 | 扩展能力：AI推荐、分库分表、监控、日志分析、原生编译 |

### 2.2 P0 阶段：核心链路（Day 1-7）

**学什么：**

| 模块 | 核心知识点 | 掌握程度 |
|------|-----------|---------|
| **parent-pom** | JDK 21 虚拟线程配置、Maven BOM 依赖管理、Spring Boot 3.5 + Spring Cloud 2025.0.x + SCA 2025.0.x 版本对齐 | 理解为什么选这些版本、虚拟线程对 Tomcat 的影响 |
| **common** | 统一返回体 R<T>、全局异常处理 @RestControllerAdvice、BeanUtils 工具类、TraceId MDC 集成、Swagger 配置 | 能自定义扩展、理解异常处理链路 |
| **nacos 配置** | 配置中心 vs 注册中心、namespace/group/dataId 三层隔离、配置热更新 @RefreshScope | 能配置灰度发布、回滚方案 |
| **ota-auth** | JWT 签发/校验/刷新、Token 黑名单（Redis）、Spring Security 6 配置、密码加密 BCrypt | 能讲清 JWT 结构、黑名单方案选型比 Redis 好在哪里 |
| **ota-user** | 用户注册 CRUD、MyBatis-Plus 用法、DTO/VO/PO 分层 | 理解 MyBatis-Plus LambdaQueryWrapper |
| **ota-gateway** | Spring Cloud Gateway 路由、JwtAuthGlobalFilter、白名单机制、RequestRateLimiter | 理解 Gateway Filter 执行顺序、WebFlux 与 WebMVC 区别 |
| **ota-hotel** | Cache-Aside 模式、空值缓存防穿透、Redis 短 TTL + 随机抖动、@Cacheable + RedisTemplate | 能讲清缓存击穿/穿透/雪崩的解决方案 |
| **ota-inventory** | 价库合一表设计、乐观锁 version 防超卖、CAS 更新 | 能画 ER 图、讲乐观锁 vs 悲观锁选型 |
| **ota-order** | 订单状态机、跨服务事务（本地事务 + 补偿）、定时任务 @Scheduled | 能画状态机图、讲方案为什么不用分布式事务 |

**面试掌握标准：**
- 能完整画出一条请求的链路图
- 能讲清楚每个模块的核心技术选型理由
- 能回答「为什么订单状态用状态机」「乐观锁怎么防超卖」

### 2.3 P1 阶段：增强可靠性（Day 8-10）

**学什么：**

| 模块 | 核心知识点 | 掌握程度 |
|------|-----------|---------|
| **Kafka** | Topic 设计（4个topic）、Producer 配置（acks=all, retries, idempotence）、Consumer 配置（enable-auto-commit=false, manual commit）、Outbox 模式 + 重试表 | 能讲 Outbox 解决了什么问题、at-least-once vs exactly-once |
| **Zipkin** | Micrometer Tracing、Trace/Span 概念、Custom Span 手动打点、Zipkin Server 部署 | 能读 Zipkin UI 排查慢调用 |
| **Sentinel** | 资源定义 @SentinelResource、流控模式（QPS/线程数）、熔断降级（慢调用/异常比例）、持久化到 Nacos | 能配置流控规则、讲清楚 Sentinel vs Hystrix 区别 |
| **OpenAPI (SpringDoc)** | springdoc-openapi 配置、@Operation/@Schema/@Parameter 注解、分组文档 | 能自动生成 API 文档 |
| **Seata AT** | AT 模式原理（全局锁 + 镜像 + 补偿 SQL）、undo_log 表、@GlobalTransactional | 理解 AT 与 TCC 的区别、AT 的使用限制 |

**面试掌握标准：**
- 能画出 Kafka Outbox 模式的架构图
- 能讲清 Seata AT 模式的「一阶段二阶段」流程
- 能回答 Sentinel 降级后怎么恢复

### 2.4 P2 阶段：扩展能力（Day 11-13）

**学什么：**

| 模块 | 核心知识点 | 掌握程度 |
|------|-----------|---------|
| **Spring AI** | ChatClient / StreamingChatClient、Prompt Template、OutputParser、向量数据库、RAG 模式 | 能实现基于本地知识的酒店问答 |
| **ShardingSphere** | 分片策略（按城市分库、按订单ID取模分表、按酒店ID分表）、分布式主键 (snowflake)、读写分离 | 能配置分片规则、理解分布式事务与分片的冲突 |
| **Prometheus + Grafana** | Micrometer 指标暴露、JVM/接口/DB 监控大盘、Alertmanager 告警规则 | 能搭监控、自定义业务指标 |
| **ELK** | 应用日志采集、Logstash 管道、Elasticsearch 索引、Kibana 检索 | 能按 traceId/orderId 还原一次订单链路的完整日志 |
| **GraalVM** | Spring Boot 3 + GraalVM Native Image、AOT 编译、反射配置 | 理解 native-image 的限制和优势 |

**面试掌握标准：**
- 能讲清楚分片键选型为什么酒店按城市、订单按 ID 取模
- 能展示 AI 推荐的 Prompt Template 设计
- 能说明 Zipkin、Prometheus/Grafana、ELK 分别解决链路、指标、日志问题
- 能说清 GraalVM 原生编译的启动时间优势和反射限制

---

## 3. 各模块关键技术拆解

### 3.1 ota-gateway (P0, Port 8080)

#### 核心技术点

| 技术点 | 说明 | 面试追问 |
|--------|------|---------|
| **Spring Cloud Gateway** | 基于 WebFlux 的响应式网关，作为唯一外部入口 | 为什么不用 Zuul？WebFlux 和 WebMVC 线程模型区别？ |
| **认证过滤器** | 解析 JWT，必要时调用 `ota-auth /auth/validate` 校验 Token、黑名单和权限上下文 | 为什么不是所有请求物理转发到 auth？调用 auth 失败怎么办？ |
| **白名单机制** | 登录、注册、刷新 Token、健康检查和 OpenAPI 放行 | 白名单是硬编码还是 Nacos 配置？怎么动态更新？ |
| **接口限流** | RequestRateLimiter / Sentinel Gateway 保护下游服务 | 令牌桶和漏桶的区别？限流 key 怎么设计（用户级/IP级）？ |
| **TraceId 注入** | Micrometer Tracing 生成/透传 trace context，并写入 MDC | 跨线程池时 TraceId 怎么传递？MDC 在虚拟线程下有问题吗？ |
| **统一错误处理** | 网关级的全局异常处理，返回统一 JSON 格式 | 503 和 504 怎么区分？网关超时怎么处理？ |

#### 关键配置项

```yaml
spring:
  cloud:
    gateway:
      routes:
        - id: ota-auth
          uri: lb://ota-auth
          predicates:
            - Path=/api/auth/**
          filters:
            - StripPrefix=1
        - id: ota-user
          uri: lb://ota-user
          predicates:
            - Path=/api/user/**
          filters:
            - StripPrefix=1
        - id: ota-hotel
          uri: lb://ota-hotel
          predicates:
            - Path=/api/hotel/**
          filters:
            - StripPrefix=1
        - id: ota-inventory
          uri: lb://ota-inventory
          predicates:
            - Path=/api/inventory/**
          filters:
            - StripPrefix=1
        - id: ota-order
          uri: lb://ota-order
          predicates:
            - Path=/api/order/**
          filters:
            - StripPrefix=1
        - id: ota-ai
          uri: lb://ota-ai
          predicates:
            - Path=/api/ai/**
          filters:
            - StripPrefix=1
      default-filters:
        - name: RequestRateLimiter
          args:
            redis-rate-limiter:
              replenishRate: 100
              burstCapacity: 200
              requestedTokens: 1
```

#### 鉴权边界

- 登录、注册、刷新 Token 直接路由到 `ota-auth`。
- 其他外部请求由 Gateway 认证过滤器解析 JWT；本地可验证签名和有效期，黑名单/权限上下文可通过 `ota-auth /auth/validate` 统一确认。
- Gateway 通过 `X-User-Id`、`X-User-Name`、`X-Roles` 向下游传递认证结果；生产环境需要内网隔离、网关签名头或 mTLS，防止外部伪造这些 Header。

#### 虚拟线程注意事项

```yaml
spring:
  threads:
    virtual:
      enabled: true

# Gateway 基于 Netty（WebFlux），默认就是非阻塞模型。
# 虚拟线程主要影响业务服务（WebMVC + Tomcat）中的阻塞调用。
# Gateway Filter 中不要做阻塞 JDBC/Redis 操作；调用 auth 建议使用 WebClient。
```

### 3.2 ota-auth (P0, Port 8081)

#### 核心技术点

| 技术点 | 说明 | 面试追问 |
|--------|------|---------|
| **JWT 签发** | 登录成功后签发 access_token（短 TTL 15分钟）+ refresh_token（长 TTL 7天） | 为什么用双 Token？refresh_token 怎么续期？ |
| **JWT 校验** | 解析 Header + Payload + Signature，nbf/exp/iss 验证；提供 `/auth/validate` 给 Gateway 内部调用 | JWT 无状态怎么做到主动失效？ |
| **Token 黑名单** | Redis SET jti + expire（TTL = token 剩余有效期），登出或强制失效时写入 | 黑名单在 Redis 内存占用怎么评估？有更好方案吗？ |
| **Spring Security 6** | SecurityFilterChain 配置、PasswordEncoder、AuthenticationManager、权限解析 | Spring Security 6 和 5 的主要变化？Filter 链怎么编排？ |
| **BCrypt 加密** | 用户密码 BCryptPasswordEncoder、每次随机 salt | BCrypt 为什么比 MD5/SHA 安全？cost factor 设多少？ |

#### 关键配置项

```yaml
jwt:
  secret: ota-cloud-jwt-secret-key-2026
  access-token-expire: 900       # 15分钟
  refresh-token-expire: 604800   # 7天
  issuer: ota-cloud-auth
```

#### 内部校验接口

```http
POST /auth/validate
Authorization: Bearer <access_token>

Response: {"valid":true,"userId":1001,"username":"test_user","roles":["USER"]}
```

`/auth/validate` 主要给 Gateway 内部调用，用于统一处理黑名单、权限上下文和 Token 异常。业务服务仍应只接受来自 Gateway 或可信内网的用户上下文，不能直接信任外部传入的 `X-User-Id`。

#### 面试追问准备

**Q: JWT 黑名单存 Redis 万一 Redis 挂了怎么办？**
A: Redis 挂了 JWT 校验可以退化——黑名单查不到默认放行，依赖短 TTL（15分钟）兜底。生产建议 Redis 主从 + Sentinel 高可用。

**Q: access_token 能不能更短？比如 5 分钟？**
A: 可以，但会增大 refresh_token 的调用频率。15 分钟是经验值平衡安全性和用户体验。

**Q: refresh_token 过期了让用户重新登录，合理吗？**
A: 合理且安全。如果用户体验要求更高，可以引入 refresh_token 的轮换（rotation）：每次使用 refresh_token 时签发新的 refresh_token，旧的立即失效。

### 3.3 ota-user (P0, Port 8082)

#### 核心技术点

| 技术点 | 说明 | 面试追问 |
|--------|------|---------|
| **MyBatis-Plus CRUD** | BaseMapper + ServiceImpl 快速开发 | MyBatis-Plus 和 MyBatis 的优缺点？ |
| **DTO/VO/PO 分层** | PO 持久化对象、DTO 传输对象、VO 视图对象 | 为什么要分层？MapStruct 和 BeanUtils 选型？ |
| **用户注册** | 参数校验 @Valid + 全局异常处理 | 分布式场景下用户名唯一性怎么保证？ |
| **Redis 缓存用户** | Cache-Aside 模式缓存用户基本信息 | 用户信息变更时缓存怎么更新？ |

### 3.4 ota-hotel (P0, Port 8083)

#### 核心技术点

| 技术点 | 说明 | 面试追问 |
|--------|------|---------|
| **Cache-Aside 模式** | 读：先查缓存 → 未命中查DB → 写入缓存；写：先更新DB → 删除缓存 | 为什么是删除缓存而不是更新？并发下的脏读问题？ |
| **空值缓存** | DB 未命中时缓存一个空值（TTL 30s）防穿透 | 缓存穿透还能怎么防？布隆过滤器为什么不在这里用？ |
| **短 TTL + 随机抖动** | 基础 TTL 600s + random(0, 60)s 防缓存雪崩 | 随机抖动范围怎么设？雪崩和击穿的区别？ |
| **Redis 数据结构** | hotel:detail 用 String (JSON)、价库用 String (JSON) | 什么时候用 Hash 更好？hotel 详情适合吗？ |
| **热点 Key 处理** | 热门酒店可能成为热点 Key | 怎么发现热点 Key？本地缓存 + Redis 双层的方案？ |

#### 缓存 Key 设计

```
hotel:detail:{hotelId}           → HotelDetailDTO (JSON)
hotel:room:{hotelId}:{roomType}  → RoomTypeDTO (JSON)
ps:{hotelId}:{roomType}:{date}   → PriceStockDTO (JSON)
```

### 3.5 ota-inventory (P0/P1, Port 8085)

#### 核心技术点

| 技术点 | 说明 | 面试追问 |
|--------|------|---------|
| **标准价库快照** | 集团 API 返回的房价、库存、取消规则标准化后落本地 | 本地价库和集团实时库存不一致怎么办？ |
| **本地短时 hold** | 创建订单前做短时预占，防重复提交，不作为最终库存依据 | 为什么还要集团试单/验价？ |
| **Redis 价库缓存** | 查询时先读 Redis（批量 mget） | 价库变更后怎么清缓存？Kafka 异步失效？ |
| **供应映射字段** | supplierCode、supplierHotelId、supplierRoomType、ratePlanCode | 多集团字段差异怎么屏蔽？ |

#### 价库快照 SQL 核心

```sql
-- 供应侧同步价库快照
INSERT INTO inventory_price_stock (
  supplier_code, supplier_hotel_id, supplier_room_type,
  rate_plan_code, hotel_id, room_type, biz_date,
  sell_price, cost_price, total, held, booked, available, version
) VALUES (...)
ON DUPLICATE KEY UPDATE
  sell_price = VALUES(sell_price),
  cost_price = VALUES(cost_price),
  total = VALUES(total),
  available = VALUES(available),
  version = version + 1;

-- 本地下单短时 hold，防止重复提交
UPDATE inventory_price_stock
SET held = held + 1, version = version + 1
WHERE supplier_code = ? AND supplier_hotel_id = ?
  AND supplier_room_type = ? AND rate_plan_code = ?
  AND biz_date = ? AND version = ?
  AND available - held >= 1;
```

#### 面试追问准备

**Q: 本地 hold 成功了，集团试单失败怎么办？**
A: 立即释放本地 hold，订单转 CLOSED，并记录供应侧错误码。因为最终库存以集团 API 为准，本地 hold 只是降低重复提交和瞬时并发冲突。

**Q: 为什么不只依赖本地库存？**
A: 酒店集团才是最终履约方。集团价库可能实时变化，所以下单前必须走集团试单/验价，创建集团订单成功后才算真正锁定履约。

**Q: 库存表一天一条记录，一个酒店一年 365 条，数据量看上去不大？**
A: 单个酒店确实不大，但全国 50 万家酒店（含民宿）× 365 天 × 3 个房型 ≈ 5.5 亿行。到达亿级后必须分表，这就是 P2 引入 ShardingSphere 的原因。

### 3.6 ota-supplier (P0/P1, Port 8086)

| 技术点 | 说明 | 面试追问 |
|--------|------|---------|
| **供应端口抽象** | `HotelContentPort`、`RateInventoryPort`、`BookingPort` | 为什么不用一个大接口？ |
| **多集团适配** | mock/华住风格 P0，P1 扩华住、东呈、雅斯特、华驿、锦江 | 不同集团字段差异怎么处理？ |
| **集团下单幂等** | platformOrderNo + supplierCode 唯一，保存 supplierOrderNo | 重复请求集团下单怎么办？ |
| **供应补偿** | supplierOrderNo 查询、取消重试、错误码落库 | 集团成功但本地失败怎么办？ |

老系统映射：

| 新能力 | 老系统参考 |
|--------|------------|
| 酒店静态/房型/价库同步 | `HotelOrderTask`、`huazhu`、`dongcheng`、`yeste`、`hyrj`、`jinjiang` |
| 华住供应 API | `RequestHzApi`：`hotels`、`hotel`、`ari/products`、`ari/daily/offers`、`ari/daily/inventory`、`reservation/prebook/book/cancel/detail` |
| 华驿/雅斯特订单能力 | `RequestHyApi`、`RequestYstApi`：验价、创建订单、取消订单、订单状态 |
| 自有分销与渠道 | `hoa-*`、`gaode`、`baidu` |

### 3.7 ota-order (P0/P1, Port 8084)

#### 核心技术点

| 技术点 | 说明 | 面试追问 |
|--------|------|---------|
| **订单状态机** | 平台订单状态 + 集团下单状态 + 支付状态 | 状态机用策略模式还是枚举？能支持逆向操作吗？ |
| **跨服务一致性** | 平台订单本地事务 + 供应侧幂等接口 + supplierOrderNo 补偿 | 为什么不用 Seata 包住集团 API？ |
| **定时任务补偿** | 扫描 WAIT_PAY、SUPPLIER_BOOKING、CANCEL_RETRY 等异常订单 | 分布式环境下的定时任务不重复执行怎么保证？ |
| **乐观锁订单版本** | order 表也带 version 字段 | 订单状态变更为什么也用了乐观锁？ |
| **Outbox 表 + Kafka (P1)** | 事务内写 outbox → 异步发送 Kafka | Outbox 怎么保证不丢消息？消费端的幂等性怎么设计？ |
| **Kafka 消息幂等** | 消费端用 order_id + event_type 做唯一键去重 | 消息重复了怎么办？为什么是 at-least-once？ |

#### 订单状态机

```
                    ┌─── CANCELLED (主动取消)
                    │
INIT ──→ PRECHECKED ──→ SUPPLIER_BOOKING ──→ WAIT_PAY ──┼─── CLOSED (超时未支付)
          │
          ▼
        PAID ──→ CONFIRMED ──→ CHECKED_IN ──→ COMPLETED
          │
          └─── REFUNDING ──→ REFUNDED (P2)
```

状态转换规则：

| 当前状态 | 可操作 | 目标状态 | 操作人 |
|----------|--------|----------|--------|
| INIT | 本地价库预校验通过 | PRECHECKED | 系统 |
| PRECHECKED | 集团试单/验价通过 | SUPPLIER_BOOKING | 系统 |
| SUPPLIER_BOOKING | 集团下单成功 | WAIT_PAY | 系统 |
| WAIT_PAY | 支付 | PAID | 用户/回调 |
| WAIT_PAY | 取消 | CANCELLED | 用户 |
| WAIT_PAY | 超时 | CLOSED | 系统 |
| PAID | 确认 | CONFIRMED | 系统/商家 |
| CONFIRMED | 入住 | CHECKED_IN | 前台 |
| CHECKED_IN | 离店 | COMPLETED | 系统 |

#### Kafka 消息设计 (P1)

| Topic | 生产者 | 消费者 | 消息内容 |
|-------|--------|--------|---------|
| `supplier-booked` | order 服务 | inventory, channel, distribution | {orderId, supplierCode, supplierOrderNo, timestamp} |
| `order-paid` | order 服务 | channel, distribution, user | {orderId, userId, hotelId, amount, timestamp} |
| `order-cancelled` | order 服务 | supplier, inventory, channel | {orderId, supplierOrderNo, reason, timestamp} |
| `inventory-changed` | supplier/inventory 服务 | channel, distribution | {supplierCode, hotelId, roomType, dates[]} |
| `cache-delete-retry` | 任意服务 | 专用重试消费者 | {cacheKey, retryCount, maxRetries} |

### 3.8 ota-ai (P2, Port 8087)

#### 核心技术点

| 技术点 | 说明 | 面试追问 |
|--------|------|---------|
| **Spring AI ChatClient** | 声明式 AI 调用接口 | Spring AI 和 LangChain4j 的选型对比？ |
| **Prompt Template** | 基于酒店数据构建上下文 | Prompt 注入怎么防？Token 限制怎么处理？ |
| **OutputParser** | 将 LLM 输出解析为结构化数据 | 流式输出怎么解析？ |
| **RAG 模式** | 向量数据库 + 酒店文档切片 | 切片策略怎么设计？向量维度对召回率的影响？ |

---

## 4. 开发路线图（13天版本规划）

### Day 1 - P0-1：基础设施搭建

**目标：** 父 POM、common 模块、Nacos 配置中心

```
ota-cloud/
├── pom.xml                          # 父 POM：JDK21, 虚拟线程, 版本管理
├── ota-common/
│   ├── src/main/java/.../common/
│   │   ├── result/R.java            # 统一返回体
│   │   ├── exception/BizException   # 业务异常
│   │   ├── exception/GlobalExceptionHandler  # 全局异常处理
│   │   ├── constant/Constants.java   # 常量定义
│   │   └── util/BeanUtils.java      # Bean 工具
│   └── pom.xml
└── docker-compose/
    └── nacos/
        ├── docker-compose.yml       # Nacos 3.1.1 standalone
        └── init.sql                 # Nacos 初始化 SQL
```

**完成标准：** mvn clean install 成功，Nacos 控制台可访问 (localhost:8848/nacos)

**关键决策：**
- Spring Cloud Alibaba 2025.0.x + Nacos 3.1.1 版本对齐
- 虚拟线程用 `spring.threads.virtual.enabled=true` 全局开启
- common 模块不引入任何 Spring Boot Starter，保持轻量

### Day 2 - P0-2：认证 + 用户 + 网关

**目标：** auth/user/gateway 三个模块联调，实现登录 → 网关认证链路

```
ota-cloud/
├── ota-auth/
│   ├── src/main/java/.../auth/
│   │   ├── controller/AuthController.java    # POST /auth/login, /auth/refresh, /auth/validate
│   │   ├── config/JwtProperties.java          # JWT 配置
│   │   ├── util/JwtUtil.java                 # JJWT 封装
│   │   └── service/impl/TokenBlacklistService.java  # Redis 黑名单
│   └── src/main/resources/bootstrap.yml      # Nacos 配置
├── ota-user/
│   ├── src/main/java/.../user/
│   │   ├── controller/UserController.java    # CRUD
│   │   ├── entity/User.java                  # PO
│   │   ├── mapper/UserMapper.java
│   │   └── service/impl/UserServiceImpl.java
│   └── bootstrap.yml
└── ota-gateway/
    ├── src/main/java/.../gateway/
    │   ├── filter/JwtAuthGlobalFilter.java   # JWT 校验过滤器
    │   ├── filter/WhiteListFilter.java       # 白名单
    │   └── config/CorsConfig.java            # CORS 配置
    └── bootstrap.yml
```

**完成标准：** curl 登录获取 JWT → 用 JWT 调用户接口 → 网关校验通过

**需启动：** Nacos + MySQL + Redis

### Day 3-4 - P0-3：供应同步 + 酒店 + 价库

**目标：** mock/华住风格供应适配器同步酒店静态和价库，标准酒店搜索 + 标准价库查询 + 缓存穿透保护

```
ota-cloud/
├── ota-hotel/
│   ├── controller/HotelController.java       # 外部 /api/hotel/search, /api/hotel/{id}
│   ├── controller/RoomTypeController.java    # 房型管理
│   ├── service/impl/HotelCacheService.java   # Cache-Aside 实现
│   ├── service/impl/NullValueCacheService.java # 空值缓存
│   └── config/RedisConfig.java               # RedisTemplate 配置
├── ota-inventory/
│   ├── controller/InventoryController.java   # 价库查询
│   ├── entity/InventoryPriceStock.java       # 价库合一表 PO
│   ├── service/impl/PriceStockService.java   # 查询+本地hold
│   └── mapper/InventoryPriceStockMapper.java
├── ota-supplier/
│   ├── port/HotelContentPort.java            # 酒店静态端口
│   ├── port/RateInventoryPort.java           # 价库库存端口
│   └── adapter/mock/MockSupplierAdapter.java # mock/华住风格供应适配器
└── sql/
    └── init/
        ├── hotel.sql                         # hotel, room_type 表
        └── inventory.sql                     # inventory_price_stock 表
```

**完成标准：** 供应同步后可搜索酒店详情（含 Redis 缓存），查询某供应商酒店、房型、日期的标准价库

**需启动：** Nacos + MySQL + Redis + gateway + auth + user + hotel + inventory + supplier

### Day 4-5 - P0-4：订单核心链路

**目标：** 创建平台订单 → 本地价库预校验/hold → 集团试单/验价 → 集团下单 → 支付/取消，跑通完整链路

```
ota-cloud/
├── ota-order/
│   ├── controller/OrderController.java       # 外部 POST /api/order/create, GET /api/order/{id}
│   ├── entity/Order.java                     # order 表 PO
│   ├── entity/enums/OrderStatusEnum.java     # 订单状态枚举
│   ├── service/impl/OrderService.java        # 订单创建 + 事务
│   ├── service/impl/OrderStateMachine.java   # 状态机
│   ├── service/impl/InventoryClient.java     # Feign 调用价库/hold
│   ├── service/impl/SupplierClient.java      # Feign 调用集团试单/下单
│   ├── service/impl/OrderCompensationTask.java # 定时补偿
│   └── service/impl/PaymentCallbackService.java  # 支付回调模拟
```

**完成标准：** 完整跑通：登录 → 供应同步 → 搜索酒店 → 查价库 → 集团试单/下单 → 支付/取消 → 查订单状态和 supplierOrderNo

### Day 5 - P0-5：文档 + 演示

**目标：** README 完善、演示脚本、curl 命令集

```
ota-cloud/
├── README.md                 # 项目介绍、架构图、快速启动
├── docs/
│   ├── API.md                 # API 文档
│   └── ARCHITECTURE.md        # 架构设计文档
├── scripts/
│   ├── demo.sh                # 演示脚本（curl 一键演示）
│   └── init-data.sql          # 测试数据
└── docker-compose/
    └── demo.yml               # 一键启动所有服务
```

**完成标准：** 任意开发者按 README 10 分钟内启动并跑通演示

### Day 6-7 - P0-6：补偿 + 文档 + 演示

**目标：** 完成订单补偿任务、演示脚本和 README/API 文档，保证 P0 主链路可独立演示

```
ota-cloud/
├── ota-order/
│   ├── service/impl/OrderCompensationTask.java # WAIT_PAY 超时释放
│   ├── service/impl/SupplierQueryRetryTask.java # supplierOrderNo 补偿查询
│   └── service/impl/SupplierCancelRetryTask.java # 集团取消失败重试
├── docs/
│   ├── api-demo.md             # API 演示说明
│   └── architecture.md         # 架构设计说明
├── scripts/
│   ├── demo.sh                 # curl 一键演示
│   └── init-data.sql           # 测试数据
└── README.md                   # 快速启动和演示路径
```

**完成标准：** 任意开发者按 README 10 分钟内启动 P0，并跑通登录 → 供应同步 → 酒店查询 → 价库查询 → 集团下单 → 支付/取消。

### Day 8 - P1-1：多集团供应适配 + Kafka Outbox

**目标：** 引入 Kafka，实现 Outbox 模式发送消息，同时把华住、东呈、雅斯特、华驿、锦江供应适配模板补出来

```yaml
# docker-compose/kafka.yml
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.8.0
  kafka:
    image: confluentinc/cp-kafka:7.8.0
    environment:
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
```

```
ota-order/
├── outbox/
│   ├── entity/Outbox.java              # Outbox 表 PO
│   ├── mapper/OutboxMapper.java
│   ├── producer/OrderEventProducer.java # 发送 Kafka 消息
│   └── scheduler/OutboxScheduler.java  # 扫描未发送的 outbox 记录重试

ota-supplier/
├── port/
│   ├── HotelContentPort.java
│   ├── RateInventoryPort.java
│   └── BookingPort.java
├── adapter/
│   ├── huazhu/
│   ├── dongcheng/
│   ├── yeste/
│   ├── hyrj/
│   └── jinjiang/
└── event/
    ├── SupplierBookedConsumer.java      # 集团下单成功事件
    ├── OrderPaidConsumer.java           # 支付成功 → 渠道/自有分销通知
    ├── OrderCancelledConsumer.java      # 取消订单 → 集团取消/通知
    ├── InventoryChangedConsumer.java    # 价库变更 → 清 Redis 缓存
    └── CacheDeleteRetryConsumer.java    # 缓存删除失败重试
```

**完成标准：** 集团下单成功后 outbox 写入 `supplier-booked` → Kafka 消息发送 → 价库缓存/渠道通知正常；可通过 supplierCode 切换不同集团适配模板。

### Day 9 - P1-2：自有分销 API + 平台渠道适配 + Zipkin/Sentinel/OpenAPI

**目标：** 对应 `hoa-*` 自有分销 API 和 `gaode/baidu` 平台渠道适配，同时补链路追踪、流控降级、API 文档

```
ota-distribution-api/
├── controller/DistributionHotelController.java
├── controller/DistributionInventoryController.java
└── controller/DistributionOrderController.java

ota-channel-adapter/
├── gaode/
├── baidu/
└── callback/

docs/
└── openapi.yaml                       # SpringDoc 生成的 API 文档
```

### Day 10 - P1-3：Seata AT 演示

**目标：** 用 Seata AT 模拟分布式事务场景（演示用，非生产路径）

```yaml
seata:
  enabled: true
  application-id: ota-order
  tx-service-group: ota_tx_group
  service:
    vgroup-mapping:
      ota_tx_group: default
```

```
ota-order/
└── service/impl/DemoSeataService.java  # @GlobalTransactional 演示
```

**注意：** P0 用本地事务 + 补偿，Seata 只用于 P1 技术演示，不侵入核心逻辑。

### Day 11 - P2-1：AI 推荐

**目标：** Spring AI 实现酒店推荐和问答

```
ota-ai/
├── controller/AiController.java       # /ai/recommend, /ai/assistant
├── service/impl/HotelRecommendService.java  # RAG + ChatClient
├── config/AiModelConfig.java          # 模型配置
└── prompts/
    ├── recommend.st                   # 推荐提示词模板
    └── qa.st                          # 问答提示词模板
```

### Day 12 - P2-2：ShardingSphere

**目标：** 分库分表实战

```yaml
spring:
  shardingsphere:
    datasource:
      names: ds0, ds1
      ds0:
        url: jdbc:mysql://localhost:3306/ota_city_0
      ds1:
        url: jdbc:mysql://localhost:3306/ota_city_1
    rules:
      sharding:
        tables:
          hotel:
            actual-data-nodes: ds$->{0..1}.hotel_$->{0..1}
            database-strategy:
              standard:
                sharding-column: city_id
                sharding-algorithm-name: city-mod
          orders:
            actual-data-nodes: ds0.orders_$->{0..3}
            table-strategy:
              standard:
                sharding-column: order_id
                sharding-algorithm-name: order-mod
          inventory_price_stock:
            actual-data-nodes: ds$->{0..1}.ips_$->{0..1}
            database-strategy:
              standard:
                sharding-column: hotel_id
                sharding-algorithm-name: hotel-mod
```

### Day 13 - P2-3：监控 + 日志分析 + CI/CD

**目标：** Prometheus + Grafana 监控，ELK 日志检索，GitHub Actions CI

```yaml
# docker-compose/monitoring.yml
services:
  prometheus:
    image: prom/prometheus:v2.55.0
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml
  grafana:
    image: grafana/grafana:11.0.0
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.15.0
  logstash:
    image: docker.elastic.co/logstash/logstash:8.15.0
  kibana:
    image: docker.elastic.co/kibana/kibana:8.15.0
```

### Day 13 - P2-4：GraalVM 编译

**目标：** 体验 native-image 编译

```xml
<!-- pom.xml -->
<plugin>
    <groupId>org.graalvm.buildtools</groupId>
    <artifactId>native-maven-plugin</artifactId>
</plugin>
```

### Day 13 - 总结 + 集成测试

**目标：** 全链路集成测试，架构评审

```
scripts/
├── integration-test.sh             # 全链路集成测试
└── performance-test.sh             # 简单压测 (wrk/ab)
```

---

## 5. 面试演示路径

本演示路径设计为 30 分钟完整演示，每个步骤都包含面试官可能的追问和你的应对回答。

### 步骤 1：项目背景介绍（2 分钟）

**你说：**
> 「这是一个 ota-cloud 微服务项目，我负责整体架构设计和核心链路开发。技术栈选了 JDK 21 虚拟线程 + Spring Boot 3.5 + Spring Cloud 2025.0.x + Spring Cloud Alibaba 2025.0.x 的稳定主线。项目 P0 包含 7 个业务服务，核心不是自建库存下单，而是把酒店集团供应 API 标准化后，再完成平台订单编排。P1 扩展自有分销 API 和高德/百度平台渠道适配，P2 扩展 AI、监控和日志分析能力。」

**面试官追问：**

Q: 为什么选 2025.0.x 而不是更新的 2025.1.x？
> A: 选择 2025.0.x 是因为它是经过 Spring 社区验证的稳定主线，2025.1.x 是升级备选。Spring Cloud Alibaba 需要等社区跟进 Boot 4.x 的兼容适配，我们在需求文档里也说明了升级路径。

Q: 虚拟线程真的在生产环境可用了吗？
> A: JDK 21 的虚拟线程已经是 GA 状态。Spring Boot 3.5 对虚拟线程有原生支持（`spring.threads.virtual.enabled=true`）。需要注意的点：1）synchronized 块会钉住虚拟线程，需要用 ReentrantLock 替代；2）ThreadLocal 在虚拟线程下性能下降（改用 ScopedValue）；3）池化思想（如数据库连接池）与虚拟线程不冲突，因为 IO 操作依然是有限的资源。

### 步骤 2：启动与验证（3 分钟）

**你演示：**
```bash
# 启动基础设施
cd docker-compose && docker compose -f nacos.yml -f mysql.yml -f redis.yml up -d

# 启动各服务（IDE 或命令行）
cd ota-gateway && mvn spring-boot:run
cd ota-auth   && mvn spring-boot:run
cd ota-user   && mvn spring-boot:run
cd ota-hotel  && mvn spring-boot:run
cd ota-inventory && mvn spring-boot:run
cd ota-order  && mvn spring-boot:run
cd ota-supplier && mvn spring-boot:run

# 验证 Nacos 注册中心：P0 7 个业务服务都在线（除 ota-ai）
```

> 「只用 Nacos + MySQL + Redis 就能启动核心链路，不需要 Kafka 等其他基础设施。P0 使用 mock/华住风格 supplier adapter，不依赖真实集团账号，也能演示集团静态/价库同步和集团下单。」

**面试官追问：**

Q: 7 个服务在一台机器上，资源够吗？
> A: 开发环境下完全够。每个服务约 256MB-512MB 堆内存，用虚拟线程后线程数不再是瓶颈。P0 也不需要 Kafka，supplier 使用 mock/华住风格适配器，进一步降低开发环境依赖。

### 步骤 3：注册与登录（3 分钟）

**你演示：**
```bash
# 注册
curl -X POST http://localhost:8080/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username":"test_user","password":"pass123","phone":"13800138000"}'
# → {"code":200,"data":{"userId":1001},"msg":"success"}

# 登录
curl -X POST http://localhost:8080/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"test_user","password":"pass123"}'
# → {"code":200,"data":{"accessToken":"eyJhbGci...","refreshToken":"eyJhbGci...","expiresIn":900}}
```

> 「注意这里登录走的是网关 8080 端口，网关白名单识别到 /api/auth/login 路径，不对 JWT 做校验，直接路由到 auth 服务。auth 服务用 BCrypt 验证密码，签发双 Token——access_token 15 分钟有效，refresh_token 7 天，用于静默续期。」

**面试官追问：**

Q: 双 Token 为什么 access_token 只设 15 分钟？
> A: 短 TTL 是为了安全性：即使 access_token 泄露，攻击者只有 15 分钟窗口。如果用户需要更长时间，客户端用 refresh_token 静默换取新的 access_token，这个过程对用户无感。

Q: access_token 泄露了怎么处理？
> A: 将 JWT 的 jti（JWT ID）加入 Redis 黑名单，黑名单的 TTL 设为 access_token 剩余有效期。所有请求在网关层都会检查黑名单。这个方案最大的优点是实时失效，缺点是要维护 Redis。

Q: 能不能用 refresh_token 向量（refresh token rotation）解决安全问题？
> A: 可以。每次使用 refresh_token 时签发一个新的 access_token + refresh_token，旧的 refresh_token 立即失效。这能有效防止 refresh_token 泄露后的长期滥用。我们的方案保留了扩展空间，P1 阶段可以加入。

### 步骤 4：JWT 网关校验（3 分钟）

**你演示：**
```bash
# 设置 JWT 到变量
TOKEN="eyJhbGci..."

# 用 JWT 查询用户信息（走网关）
curl http://localhost:8080/api/user/profile \
  -H "Authorization: Bearer $TOKEN"
# → {"code":200,"data":{"userId":1001,"username":"test_user"},"msg":"success"}

# 没有 Token 请求受保护路径
curl http://localhost:8080/api/user/profile
# → {"code":401,"msg":"未授权访问，Token不存在"}
```

> 「网关的 JwtAuthGlobalFilter 做了三件事：1）从 Authorization 头提取 JWT；2）解析并校验签名和有效期；3）检查 Redis 黑名单。通过后把 userId 和 userName 放入请求头，传递给下游服务。」

**面试官追问：**

Q: JWT 校验在网关层，那 auth 服务自己还需要校验吗？
> A: auth 服务仍然是认证中心，负责签发、校验、刷新 Token 和维护黑名单。Gateway 是唯一外部入口，会本地解析 JWT，并在需要时调用 auth 的 `/auth/validate` 统一确认黑名单和权限上下文。业务服务可以信任网关注入的用户头，但前提是服务只暴露在内网，并通过网关签名头、mTLS 或网络策略防止 Header 伪造。

Q: 网关是单点，网关挂了怎么办？
> A: 网关可以多实例部署（Nacos 负载均衡），前面加一层 Nginx/LVS 做负载和高可用。此外，网关是无状态的——JWT 本身包含认证信息，重启后不受影响。

### 步骤 5：酒店搜索与缓存策略（4 分钟）

**你演示：**
```bash
# 搜索酒店
curl "http://localhost:8080/api/hotel/search?cityId=1&keyword=希尔顿"
# → {"code":200,"data":[{"hotelId":101,"name":"北京希尔顿酒店",...}]}

# 查看酒店详情（第一次：查 DB 并回填缓存）
curl http://localhost:8080/api/hotel/101
# Response time: ~120ms (cache miss)

# 再次查看（缓存命中）
curl http://localhost:8080/api/hotel/101
# Response time: ~3ms (cache hit)
```

> 「这里实现了 Cache-Aside 模式。第一次查询先走 Redis，key 为 hotel:detail:101，未命中则查 MySQL，结果写入 Redis 并设置 TTL 600 秒加随机 60 秒抖动。我们还做了空值缓存：如果 DB 也没查到，Redis 缓存一个空值 30 秒，防止恶意请求穿透到 DB。」

**面试官追问：**

Q: 为什么「写」策略是删除缓存而不是更新缓存？
> A: 删除缓存是惰性删除，下次读时重新加载。更新缓存的问题是并发写时会导致缓存与 DB 不一致（后写的 DB 但先写的缓存）。删除缓存在写入和读入之间有一个缓存空窗期，这个窗口非常短，实际影响很小。

Q: 删除缓存后，马上有请求进来，还是会穿透到 DB 啊？
> A: 对，但这是 Cache-Aside 的标准行为。如果要避免「缓存击穿」（热点 Key 失效时大量请求打到 DB），可以用互斥锁：只有一个线程去查 DB 回填缓存，其他线程等待锁或快速返回旧值。hotel 场景下不太需要，因为酒店详情不是超高并发 Key。

Q: 随机抖动解决了什么问题？
> A: 缓存雪崩。大量的 Key 在同一时间 TTL 到期，同时回填 DB 造成瞬间压力。在基础 TTL 上增加 0~60s 的随机值，Key 的过期时间分散开，避免了流量尖峰。

### 步骤 6：价库查询与供应快照（4 分钟）

**你演示：**
```bash
# 查询标准价库
curl -X POST http://localhost:8080/api/inventory/price/query \
  -H "Content-Type: application/json" \
  -d '{"supplierCode":"huazhu","hotelId":101,"roomType":"DBL",
       "startDate":"2026-06-01","endDate":"2026-06-03"}'
# → {"code":200,"data":[{"bizDate":"2026-06-01","sellPrice":599.00,
#     "available":10},...]}
```

> 「价库是集团 API 同步后的标准快照，包含 supplierCode、supplierHotelId、roomType、ratePlanCode、价格、库存、取消规则和版本号。查询时先走 Redis 缓存，缓存未命中再查 MySQL。这里的 available 是本地快照，最终是否可订以下单前集团试单/验价为准。」

**面试官追问：**

Q: 本地价库和集团实时价库不一致怎么办？
> A: 本地价库用于搜索和展示，不能作为最终履约依据。创建订单前必须调用集团试单/验价接口，确认实时价格、库存和取消规则；集团下单成功后才保存 supplierOrderNo。

Q: 价库合一表的性能瓶颈是什么？
> A: 主要是 WHERE 条件涉及 hotel_id, room_type, biz_date 三个字段，联合索引建好后查询很快。写操作是行级乐观锁，冲突小。真正的大问题是数据量——全国酒店 5.5 亿行时，单表性能下降严重，这是 P2 阶段引入 ShardingSphere 分表的原因。

Q: 为什么还要本地 hold？
> A: hold 只解决平台侧重复提交和瞬时并发，不表示真的锁住集团库存。最终锁定由集团下单接口完成，hold 失败可以快速拒绝，集团试单失败则释放 hold。

### 步骤 7：集团试单与下单（5 分钟）

**你演示：**
```bash
# 创建订单
curl -X POST http://localhost:8080/api/order/create \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"supplierCode":"huazhu","hotelId":101,"roomType":"DBL","checkIn":"2026-06-01",
       "checkOut":"2026-06-03","quantity":1}'
# → {"code":200,"data":{"orderId":"ORD202606011001",
#     "status":"WAIT_PAY","supplierOrderNo":"HZ202606011001",
#     "amount":1198.00,"paymentUrl":"..."},"msg":"success"}

# 查询供应侧订单状态
curl -X POST http://localhost:8080/api/supplier/order/query \
  -H "Content-Type: application/json" \
  -d '{"supplierCode":"huazhu","supplierOrderNo":"HZ202606011001"}'
# → {"code":200,"data":{"supplierStatus":"BOOKED"}}
```

> 「创建订单时，order 服务先创建平台订单，再做本地价库预校验和短时 hold，随后调用 supplier/prebook 做集团试单/验价，最后调用 supplier/book 创建集团订单。集团返回 supplierOrderNo 后，订单才进入 WAIT_PAY。」

**面试官追问：**

Q: 集团下单成功了，但本地保存 supplierOrderNo 失败怎么办？
> A: 请求集团下单时带 platformOrderNo，供应侧适配器记录请求流水。补偿任务可以用 platformOrderNo 或已返回的 supplierOrderNo 查询集团订单，再修复本地订单状态，避免集团有单、本地无单。

Q: 为什么不用 Seata 包住集团下单？
> A: 集团 API 是外部系统，不参与我们的本地数据库事务，也没有 undo_log。Seata 只能演示本地订单库和本地流水库的一致性；外部集团下单成功后的异常要靠 supplierOrderNo 查询、取消补偿和人工介入兜底。

Q: WAIT_PAY 超时怎么办？
> A: 平台订单超时后，先调用 supplier/cancel 取消集团订单，再释放本地 hold。取消失败进入 CANCEL_RETRY，后台继续按 supplierOrderNo 重试。

### 步骤 8：支付回调与订单事件（3 分钟）

**你演示：**
```bash
# 模拟支付回调
curl -X POST http://localhost:8080/api/order/pay \
  -H "Content-Type: application/json" \
  -d '{"orderId":"ORD202606011001","paymentAmount":1198.00,
       "paymentMethod":"wechat","tradeNo":"WX2026060110012345"}'
# → {"code":200,"msg":"success"}

# 检查订单状态
curl http://localhost:8080/api/order/ORD202606011001 \
  -H "Authorization: Bearer $TOKEN"
# → {"code":200,"data":{"orderId":"ORD202606011001",
#     "status":"PAID"},"msg":"success"}

# 查看 outbox
# event_type = order-paid, payload 中包含 supplierOrderNo
```

> 「支付回调只改变平台支付状态和写 outbox，不再描述成扣减本地库存。集团订单在 supplier/book 时已经创建成功；支付成功后可以通知渠道、自有分销 API 或用户。」

**面试官追问：**

Q: 支付成功但渠道通知失败怎么办？
> A: 订单保持 PAID，通知事件留在 outbox 里重试。渠道通知和用户通知是最终一致，不影响平台订单和集团订单的主状态。

Q: 支付是同步的，那 Kafka 发送失败会阻塞返回吗？
> A: 不会。Kafka 发送是在事务提交后的钩子中执行的，发送失败不会影响主流程返回。我们依赖 outbox 表和定时任务来保证最终送达——如果发送失败，outbox 记录状态为 PENDING，定时任务每 30s 扫描重试，最多 3 次。

### 步骤 9：Kafka Outbox 与最终一致性（P1，3 分钟）

**你演示：**
```bash
# 查看 outbox 表
SELECT * FROM outbox WHERE event_id = 'xxx';
# status: SENT（已成功发送）
# 如果 status 还是 PENDING，说明 kafka 发送失败，等待定时任务重试

# 验证 Kafka 消息消费
# supplier-booked/order-paid 已被渠道或自有分销消费者消费
```

> 「Outbox 模式是保证消息可靠投递的标准方案。核心思想是 `消息写入和业务操作在同一个本地事务中`。本项目重点事件包括 supplier-booked、order-paid、order-cancelled、inventory-changed。」

**面试官追问：**

Q: Outbox 模式和直接发 Kafka 消息有什么区别？
> A: 直接发 Kafka 消息，如果发送成功但数据库事务回滚了，就产生了「幽灵消息」。Outbox 把消息写入和业务操作绑定在同一个事务中，要么一起成功，要么一起回滚，从根本上解决了这个问题。

Q: Outbox 定时任务扫描的间隔是 30 秒，最多 3 次重试，那 90 秒内消息还没送出去怎么办？
> A: 我们的设计保证最多 90 秒延迟（3 次 × 30 秒间隔）。如果 3 次都失败，记录到告警日志，人工介入。正常情况第一次发送成功率应通过监控统计确认，不能在没有历史数据时承诺固定百分比。

Q: why Kafka instead of RocketMQ or RabbitMQ?
> A: 1）高吞吐——OTA 场景的订单事件量大但要求不高（允许秒级延迟）；2）Kafka 的 partition 机制天然支持顺序消费，同一个订单的事件可以保证有序；3）社区生态成熟，Spring Kafka 集成度高；4）团队对 Kafka 运维经验更丰富。

### 步骤 10：订单取消与集团取消（3 分钟）

**你演示：**
```bash
# 取消订单
curl -X POST http://localhost:8080/api/order/cancel \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"orderId":"ORD202606011001","reason":"用户主动取消"}'
# → {"code":200,"data":{"orderId":"ORD202606011001",
#     "status":"CANCELLED"},"msg":"success"}

# 确认集团订单已取消
curl -X POST http://localhost:8080/api/supplier/order/query \
  -H "Content-Type: application/json" \
  -d '{"supplierCode":"huazhu","supplierOrderNo":"HZ202606011001"}'
# supplierStatus=CANCELLED
```

> 「取消操作受状态机约束：WAIT_PAY 状态可以直接取消；如果已经 PAID，则需要按取消规则判断是否走退款。取消时先调 supplier/cancel，再释放本地 hold，并写 order-cancelled outbox。」

**面试官追问：**

Q: 如果把订单状态机用数据库实现，会怎么设计？
> A: 可以建一张 order_state_machine 表存状态转换规则，用触发器或 CHECK 约束强制校验。但用 Java 枚举 + 转换 Map 更灵活、可测试。我们的状态机用 `Map<OrderStatus, List<OrderStatus>>` 维护允许的转换，新增状态时只需要改这一个 Map 和枚举类。

Q: 订单取消后，缓存的价库数据怎么更新？
> A: 取消时同样走 Kafka outbox，发送 order-cancelled 消息。inventory 侧消费后清除 Redis 价库缓存。所以下一个查询价库的请求会重新查 DB 回填缓存，显示最新的可用库存。

### 步骤 11：Sentinel 限流与降级（P1，2 分钟）

**你演示：**
```yaml
# Nacos 配置 sentinel-rule
[
    {
        "resource": "createOrder",
        "count": 100.0,
        "grade": 1,
        "limitApp": "default"
    }
]
```

```bash
# 高并发测试（超过限流阈值）
wrk -t4 -c200 -d30s http://localhost:8080/api/order \
  -H "Authorization: Bearer $TOKEN"
# → 100 QPS 内的请求正常处理
# → 超过的返回：{"code":429,"msg":"Blocked by Sentinel: createOrder"}
```

> 「Sentinel 的降级有几种模式：QPS 限流、线程数限流、慢调用比例降级、异常比例降级。我们主要用 QPS 限流保护下游服务和 DB。规则持久化到 Nacos，实现动态热更新。Sentinel 监控界面可以实时看到每个接口的流量和拒绝情况。」

**面试官追问：**

Q: Sentinel 降级后怎么恢复？
> A: 降级后有两种恢复方式：1）半开恢复（half-open）——允许少量请求通过，如果成功则恢复；2）时间窗口恢复——等待指定时间后自动恢复。我们用的是时间窗口模式，通常设置 30-60 秒的熔断时长。

Q: Sentinel 和 Hystrix 的对比？
> A: Hystrix 已经停止维护。Sentinel 的优势：1）多种流控模式（QPS、线程数、慢调用等）；2）实时监控面板；3）支持动态规则（Nacos/APOLLO）；4）对 Spring Cloud Gateway 和 Feign 有原生支持。

### 步骤 12：Zipkin 链路追踪（P1，2 分钟）

**你演示：**
```bash
# 发起一次请求
curl http://localhost:8080/api/order/ORD202606011001 \
  -H "Authorization: Bearer $TOKEN"

# 查看 Zipkin UI (localhost:9411)
# → 可以看到 Gateway → Auth 校验 → order → inventory 的完整调用链
# → 每个 Span 包含了调用耗时、HTTP 状态码、异常信息
```

> 「Zipkin 通过 Micrometer Tracing 上报 Span，自动收集 Spring Cloud Gateway、OpenFeign 和 WebClient/RestTemplate 的调用信息。TraceId 在整个链路中透传，从网关、Auth 校验到每个业务服务。我们可以通过 Zipkin 发现慢调用——比如 inventory 的 DB 查询如果超过 500ms，就会红色高亮。」

**面试官追问：**

Q: Zipkin 的性能开销大吗？
> A: 采样率是关键。生产环境设置采样率 0.01~0.1（1%~10%），不需要全量采集。Micrometer Tracing 的埋点开销很小，每个 Span 约几微秒的开销。我们通过 Nacos 配置采样率，可以在问题排查时动态调高。

### 步骤 13：Seata AT 演示（P1，2 分钟）

**你演示：**
```bash
# 启动 Seata Server
docker compose -f docker-compose/seata.yml up -d

# 配置 Seata（已内置 DataSourceProxy）
# 在 DemoSeataService 上加 @GlobalTransactional
# 同时操作 order 表和 inventory 表

# 演示 Seata 事务回滚
# 执行一个故意失败的支付操作，观察 undo_log 表
SELECT * FROM undo_log WHERE branch_id = ?;
# → rollback_info 字段中包含回滚 SQL
```

> 「Seata AT 模式对代码侵入最小：只需要在 Service 上加 @GlobalTransactional 注解。它的原理是：一阶段业务 SQL + 生成 undo_log（前镜像 + 后镜像）；二阶段如果成功，删除 undo_log；如果失败，用 undo_log 中的前镜像回滚。在我们的项目中，Seata 只用于 P1 技术演示，P0 核心链路还是用本地事务 + 定时补偿——因为 Seata 有性能开销，而且不是所有场景都需要强一致性。」

**面试官追问：**

Q: Seata AT 和 TCC 模式的区别？
> A: AT 是自动补偿——框架自动生成回滚 SQL，开发者无感知。TCC 需要手动实现 Try/Confirm/Cancel 三个接口，对业务代码有侵入。外部集团 API 不能被 Seata AT 自动回滚，所以本项目 P0 用 supplierOrderNo 查询、取消补偿和 outbox 处理外部一致性；Seata 只演示本地库一致性。

Q: Seata AT 在虚拟线程下有兼容问题吗？
> A: Seata 目前依赖 ThreadLocal 来做 XID 传递。JDK 21 虚拟线程下 ThreadLocal 性能下降，需要用 seata 的虚拟线程兼容方案（替代为 ScopedValue 或复制 TransmittableThreadLocal）。这是我们在 P1 中需要验证的。

### 步骤 14：Spring AI 演示（P2，3 分钟）

**你演示：**
```bash
# 酒店推荐（外部访问仍走 Gateway，不直连 8087）
curl -X POST http://localhost:8080/api/ai/recommend \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"cityId":1,"preferences":["亲子","泳池","早餐"],"budget":"1000-2000"}'
# → GPT/国产大模型返回推荐结果，包含酒店详情和推荐理由

# 酒店问答
curl -X POST http://localhost:8080/api/ai/assistant \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"hotelId":101,"question":"是否有接机服务？退房时间是几点？"}'
# → 基于酒店详细信息的 RAG 问答
```

> 「Spring AI 1.1.7 提供了统一的 ChatClient API，支持切换不同的 LLM 提供商。我们用了 Prompt Template 来构造提示词，结合酒店数据做上下文增强。RAG 模式下，酒店说明文档被切分成 chunks，存储到向量数据库，查询时检索相关内容拼入 Prompt。」

**面试官追问：**

Q: Spring AI 和直接调 OpenAI API 有什么区别？
> A: Spring AI 是 Spring 生态原生——可以用熟悉的 Bean 管理和配置方式。它提供了 ChatClient 声明式接口、OutputParser 结构化输出、Prompt Template、向量数据库抽象等。最大的好处是切换提供商只需要改配置文件。

Q: 怎么保证 LLM 回答的准确性？
> A: 1）RAG 模式——只基于提供的酒店数据回答，不依赖 LLM 内部知识；2）提示词中明确约束「如果信息中找不到，如实告知」；3）输出解析为结构化 JSON，业务侧做字段校验。完全避免 LLM 幻视，关键信息（如价格）仍然从业务 API 获取。

### 步骤 15：压测与性能（3 分钟）

**你演示：**
```bash
# 用 wrk 做简单压测
wrk -t4 -c100 -d60s \
  -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/api/hotel/101

# 虚拟线程 + 缓存命中
# 记录：机器配置、JDK版本、服务堆内存、数据量、缓存命中率、Hikari/Feign配置、采样时间
# 压测目标示例：观察 QPS、P95、P99 和错误率，不在没有报告时承诺固定数值

# 读 DB 的场景（缓存未命中，压 uncached 端点）
wrk -t4 -c50 -d30s \
  http://localhost:8080/api/hotel/102 \
  -H "Authorization: Bearer $TOKEN"

# 记录 DB 查询耗时、连接池等待、Redis 命中率和慢 SQL；结果以压测报告为准
```

> 「压测主要验证：1）虚拟线程下 Tomcat 的线程模型表现——传统线程池每个请求一个线程，虚拟线程可以支持数万并发；2）缓存命中率——记录实际命中率，并结合数据规模判断是否需要调 TTL 或预热；3）乐观锁性能——高并发下单场景下 version 冲突率的监控。」

**面试官追问：**

Q: 虚拟线程 + Tomcat 的配置怎么调优？
> A: Tomcat 的 max-threads 默认 200，开启虚拟线程后这个配置失效——每个请求直接创建一个虚拟线程，由 JVM ForkJoinPool 调度。需要调的是：1）数据库连接池（HikariCP 默认 10，压测时可以适当调高）；2）Feign 的连接池和超时时间；3）Kafka 的生产者缓冲区大小。

Q: 你预期这个项目的性能瓶颈在哪里？
> A: 两个瓶颈：1）库存的乐观锁更新——如果同一房型同一天有大量并发下单（秒杀），version 冲突导致重试，TPS 会下降。2）MySQL——所有核心状态都落地到 DB，读有缓存兜底，但写（订单创建、库存更新）必须写 DB。如果要达到更高的 TPS，需要引入 Redis 库存预扣 + 异步落库的折中方案，或者分库分表。

### 步骤 16：总结与架构评审（2 分钟）

**你总结：**

> 「总结一下这个项目的设计哲学：
>
> **第一，务实的技术选型——** 选了 Spring Boot 3.5 稳定主线 + JDK 21（GA 版本），没有追新。P0 只需要 Nacos + MySQL + Redis 三个基础设施就能跑通核心链路。
>
> **第二，合理的分布式事务策略——** 核心链路用本地事务 + 乐观锁 + 定时补偿，没有一刀切上 Seata。在 P1 才引入 Seata 做技术演示，P0 保持轻量。
>
> **第三，渐进式架构演进——** 从 P0 的 7 个服务和三个基础设施，到 P1 的多集团供应适配、自有分销 API、平台渠道适配和 Kafka/Zipkin/Sentinel，再到 P2 的 AI/分库分表/Prometheus/Grafana/ELK/GraalVM。每个阶段都有明确的业务价值和技术目标。
>
> **第四，充分的前瞻性——** 价库合一表设计时就考虑了分库分表的分片键（city_id, hotel_id, order_id），代码预留了六边形架构的接口抽象。缓存策略考虑了穿透、击穿、雪崩的三种保护。」

**最终追问准备：**

Q: 如果让你重做这个项目，你会改什么？
> A: 三件事：1）库存模块的 Inbound/Outbound Adapter（六边形架构）应该提前到 P0 而不是 P1，早期分离接口和实现有利于测试和后续扩展；2）可以考虑在 P0 就引入 Kafka（而不是 P1），因为 order 模块的 outbox 模式和事件驱动是核心架构的一部分；3）API 文档（OpenAPI）应该在写 Controller 时就同步维护，P1 再补会比较痛苦。不过这些是 hindsight，当前的分阶段策略在资源有限的情况下是最务实的。

Q: 这个项目最大亮点是什么？
> A: 四个字——**知行合一**。从技术选型、版本规划、模块划分到代码实现，每个决策都有明确的技术理由和 trade-off 分析。没有为了用分布式事务而用 Seata，没有为了微服务而微服务化每个模块，没有为了虚拟线程而强行改所有代码。P0 跑通链路，P1 加可靠性，P2 扩能力——每一步都是验证过的，不是拍脑袋。

---

## 附录

### A. 端口汇总

| 服务 | 端口 | 阶段 | 启动顺序 |
|------|------|------|----------|
| Nacos | 8848 | 基础设施 | 1 |
| MySQL | 3306 | 基础设施 | 1 |
| Redis | 6379 | 基础设施 | 1 |
| ota-gateway | 8080 | P0 | 2 |
| ota-auth | 8081 | P0 | 3 |
| ota-user | 8082 | P0 | 3 |
| ota-hotel | 8083 | P0 | 4 |
| ota-inventory | 8085 | P0 | 4 |
| ota-order | 8084 | P0 | 5 |
| ota-ai | 8087 | P2 | 6 |
| Zipkin | 9411 | P1 | - |
| Kafka | 9092 | P1 | - |
| Prometheus | 9090 | P2 | - |
| Grafana | 3000 | P2 | - |
| Elasticsearch | 9200 | P2 | - |
| Logstash | 5044 | P2 | - |
| Kibana | 5601 | P2 | - |
| Seata | 8091 | P1 | - |

### B. 关键依赖版本

| 依赖 | 版本 | 说明 |
|------|------|------|
| JDK | 21 LTS | 虚拟线程 |
| Spring Boot | 3.5.14 | 稳定主线 |
| Spring Cloud | 2025.0.x | 对应 Boot 3.5.x |
| Spring Cloud Alibaba | 2025.0.x | 对应 Cloud 2025.0.x |
| Nacos | 3.1.1 | 配置中心 + 注册中心 |
| MyBatis-Plus | 3.5.16 | ORM |
| MySQL | 8.0+ | 数据库 |
| Redis | 7.x | 缓存 + 黑名单 |
| Kafka | 3.9.x (P1) | 异步消息 |
| Sentinel | 1.8.10 (P1) | 限流降级 |
| Seata | 2.6.0 (P1) | 分布式事务演示 |
| Zipkin | 3.x (P1) | 链路追踪 |
| ShardingSphere | 5.x (P2) | 分库分表 |
| Spring AI | 1.1.7 (P2) | AI 集成 |
| ELK | 8.x (P2) | 集中日志检索 |

### C. 推荐学习资源

| 主题 | 资源 |
|------|------|
| JDK 21 虚拟线程 | JEP 444: Virtual Threads (OpenJDK)，Project Loom 官方文档 |
| Spring Boot 3.5 | Spring Boot Reference Documentation |
| Spring Cloud Gateway | Spring Cloud Gateway Reference Guide |
| JWT | RFC 7519, JJWT GitHub |
| Kafka | Kafka: The Definitive Guide (Confluent) |
| Seata AT 原理 | Seata 官方文档 - AT 模式 |
| Sentinel | Sentinel 官方 Wiki (GitHub) |
| Micrometer Tracing + Zipkin | Spring Boot Observability 文档、OpenZipkin 文档 |
| ELK | Elastic 官方文档、Logstash Pipeline 文档、Kibana Discover 文档 |
| Spring AI | Spring AI Reference Documentation |
| ShardingSphere | ShardingSphere 官方文档 - 分片配置 |

---

> **最后的话：**
> 这个学习指南的目标不是让你背答案，而是帮你建立全链路视角。
>
> 面试官问一个「集团供应下单怎么保证一致」的时候，你要能答出：
> - 本地价库只是快照，最终以下单前集团试单/验价为准
> - 平台订单号、supplierCode、supplierOrderNo 如何映射
> - 集团下单成功但本地失败怎么补偿查询
> - 取消失败为什么要进入 CANCEL_RETRY
> - Outbox 负责渠道和自有分销通知的最终一致性
> - Seata AT 为什么只能解决本地库，不能回滚外部集团 API
>
> 这就是 19 年经验应该有的深度。
