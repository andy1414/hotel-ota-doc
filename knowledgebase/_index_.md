# ota-cloud 知识库索引

> 更新时间：2026-11-02  
> 涵盖：OTA 酒店云平台需求设计、架构演进规划、全链路学习指南、分章实战教程、业务优化与交易闭环专题、渠道HMAC签名接入指南、生产级全景架构指南（共 35 篇文档）

---

## 📋 一、总体规划与核心文档（6篇）

| 文档 | 说明 |
|------|------|
| [ota-cloud-需求文档](./ota-cloud-需求文档.md) | 业务背景、微服务架构边界、P0~P2 阶段需求与数据模型规范 |
| [ota-cloud-链路学习指南](./ota-cloud-链路学习指南.md) | 从全链路视角拆解请求追踪、各模块关键技术、13天开发路线与面试演示路径 |
| [ota-cloud-演进实施计划](./ota-cloud-演进实施计划.md) | 从 `otaapi` Monorepo 演进到现代微服务平台的实施计划、模块映射与验收口径 |
| [ota-cloud-演进任务看板](./ota-cloud-演进任务看板.md) | P0/P1/P2 及高可用扩展（Chapter 10~15）、三阶段交易闭环优化任务进度看板与验收清单 |
| [渠道API HMAC签名鉴权指南](./channel-hmac-authentication-guide.md) | 下游渠道分销 API 接入规范、HMAC-SHA256 签名机制、时间戳与 Nonce 防重放、Java 原生客户端代码示例与排障指南 |
| [实战教程总览 (README)](./README.md) | 分章实战教程路线、服务端口统一约定、快速启动命令与全景技术栈导航 |

---

## 🛠️ 二、分章实战教程与演进专题（18篇）

| 章节 | 对应阶段 | 内容概要 |
|------|----------|----------|
| [第1章：项目骨架与Nacos](./chapter01-project-scaffold-nacos.md) | P0-1 | 父POM、`ota-common`、Nacos 注册+配置中心、Docker Compose |
| [第2章：Gateway与Auth](./chapter02-gateway-auth.md) | P0-2 | JWT 认证、Gateway 路由鉴权、用户管理 |
| [第3章：Hotel + Redis缓存](./chapter03-hotel-service-redis-cache.md) | P0-3 | 酒店/房型服务、Cache-Aside、缓存穿透/击穿防护 |
| [第4章：Inventory价库服务](./chapter04-inventory-service.md) | P0-3 | 价库合一表、乐观锁冻结/释放/扣减、库存流水 |
| [第4章补充：Admin中台配置后端一期](./chapter04b-admin-service.md) | P0-3.5 | 下游渠道、可售范围/加价系数、酒店来源候选映射、价库来源字段 |
| [第5章：Order订单链路](./chapter05-order-service.md) | P0-4 | 订单状态机、库存幂等接口、库存流水、定时补偿任务 |
| [第6章：Kafka事件驱动](./chapter06-kafka-outbox.md) | P0-6 | Outbox 模式、重试+死信队列 |
| [第7章：进阶技术](./chapter07-advanced-tech.md) | P1~P2 | Zipkin / Sentinel / Seata / ShardingSphere / Spring AI / Prometheus+Grafana / ELK |
| [第8章：回归测试与真实环境Smoke](./chapter08-regression-smoke.md) | P0 收敛 | 全模块单元与集成测试、真实 Docker 中间件冒烟自动化脚本 |
| [第8.5章：作品展示Demo前端](./chapter08b-portfolio-demo.md) | 展示站 | 独立 Vite + React 前端、全链路调用回放状态机、分层网络拓扑图 |
| [第9章：上游供应方适配](./chapter09-supplier-service.md) | P1-1 | 华住 CRS/B2B 协议适配、`SupplierAdapter` 统一端口、反向对账与补偿 Job |
| [第10章：商家中心（代码已归档退役）](./chapter10-merchant-service.md) | 业务扩展 | 商家独立账号与 RBAC 体系、`MerchantContext` 隔离；现主体已纠偏为平台/供应商/渠道 |
| [第11章：下游渠道分发](./chapter11-channel-service.md) | P1-2 | 下游分销适配、渠道动态加价引擎、推单/查单/取消、Kafka 状态事件总线闭环 |
| [第12章：可观测性与监控告警](./chapter12-observability.md) | 生产就绪 | Prometheus 业务指标、Alertmanager 8条告警规则、Grafana 4大仪表盘、ELK 集中日志 |
| [第13章：缓存与高可用](./chapter13-multilevel-cache.md) | 高可用扩展 | Caffeine L1 + Redis L2 多级缓存骨架、订单读缓存、Hot Key 预热、穿透与雪崩 Single-Flight 防御 |
| [第14章：Sentinel高并发韧性](./chapter14-sentinel-resilience.md) | 容灾防卫 | 网关前置流控与 429 统一返回、日历价库热点参数防护 (`hotelId`)、上游供应慢调用/异常熔断与 Nacos 持久化 |
| [第15章：Seata分布式事务](./chapter15-seata-distributed-transaction.md) | 一致性进阶 | Seata 2.6.x AT 模式、跨微服务 XID 透传、两阶段提交（2PC）、`undo_log` 反向镜像补偿与回滚演练 |
| [第16章：业务优化与三阶段交易闭环专题](./chapter16-business-optimization-transaction-closure.md) | 核心业务闭环 | 静态来源归一、房型映射、共享配额池、INSTANT/ASYNC履约模型、权威取消事实结构化与可靠通知游标机制 |

---

## 🏗️ 三、全景技术栈架构指南（11篇）

| 技术栈                      | 架构指南文档                                                        | 核心机制                                                     |
| ------------------------ | ------------------------------------------------------------- | -------------------------------------------------------- |
| **Spring Cloud Gateway** | [网关架构指南](./gateway-architecture-guide.md)                     | WebFlux 响应式、JWT 全局认证、全局 TraceId 染色、动态路由                  |
| **Alibaba Nacos**        | [Nacos 架构指南](./nacos-architecture-guide.md)                   | 命名空间物理隔离、共享配置继承、`@RefreshScope` 免重启热更新                   |
| **Redis & 多级缓存**         | [Redis 缓存指南](./redis-cache-architecture-guide.md)             | Cache-Aside 读写模式、分布式互斥锁防击穿、空值防穿透、随机 TTL 防雪崩、Caffeine L1  |
| **MySQL & MyBatis-Plus** | [MySQL 持久层指南](./mysql-mybatis-architecture-guide.md)          | 价库合一表设计、CAS 乐观锁防超卖、流水表幂等、严格 XML SQL 规范                   |
| **Apache Kafka**         | [Kafka 架构指南](./kafka-architecture-guide.md)                   | Transactional Outbox 事务发件箱、跨进程 TraceId 透传、手动 ACK、死信隔离    |
| **Alibaba Sentinel**     | [Sentinel 防护指南](./sentinel-architecture-guide.md)             | QPS 流量整形、爆款酒店热点参数流控、慢调用熔断、`blockHandler`/`fallback`      |
| **Apache Seata**         | [Seata 事务指南](./seata-architecture-guide.md)                   | 无侵入 AT 模式、两阶段提交（2PC）、`undo_log` 反向补偿镜像                   |
| **ShardingSphere**       | [Sharding 分表指南](./shardingsphere-architecture-guide.md)       | 订单 16 分表 (`% 16`)、房价库存 100 分表 (`hotel_id % 100`)、跨分片分页归并 |
| **Prometheus + Grafana** | [Prometheus 监控指南](./prometheus-grafana-architecture-guide.md) | Micrometer 业务指标暴露、8 条核心业务告警规则、Alertmanager 分级路由抑制        |
| **ELK + Zipkin**         | [可观测性指南](./observability-elk-zipkin-guide.md)                 | W3C TraceId 透传、Logstash TCP JSON 直推、ES 按天滚动、Zipkin 耗时火焰图 |
| **Spring AI**            | [Spring AI 架构指南](./spring-ai-architecture-guide.md)           | Prompt 工程、智能酒店推荐与行程助手、虚拟线程（Virtual Threads）、无 Key 稳定降级   |

---

[返回知识库总索引](../_index_.md)
