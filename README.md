# OTA 酒店云平台开发文档与实战教程 (hotel-ota-doc)

本项目归档汇总了 **OTA 酒店分布式云平台 (ota-cloud)** 的全套设计文档、分章实战教程、全景技术栈架构指南以及业务设计与接口契约。

---

## 📂 目录结构

```text
hotel-ota-doc/
├── knowledgebase/                     # 核心实战教程与架构知识库 (35+ 篇)
│   ├── _index_.md                     # 知识库完整目录索引
│   ├── README.md                      # 实战教程总览与学习路线
│   ├── ota-cloud-需求文档.md           # 总体业务需求与数据模型
│   ├── ota-cloud-链路学习指南.md       # 全链路技术实现与学习指南
│   ├── ota-cloud-演进任务看板.md       # 演进任务排期与验收清单
│   ├── channel-hmac-authentication-guide.md # 渠道 API HMAC 签名鉴权指南
│   ├── chapter01~16-*.md              # 分章实战教程 (Chapter 1 至 16)
│   └── *-architecture-guide.md        # 11 个核心中间件全景架构指南
│
└── docs/                              # 业务与设计契约文档
    ├── 交易接口契约.md
    ├── 渠道API接入签名鉴权指南.md
    ├── 系统交互流程工作台设计方案.md
    ├── 业务表与老系统差异分析.md
    ├── 三阶段交易闭环验收记录.md
    └── ...                            # 阶段实施与验收复核记录
```

---

## 📖 核心文档快速入口

- **知识库总索引**：[knowledgebase/_index_.md](./knowledgebase/_index_.md)
- **实战教程总览**：[knowledgebase/README.md](./knowledgebase/README.md)
- **需求与架构设计**：[knowledgebase/ota-cloud-需求文档.md](./knowledgebase/ota-cloud-需求文档.md)
- **全链路开发学习指南**：[knowledgebase/ota-cloud-链路学习指南.md](./knowledgebase/ota-cloud-链路学习指南.md)
- **渠道 API HMAC 签名鉴权指南**：[knowledgebase/channel-hmac-authentication-guide.md](./knowledgebase/channel-hmac-authentication-guide.md)
- **交易接口契约规范**：[docs/交易接口契约.md](./docs/交易接口契约.md)
- **系统交互流程工作台方案**：[docs/系统交互流程工作台设计方案.md](./docs/系统交互流程工作台设计方案.md)

---

## 🛠️ 分章实战教程（Chapter 01 ~ 16）

| 章节 | 核心主题 | 说明 |
| :--- | :--- | :--- |
| **Chapter 01** | [项目骨架与 Nacos](./knowledgebase/chapter01-project-scaffold-nacos.md) | 父 POM、公共模块、Nacos 注册与配置中心、Docker Compose |
| **Chapter 02** | [Gateway 与 Auth](./knowledgebase/chapter02-gateway-auth.md) | Spring Cloud Gateway、JWT 认证鉴权、用户权限控制 |
| **Chapter 03** | [Hotel + Redis 缓存](./knowledgebase/chapter03-hotel-service-redis-cache.md) | 酒店/房型服务、Cache-Aside 模式、击穿/穿透/雪崩防御 |
| **Chapter 04** | [Inventory 价库库存](./knowledgebase/chapter04-inventory-service.md) | 价库合一、乐观锁防超卖、冻结/扣减/释放流水 |
| **Chapter 04b** | [Admin 运营中台](./knowledgebase/chapter04b-admin-service.md) | 下游渠道配置、加价策略、来源映射管理 |
| **Chapter 05** | [Order 订单中心](./knowledgebase/chapter05-order-service.md) | 订单有限状态机、分布式幂等、自动补偿 Job |
| **Chapter 06** | [Kafka 事务发件箱](./knowledgebase/chapter06-kafka-outbox.md) | Transactional Outbox 模式、可靠投递、死信隔离 |
| **Chapter 07** | [进阶分布式技术](./knowledgebase/chapter07-advanced-tech.md) | Zipkin、Sentinel、Seata、ShardingSphere、Spring AI、Prometheus、ELK |
| **Chapter 08** | [自动化回归与冒烟测试](./knowledgebase/chapter08-regression-smoke.md) | 全链路回归测试与 Docker 真实环境冒烟自动化 |
| **Chapter 08b** | [作品演示站 Demo](./knowledgebase/chapter08b-portfolio-demo.md) | Vite + React 可视化大屏与全链路调用拓扑图 |
| **Chapter 09** | [Supplier 上游适配](./knowledgebase/chapter09-supplier-service.md) | 华住 CRS 协议适配、SupplierAdapter 统一端口、反向对账 |
| **Chapter 10** | [Merchant 商家中心 (归档)](./knowledgebase/chapter10-merchant-service.md) | 商家独立 RBAC 与数据隔离模型归档 |
| **Chapter 11** | [Channel 下游分销](./knowledgebase/chapter11-channel-service.md) | 下游渠道 API 分发、动态加价引擎、HMAC 验签 |
| **Chapter 12** | [可观测性与告警](./knowledgebase/chapter12-observability.md) | Prometheus 指标监控、Alertmanager 告警路由、Grafana 仪表盘 |
| **Chapter 13** | [多级缓存高可用](./knowledgebase/chapter13-multilevel-cache.md) | Caffeine L1 + Redis L2 多级缓存、热点预热与 Single-Flight 防护 |
| **Chapter 14** | [Sentinel 高并发韧性](./knowledgebase/chapter14-sentinel-resilience.md) | 网关流控、热点参数限流、上游熔断降级与规则热加载 |
| **Chapter 15** | [Seata 分布式事务](./knowledgebase/chapter15-seata-distributed-transaction.md) | Seata AT 模式、跨微服务 XID 透传、两阶段提交与回滚演练 |
| **Chapter 16** | [交易闭环与履约优化](./knowledgebase/chapter16-business-optimization-transaction-closure.md) | 静态归一、房型映射、共享配额池、INSTANT/ASYNC 履约模型与通知机制 |

---

## 🏛️ 技术栈全景架构指南

- [Spring Cloud Gateway 网关架构指南](./knowledgebase/gateway-architecture-guide.md)
- [Nacos 注册与配置中心架构指南](./knowledgebase/nacos-architecture-guide.md)
- [Redis 缓存设计指南](./knowledgebase/redis-cache-architecture-guide.md)
- [MySQL & MyBatis-Plus 持久层指南](./knowledgebase/mysql-mybatis-architecture-guide.md)
- [Kafka 消息总线架构指南](./knowledgebase/kafka-architecture-guide.md)
- [Sentinel 容灾韧性架构指南](./knowledgebase/sentinel-architecture-guide.md)
- [Seata 分布式事务架构指南](./knowledgebase/seata-architecture-guide.md)
- [ShardingSphere 分库分表架构指南](./knowledgebase/shardingsphere-architecture-guide.md)
- [Prometheus + Grafana 监控告警架构指南](./knowledgebase/prometheus-grafana-architecture-guide.md)
- [ELK + Zipkin 链路追踪可观测性指南](./knowledgebase/observability-elk-zipkin-guide.md)
- [Spring AI 智能助手集成指南](./knowledgebase/spring-ai-architecture-guide.md)
