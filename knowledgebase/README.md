# ota-cloud 实战教程

> 配套 [ota-cloud-链路学习指南](./ota-cloud-链路学习指南.md) 的具体代码和配置教程  
> 版本：v1.3 | 基于业务优化与三阶段交易闭环设计 2026-10-31

## 学习路线

从搭建骨架到跑通全链路，以下按推荐顺序学习：

| 章节 | 对应阶段 | 时间 | 内容 |
|------|----------|------|------|
| [第1章：项目骨架与Nacos](./chapter01-project-scaffold-nacos.md) | P0-1 | 1天 | 父POM、ota-common、Nacos注册+配置中心、docker-compose |
| [第2章：Gateway与Auth](./chapter02-gateway-auth.md) | P0-2 | 1天 | JWT认证、Gateway路由鉴权、用户管理 |
| [第3章：Hotel + Redis缓存](./chapter03-hotel-service-redis-cache.md) | P0-3 | 1天 | 酒店/房型服务、Cache-Aside、缓存穿透/击穿防护 |
| [第4章：Inventory价库服务](./chapter04-inventory-service.md) | P0-3 | 1天 | 价库合一表、乐观锁冻结/释放/扣减、库存流水 |
| [第4章补充：Admin中台配置后端一期](./chapter04b-admin-service.md) | P0-3.5 | 0.5天 | 下游渠道、可售范围/加价系数、酒店来源候选映射、价库来源字段 |
| [第5章：Order订单链路](./chapter05-order-service.md) | P0-4 | 2天 | 订单状态机、库存幂等接口、库存流水、定时补偿任务 |
| [第6章：Kafka事件驱动](./chapter06-kafka-outbox.md) | P0-6 (Bonus) | 1天 | outbox模式、重试+死信队列、[Kafka全景架构指南](./kafka-architecture-guide.md) |
| [第7章：进阶技术](./chapter07-advanced-tech.md) | P1-2~P2-3 | 7项各1天 | Zipkin / Sentinel / Seata / ShardingSphere / Spring AI / Prometheus+Grafana / ELK |
| [第8章：回归测试与真实环境Smoke](./chapter08-regression-smoke.md) | P0 收敛 | 1天 | Focused Tests、全模块单元与集成测试、真实 Docker 中间件冒烟自动化脚本 |
| [第8.5章：作品展示Demo前端](./chapter08b-portfolio-demo.md) | 展示站 | 1天 | 独立 Vite + React 前端、全链路调用回放状态机、分层网络拓扑图 |
| [第9章：上游供应方适配](./chapter09-supplier-service.md) | P1-1 | 2天 | 华住 CRS/B2B 协议适配、SupplierAdapter 统一端口、反向对账与补偿 Job |
| [第10章：商家中心（代码已归档退役）](./chapter10-merchant-service.md) | 业务扩展 | 2天 | 商家独立账号与 RBAC 体系；现核心经营主体已纠偏为平台/供应商/渠道 |
| [第11章：下游渠道分发](./chapter11-channel-service.md) | P1-2 | 2天 | 下游分销适配、渠道动态加价引擎、推单/查单/取消、Kafka 状态事件总线闭环；配套 [渠道HMAC签名鉴权指南](./channel-hmac-authentication-guide.md) |
| [第12章：可观测性与监控告警](./chapter12-observability.md) | 生产就绪 | 2天 | Prometheus 业务指标、Alertmanager 8条告警规则、Grafana 4大仪表盘、ELK 集中日志 |
| [第13章：缓存与高可用](./chapter13-multilevel-cache.md) | 高可用扩展 | 2天 | Caffeine L1 + Redis L2 多级缓存骨架、订单读缓存、Hot Key 预热、穿透与雪崩 Single-Flight 防御 |
| [第14章：Sentinel高并发韧性](./chapter14-sentinel-resilience.md) | 容灾防卫 | 2天 | 网关前置流控与 429 统一返回、日历价库热点参数防护 (hotelId)、上游供应慢调用/异常熔断与 Nacos 持久化 |
| [第15章：Seata分布式事务](./chapter15-seata-distributed-transaction.md) | 一致性进阶 | 2天 | Seata 2.6.x AT 模式、跨微服务 XID 透传、两阶段提交（2PC）、`undo_log` 反向镜像补偿与回滚演练 |
| [第16章：业务优化与三阶段交易闭环专题](./chapter16-business-optimization-transaction-closure.md) | 核心业务闭环 | 3天 | 静态来源归一、房型显式映射、共享配额池、INSTANT/ASYNC履约模型、权威取消事实结构化与可靠通知游标机制 |

## 服务端口统一约定

平台服务沿用 `808x`，商家与渠道按业务视角错开（端口单一来源为 `ServicePortConstants`）：

| 服务名 | 端口 | 视角 | 外部路径（走 Gateway） |
|--------|------|------|------------------------|
| `ota-gateway` | **8080** | 网关入口 | `/` |
| `ota-auth` | **8081** | 平台认证 | `/api/auth/**` |
| `ota-user` | **8082** | 用户中心 | `/api/user/**` |
| `ota-hotel` | **8083** | 酒店标准数据 | `/api/hotel/**` |
| `ota-inventory` | **8084** | 价库与库存 | `/api/inventory/**` |
| `ota-order` | **8085** | 订单中心 | `/api/order/**` |
| `ota-merchant` | *(8086)* | *(已退役归档)* | *代码保留参考，不再运行* |
| `ota-admin` | **8087** | 平台运营中台 | `/api/admin/**` |
| `ota-ai` | **8088** | AI 辅助服务 | `/api/ai/**` |
| `ota-supplier` | **8089** | 上游供应适配 | `/api/supplier/**` |
| `ota-channel` | **8090** | 下游渠道分发 | `/api/channel/**` |

## 快速启动

```bash
# 说明：本教程目录为分章教学知识库，完整工程代码与基础设施配置统一位于 ota-cloud 根目录。

# 1. 在 ota-cloud 工程根目录下启动基础设施（Nacos + MySQL + Redis + Kafka）
docker compose up -d nacos mysql redis kafka

# 2. 在最终 ota-cloud 工程根目录依次启动核心业务服务
mvn spring-boot:run -pl ota-auth       # 8081
mvn spring-boot:run -pl ota-user       # 8082
mvn spring-boot:run -pl ota-hotel      # 8083
mvn spring-boot:run -pl ota-inventory  # 8084
mvn spring-boot:run -pl ota-order      # 8085
mvn spring-boot:run -pl ota-admin      # 8087
mvn spring-boot:run -pl ota-supplier   # 8089 (上游供应)
mvn spring-boot:run -pl ota-channel    # 8090 (下游渠道)
mvn spring-boot:run -pl ota-gateway    # 8080 (统一入口，建议在业务服务就绪后启动)

# 3. 启动进阶可观测性与辅助设施（按需）
docker compose up -d zipkin prometheus grafana alertmanager elasticsearch logstash kibana
mvn spring-boot:run -pl ota-ai         # 8088 (AI 辅助服务)

# 4. 启动作品演示静态前端（独立进程）
cd ota-portfolio-demo
npm install && npm run dev             # 访问 http://localhost:5173/
```

## 演示路径

```
【上游供应入库与静态归一】
集团/第三方快照同步(ota-supplier) → 多维模糊匹配与人工纠错(ota-admin) → 确认发布至标准化酒店(ota-hotel) & 共享配额价库(ota-inventory)

【C 端交易主链路】
注册 → 登录(拿JWT) → 查酒店详情(多级缓存) → 查价库(动态渠道加价) → 创建订单(预占库存) → 支付 → 本地写Outbox

【可靠交易履约与异步确认】
意图先落库屏障(submitStarted) → 即时/异步供应确认(INSTANT/ASYNC) → 履约收件箱收敛(Inbox) → 游标防回退通知分发(ota-channel)

【运营与全量监控对账】
平台三方订单与履约状态对账(ota-admin) ↔ 自动化监控告警(Prometheus+Grafana) ↔ Zipkin 分布式全链路追踪
```

---

## 全景技术栈架构指南矩阵

针对项目中落地的关键核心技术栈，均提供了生产级的全景架构设计、微服务拓扑、核心参数矩阵、落地源码与运维排障指南：
