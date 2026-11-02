# ota-cloud 现代微服务平台演进任务看板

> 对齐依据：[ota-cloud-需求文档.md](./ota-cloud-需求文档.md) v1.2 与 [业务优化及三阶段交易闭环实施演进](./chapter16-business-optimization-transaction-closure.md)  
> 当前状态说明：未开始项保持 `[ ]`，完成后手动改为 `[x]`。

## P0：核心交易闭环，约 7 天

- `[x]` P0-1 父 POM 与基础骨架
  - `[x]` 创建 `ota-cloud` 聚合模块并接入根 `pom.xml`
  - `[x]` 配置 JDK 21、Spring Boot 3.5.14、Spring Cloud 2025.0.x、Spring Cloud Alibaba 2025.0.x
  - `[x]` 创建 `ota-common`，沉淀统一响应、异常、DTO、工具类
  - `[x]` 准备 Nacos、MySQL、Redis 的 P0 Docker Compose 配置
- `[x]` P0-2 认证、用户与网关
  - `[x]` `ota-auth`：登录、登出、刷新 Token、`/auth/validate`
  - `[x]` `ota-user`：注册、用户信息、资料更新
  - `[x]` `ota-gateway`：路由、白名单、JWT 认证过滤、用户上下文透传
  - `[x]` Redis Token 黑名单，TTL 与 JWT 剩余有效期一致
- `[x]` P0-3 酒店查询、供应同步与缓存
  - `[x]` `ota-hotel`：酒店列表、详情、多条件搜索、房型列表
  - `[x]` `ota-supplier`：mock/华住风格酒店静态同步接口
  - `[x]` 保存 `supplierCode + supplierHotelId` 与平台 `hotelId` 映射
  - `[x]` 酒店详情 Cache-Aside、空值缓存、随机 TTL
  - `[x]` 热点 hotelId 互斥锁或逻辑过期方案
- `[x]` P0-4 价库库存核心能力
  - `[x]` `inventory_price_stock` 价库合一表
  - `[x]` 增加 `supplierCode`、`supplierHotelId`、`supplierRoomType`、`ratePlanCode`
  - `[x]` `inventory_record` 本地 hold 和供应履约流水表
  - `[x]` `ota-supplier`：mock/华住风格价库同步接口
  - `[x]` `/inventory/price/query` 房价库存查询
  - `[x]` `/inventory/precheck`、`/inventory/hold`、`/inventory/release` 幂等接口
  - `[x]` 本地 hold 只防重复提交，不作为最终库存依据
- `[x]` P0-5 订单核心链路
  - `[x]` `/order/create` 创建平台订单，调用供应侧试单/验价和集团下单
  - `[x]` 保存 `supplierCode`、`supplierOrderNo`、可选 `channelCode`、`channelOrderNo`
  - `[x]` `/order/pay` 支付成功后更新平台支付状态并写 outbox
  - `[x]` `/order/cancel` 取消或超时调用供应侧取消集团订单
  - `[x]` `/order/{id}`、`/order/list` 查询接口
  - `[x]` 订单状态机：INIT、PRECHECKED、SUPPLIER_BOOKING、WAIT_PAY、PAID、CANCELLED、CLOSED、CONFIRMED、CHECKED_IN、COMPLETED
- `[x]` P0-6 补偿、文档与演示
  - `[x]` WAIT_PAY 超时取消集团订单并释放本地 hold
  - `[x]` 集团下单成功但本地更新失败时按 supplierOrderNo 补偿查询
  - `[x]` 集团取消失败进入 CANCEL_RETRY 并重试
  - `[x]` 初始化 SQL、演示数据、curl/Postman 脚本
  - `[x]` README、API 演示文档、架构说明
  - `[x]` P0 主链路联调：登录 → 供应同步 → 酒店查询 → 价库查询 → 集团下单 → 支付/取消

## P1：治理与可靠性增强，约 3 天

- `[x]` P1-1 多集团供应适配 + Kafka Outbox
  - `[x]` `ota-supplier`：`HotelContentPort`、`RateInventoryPort`、`BookingPort`
  - `[x]` `adapter.huazhu`、`adapter.dongcheng`、`adapter.yeste`、`adapter.hyrj`、`adapter.jinjiang` 适配模板
  - `[x]` `SupplierCode` 路由、集团接口 request/response 日志、错误码映射
  - `[x]` `order_outbox` 表和 Outbox 扫描任务
  - `[x]` Topic：`supplier-booked`、`order-paid`、`order-cancelled`、`inventory-changed`、`cache-delete-retry`
  - `[x]` Producer 失败重试、Consumer 幂等、死信 topic
  - `[x]` 价库缓存精确删除和删除失败重试
- `[x]` P1-2 自有分销 API、平台渠道适配与可观测性
  - `[x]` `ota-distribution-api`：对应 `hoa-*` 标准酒店/价库/订单 API
  - `[x]` `ota-channel`：适配下游渠道查价、下单、取消、回调报文转换
  - `[x]` 渠道订单号与平台订单号、集团订单号映射
  - `[x]` Micrometer Tracing + Zipkin 3.x
  - `[x]` Gateway → Channel/Distribution → Order → Supplier TraceId 透传
  - `[x]` Sentinel Gateway 热点接口限流和熔断降级
  - `[x]` SpringDoc OpenAPI 聚合文档
- `[x]` P1-3 Seata AT 演示
  - `[x]` Seata 2.6.0 服务端和 undo_log
  - `[x]` 订单库 + 库存库跨库回滚演示
  - `[x]` 明确 Seata 仅用于 P1 演示，不侵入 P0 主路径

## P2：技术亮点扩展，约 3 天

- `[x]` P2-1 AI 推荐与问答
  - `[x]` `ota-ai` 独立服务，端口 8088
  - `[x]` `/ai/recommend`、`/ai/assistant`、`/ai/summary`
  - `[x]` Spring AI 1.1.7 + DeepSeek/OpenAI API
  - `[x]` 未配置 API Key 时不影响 P0 主链路
- `[x]` P2-2 ShardingSphere 分库分表演示
  - `[x]` 酒店按 city_id 分库方案
  - `[x]` 订单按 order_id 取模分表
  - `[x]` 库存按 hotel_id 分表
  - `[x]` 非分片键查询、分页排序、扩容迁移口径
- `[x]` P2-3 监控、日志与 CI
  - `[x]` Prometheus/Grafana 基础指标大盘（4 大仪表盘）
  - `[x]` ELK 日志采集与 traceId/orderId 检索
  - `[x]` Alertmanager 8 条核心告警规则
  - `[x]` GitHub Actions 编译检查
- `[ ]` P2-4 GraalVM Native Image
  - `[ ]` 单服务 native-image 构建
  - `[ ]` 输出启动耗时、内存占用、镜像体积对比
  - `[ ]` 记录反射、动态代理、资源文件限制

## 生产级高可用扩展（Chapter 10 ~ 15）

- `[x]` 商家中心服务归档与主体纠偏（Chapter 10）
  - `[x]` 核心主体明确定位为平台、供应商、渠道；`ota-merchant:8086` 代码归档退役，移除运行调度
- `[x]` 下游渠道分发闭环（Chapter 11）
  - `[x]` `ota-channel:8090` 加价规则引擎与 Kafka 事件消费闭环
- `[x]` 多级缓存与高可用治理（Chapter 13）
  - `[x]` Caffeine L1 + Redis L2 多级缓存骨架
  - `[x]` 订单读路径缓存与库存 Hot Key 自动预热
  - `[x]` 缓存穿透防护（空值哨兵）与缓存雪崩防御（Single-Flight 互斥锁）
- `[x]` 分布式高并发韧性防护（Chapter 14）
  - `[x]` Gateway 前置限流拦截与 429 统一 JSON 响应
  - `[x]` 库存服务日历价库热点参数流控 (hotelId) 与下单并发写流控
  - `[x]` 上游供应商慢调用比例与异常比例熔断降级
  - `[x]` Nacos 动态规则源持久化监听
- `[x]` Seata AT 分布式事务实战演练（Chapter 15）
  - `[x]` 跨微服务 XID 链路透传（Feign RequestInterceptor + Spring WebMvc HandlerInterceptor）
  - `[x]` `ota-order` TM 事务边界编排与独立演示端点 `POST /order/seata/create-order`
  - `[x]` 两阶段提交与异常回滚演练（`undo_log` 反向镜像恢复）
  - `[x]` 单测与集成测试通过，提供冒烟脚本与教学文档收口

## 平台供应商渠道业务优化与三阶段交易闭环（Chapter 16 架构重塑）

- `[x]` 静态来源归一与房型映射
  - `[x]` 新增供应商档案 `partner`，区分 GROUP / THIRD_PARTY，支持多来源快照保存
  - `[x]` 多维模糊匹配算法（名称二元组 + 地址门牌 + 经纬度距离计算），集团静态合并优先
  - `[x]` 来源酒店审核/纠错与物理房型显式映射（`init-sql/005` ~ `007`）
- `[x]` 多来源报价与共享配额池
  - `[x]` 独立价格计划（`inventory_rate_plan`）与多计划共享配额池（`inventory_pool_daily`）
  - `[x]` 渠道计划授权与加价控制（`channel_rate_access`），生成不可篡改报价凭证（Quote Token）
- `[x]` 即时与异步履约确认模型
  - `[x]` 报价快照绑定确认模式（INSTANT / ASYNC）与最长确认时限
  - `[x]` 履约 begin 意图先落库屏障：配额受保护不超时释放，UNKNOWN 绝不盲目重复下单
- `[x]` 三阶段收尾与权威取消事实结构化闭环
  - `[x]` 文本中立性保证：全面移除 `reason.contains(...)` 状态推导，reason 仅作展示
  - `[x]` `BookingFlowContracts.SupplierResult` 结构化字段落地（`cancelRequestNo`、`priorState`、`priorEventId`）
  - `[x]` 订单主表权威取消事实（`cancel_fact`）与冲突证据（`cancel_conflict`）结构化持久化
  - `[x]` 取消冲突只记录证据并推向 `REVIEW_REQUIRED`，绝不调用 `cancelled` 接口或触发退款；提供 `verifyMissingCancelFact` 主动补偿查证
  - `[x]` 渠道通知版本游标（Highest Version Cursor）防回退，旧版本自动收敛为 `SUPERSEDED`，死信重放强制审计
  - `[x]` 全 11 个运行模块分批测试（373 项测试 100% 通过），完成 Google Java Format 代码规范治理

## 验收清单

- `[x]` P0 只依赖 Nacos、MySQL、Redis 和 mock/华住风格供应适配器即可跑通主链路
- `[x]` Gateway 是唯一外部入口，业务服务只信任网关或可信内网 Header
- `[x]` 集团试单、下单、取消、查询均按 platformOrderNo + supplierCode 幂等
- `[x]` Kafka 使用 Outbox 保证最终一致性，不描述为 Seata AT 事务的一部分
- `[x]` AI、ShardingSphere、Prometheus/Grafana、ELK、GraalVM 均不阻塞 P0
- `[x]` 性能指标以 JMeter/Gatling/wrk 压测报告为准，不写无依据固定承诺
- `[x]` 权威取消事实与冲突证据解耦，拒绝文本解析推断状态，保证配额与资金绝对安全
