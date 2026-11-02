# Chapter 11: 下游渠道分发 (Outbound Channel Distribution)

**Date**: 2026-06-10
**Status**: Planning
**Owners**: ota-cloud

## 背景

Chapter 9 把"上游供应"（华住 crs-api / b2b-api 适配）做完了，但 OTA 平台模型是"两头上"：

- **上游（inbound）**：从供应商（华住、东呈、锦江）拉酒店/价库/下单 → `ota-supplier`（Chapter 9）
- **下游（outbound）**：把本地酒店/价库/订单推给分销渠道（携程、美团、飞猪、逸扉） → `ota-channel`（本章）

Admin phase 1 已经在 `ota-admin` 建好了 `downstream_channel` / `channel_data_range` / `hotel_source_mapping` 三张配置表，**但没有消费方**：渠道规则没有在任何业务路径里生效，加价系数 `coe` 只能 preview，平台与渠道之间没有真实的"推单 / 查单 / 取消"链路。

Chapter 11 闭合这个模型：

1. 新建 `ota-channel` 服务，定义 `ChannelAdapter` 端口 + MOCK 实现；
2. 在 `ota-inventory` 价库查询时按 `channelCode` 应用加价规则（复用 `ChannelDataRangeService.previewEffectiveCoe` 的累加逻辑）；
3. 推单 / 查单 / 取消三件套 + Kafka 事件总线（订单状态变更 → 渠道回执 → 反向更新）；
4. `ota-admin` 新增"渠道推单追踪"页。

## Phase 0：现状与可复用资产（调研）

**目标**：把后续 Phase 要复用的现有代码、DDL、Filter 模式定位清楚。

**输出**：本计划文档（本文）。

**主要参考资料**：

| 复用对象 | 路径 | 用途 |
| --- | --- | --- |
| 渠道配置表 | [DownstreamChannel.java](file:///Users/fuxinwei/work/ota-cloud/ota-admin/src/main/java/com/ota/cloud/admin/entity/DownstreamChannel.java) | 渠道主数据；字段已含 channelCode/secret/shareRate |
| 渠道可售范围 | [ChannelDataRange.java](file:///Users/fuxinwei/work/ota-cloud/ota-admin/src/main/java/com/ota/cloud/admin/entity/ChannelDataRange.java) | 6 类 rule_type + coe 累加 |
| 酒店来源映射 | [HotelSourceMapping.java](file:///Users/fuxinwei/work/ota-cloud/ota-admin/src/main/java/com/ota/cloud/admin/entity/HotelSourceMapping.java) | 跨集团/集团内多渠道酒店 ID 映射 |
| 加价预览服务 | [ChannelDataRangeService.java](file:///Users/fuxinwei/work/ota-cloud/ota-admin/src/main/java/com/ota/cloud/admin/service/ChannelDataRangeService.java) `previewEffectiveCoe` | 6 维累加实现，本章要 Feign 暴露给 `ota-inventory` |
| 上游端口模式 | [SupplierAdapter.java](file:///Users/fuxinwei/work/ota-cloud/ota-supplier/src/main/java/com/ota/cloud/supplier/port/SupplierAdapter.java) | 端口 + 路由器 + 适配器同构 |
| 上游路由器 | [SupplierRouter.java](file:///Users/fuxinwei/work/ota-cloud/ota-supplier/src/main/java/com/ota/cloud/supplier/router/SupplierRouter.java) | 按 supplierCode 选取具体 Adapter，未匹配回退默认 |
| MOCK 适配器 | [MockSupplierAdapter.java](file:///Users/fuxinwei/work/ota-cloud/ota-supplier/src/main/java/com/ota/cloud/supplier/adapter/mock/MockSupplierAdapter.java) | 内存数据 + requestNo 幂等 + 注入式失败 |
| 价库 / 订单事件 | [ota-order](file:///Users/fuxinwei/work/ota-cloud/ota-order) Kafka outbox | `order-paid` / `order-cancelled` 事件，本章渠道回执要消费 |
| 老系统推单 | [/Users/fuxinwei/work/otaapi/dongcheng/dc-important](file:///Users/fuxinwei/work/otaapi/dongcheng) | 推单 API + 回调任务 + 状态映射 |
| 老系统下游渠道路由 | [/Users/fuxinwei/work/otaapi/yeste/yst-important](file:///Users/fuxinwei/work/otaapi/yeste) | 独立 yst-important 模块独立部署 |
| 老系统下游回调 | [/Users/fuxinwei/work/otaapi/jinjiang/jj-important](file:///Users/fuxinwei/work/otaapi/jinjiang) | 推单 + 回调 + 状态轮询 |
| 老系统 DDL | [/Users/fuxinwei/work/db/hoa_channel.sql](file:///Users/fuxinwei/work/db/hoa_channel.sql) | `hoa_channel` / `hoa_channel_data_range` 字段来源 |

**反模式（不要做）**：

- 不把 `downstream_channel.secret` 字段直接用于真实签名（P0 不接真渠道，避免泄漏密钥）。
- 不复用 `ota-supplier` 的 `BookingPort`，那是上游（inbound）能力；下游要重新定义 `ChannelBookingPort`（语义不同：上游是"供应商给本平台"，下游是"本平台推给分销渠道"）。
- 不让 `ota-channel` 直接调 `ota-inventory` 数据库读 `inventory_price_stock`；加价一定要走 ota-admin 的 `previewEffectiveCoe` 同一套规则，避免规则双写。
- 不在 `ota-hotel` 内做"渠道推送"——酒店是内容域，渠道是分发域，关注点不同。
- 不动 `ota-order` 的订单状态机；`ota-channel` 通过 Kafka 事件订阅 `order-paid` / `order-cancelled`，**只读** + 触发渠道动作。

**完成判定**：

- 本计划文档被用户在 `cloud-tutorial-progress.md` 引用。
- `legacy-otaapi-reuse.md` 增补"第十一章下游渠道分发"小节。

## Phase 1：`ota-channel` 服务骨架 + 端口 + MOCK 适配器

**目标**：新建 `ota-channel` 服务，跑通"按 channelCode 选适配器 + 推单/查单/取消 3 件套"主链路。

**新建**：

- `ota-channel/pom.xml`：参考 `ota-supplier/pom.xml` 依赖（Spring Boot 3 + Nacos + MyBatis-Plus + Sentinel + MySQL + Redis）。
- `ChannelApplication.java`、bootstrap.yml、application.yml（Nacos dataId=`ota-channel.yml`）。
- **端口定义**（包 `com.ota.cloud.channel.port`）：
  - `ChannelAdapter`：`channelCode()` + `channelBooking()` + `channelEnabled()`，参考 `SupplierAdapter` 同构。
  - `ChannelBookingPort`：`pushOrder` / `queryOrder` / `cancelOrder`，对应渠道的推单/查单/取消三件套。
- **域模型**（包 `com.ota.cloud.channel.domain`）：
  - `ChannelCode` 枚举：`MOCK` / `CTRIP` / `MEITUAN` / `FLIGGY` / `YESTE`（P0 只实现 MOCK + YESTE）。
  - `ChannelOrderRequest` / `ChannelOrderResult` / `ChannelCancelResult` / `ChannelOrderStatus`。
  - 字段口径继承老系统 `/Users/fuxinwei/work/otaapi/dongcheng/dc-important/bean/request` 的 `DcAddOrderQQ` / `DcCancelOrderQQ`。
- **适配器**：
  - `MockChannelAdapter`：内存 ConcurrentHashMap 模拟推单/查单/取消；requestNo 幂等；延迟可配。
  - `YesteChannelAdapter`（P0 占位）：构造 YESTE 渠道请求 DTO，预留 HTTP 调用入口但 `enabled=false` 时不真连（参考 `HzSupplierAdapter` 模式）。
- **路由器**：
  - `ChannelRouter`：按 `channelCode` 精确匹配 → 缺省回退 MOCK → 缺省未启用抛错（参考 `SupplierRouter` 同构）。
- **自动配置**：
  - `ChannelAutoConfiguration`：用 `@ConditionalOnProperty` 决定 YESTE 是否注册到容器；Nacos 改 `channel.yeste.enabled=true` 才真连。

**Controller**：

- `ChannelInternalController`：`/channel/internal/push` / `/channel/internal/query/{channelOrderNo}` / `/channel/internal/cancel/{channelOrderNo}`，**不挂 Gateway**，仅 ota-order/ota-admin Feign 调用。
- `ChannelHealthController`：`GET /api/channel/health`，白名单放行。

**Gateway 改造**：

- [application.yml](file:///Users/fuxinwei/work/ota-cloud/ota-gateway/src/main/resources/application.yml) 加 `/api/channel/**` 路由指向 `lb://ota-channel`，白名单加 `/api/channel/health`。

**端口约定**：`ota-channel:8090`（按 [AGENTS.md](file:///Users/fuxinwei/work/ota-cloud/AGENTS.md) 服务端口约定，与 ota-supplier 错开）。

**完成判定**：

- `mvn -pl ota-channel -am package` 编译通过。
- `ChannelRouterTest` 覆盖：精确匹配 / 缺省回退 / 未匹配抛错。
- `MockChannelAdapterTest` 覆盖：requestNo 幂等 / push→query→cancel 全链路。
- 真实环境 smoke：`scripts/chapter11-channel-smoke.sh` 步骤 1（直连 ota-channel 健康检查 + Gateway 白名单健康检查）。

## Phase 2：加价引擎在价库查询时生效

**目标**：让 `channel_data_range` 规则在价库查询时真正生效，下游渠道看到的是"加价后"的价格，而不是原始 sellPrice。

**问题定位**：

- 当前 `InventoryService.queryPriceStock`（`ota-inventory`）只返回 `inventory_price_stock.sell_price`，不消费任何 `channelCode`；
- `ChannelDataRangeService.previewEffectiveCoe` 在 `ota-admin` 只做 preview，不参与真实计算。

**实现**：

- `ota-admin` 暴露 Feign 端点 `GET /admin/internal/pricing/preview-effective-coe?channelCode=&partnerCode=&brandCode=&hotelId=&saleCode=`，返回 `EffectiveCoeVO`。
- `ota-inventory` 新增 `InventoryChannelPricingClient`（Feign）调上述端点，**加缓存**：
  - 缓存 key：`channel-pricing:{channelCode}:{hotelId}:{saleCode}`，TTL 5 分钟（与 `RedisConfig` 风格一致）；
  - 缓存值：`EffectiveCoeVO` JSON；
  - 缓存击穿：用 `synchronized` 锁 + 单飞。
- `InventoryService.queryPriceStock` 改造：增加可选 `channelCode` 入参（默认 null 不加价）；有 channelCode 时：
  1. 先按 `hotelId + saleCode` 查酒店（拿 partnerCode / brandCode）；
  2. 调 `InventoryChannelPricingClient.previewEffectiveCoe` 拿 coe；
  3. 返回 `sellPrice * (1 + coe)`。

**字段来源**：

- `partnerCode` / `brandCode` 字段来源：`ota-hotel` 酒店的 `partnerCode` / `brandCode` 字段（已存在）。
- `saleCode` 字段来源：`inventory_price_stock.sale_code`（已存在）。
- `coe` 计算口径沿用 `ChannelDataRangeService.previewEffectiveCoe`（已存在），本章不重写累加规则。

**反模式**：

- 不在 `ota-inventory` 内复刻 coe 累加逻辑；规则只在 `ota-admin` 一处实现，避免双写。
- 不让 `ota-channel` 直接调 `ChannelDataRangeService`（跨服务调用必须走 Feign）。

**完成判定**：

- `ChannelPricingClientTest` 覆盖：缓存命中 / 缓存击穿 / 渠道未配置时 coe=0 / 加价正确。
- `InventoryServiceTest` 新增 case：传入 channelCode 触发加价。
- smoke 步骤 2：调 `/api/inventory/price-stock?channelCode=MOCK&hotelId=1&saleCode=BAR` 返回 `sellPrice * 1.05`（5% 加价）。

**Phase 2 完成时间**：2026-06-10。已实施内容：

- `ota-admin.AdminInternalController`：新增 `/admin/internal/pricing/preview-effective-coe`（不挂 Gateway，Feign 专用）。
- `ota-inventory.AdminPricingClient` + `ota-inventory.HotelClient` + `ota-inventory.HotelBaseVO` + `ota-inventory.EffectiveCoeVO`：两个 Feign 客户端与两个最小 VO 副本。
- `ota-inventory.ChannelPricingService`：5 维 coe 累加 + Redis 5 min 缓存（NULL 哨兵 30 s 防击穿）+ `applyChannelCoe`（4 舍 5 入 2 位）+ `invalidate` 失效方法；Feign 失败降级不加价。
- `ota-inventory.InventoryService`：`queryPriceAndStock` 增加 `channelCode` 与 `saleCode` 可选入参，多日期查询共享一次 coe。
- `ota-inventory.InventoryPriceVO`：新增 `channelCode` / `originalSellPrice` / `effectiveCoe` 三个字段。
- `ota-inventory.InventoryController`：`/inventory/price/query` 新增 `channelCode` + `saleCode` 可选入参。
- `ota-inventory` 启动类开启 `@EnableFeignClients` + `pom.xml` 引入 `spring-cloud-starter-openfeign` 与 `spring-cloud-starter-loadbalancer`。
- 测试：`ChannelPricingServiceTest` 10 个场景（空入参/缓存命中/Feign 拉取/2 位 4 舍 5 入/hotel-admin 双 Feign 降级/invalidate）；`InventoryServiceTest` 兼容 4 参构造并新增 3 个加价 case。
- smoke：`scripts/chapter11-phase2-pricing-smoke.sh` 3 步（不加价路径 / 加价路径 / admin 内部端点直连）。

## Phase 3：推单 / 查单 / 取消三件套

**目标**：把"订单在本地创建后推给下游渠道"做成完整闭环：push → query → cancel 三件套 + 失败重试。

**实现**：

- `ChannelOrderRequest` 字段（参考老系统 DcAddOrderQQ / YstAddOrderQQ）：
  - `requestNo`（幂等键，=本地 `orders.orderId`）；
  - `hotelId` / `roomTypeCode` / `arrivalDate` / `departureDate` / `roomCount`；
  - `guestName` / `guestMobile` / `idCard`；
  - `salePrice`（已加价后的最终价，本地价库 + ChannelDataRange 累加）；
  - `channelCode`（MOCK / YESTE）。
- `ota-order` 改造：
  - 现有订单表新增 `channel_code` / `channel_order_no` / `channel_status` 3 个字段（参考 `supplier_*` 字段同构；DDL 见 `init-sql/004-order-channel.sql`）。
  - `OrderService.createOrder` 完成后，**同步** 调 `ChannelOrderService.push(orderId)` 推单（用 `@Async` 包装，失败不阻塞下单主链路）。
  - `OrderService.cancel` 完成后，**同步** 调 `ChannelOrderService.cancel(orderId)`。
  - 启动时启动 `ChannelOrderQueryJob`（参考 `SupplierCompensationJob`）：每 60s 扫描 `channel_status in (PUSHING, PUSHED)` 但本地已支付/已取消的订单，对账一次。
- `ota-channel` 推单：
  - `ChannelBookingPort.pushOrder(request) → ChannelOrderResult`，返回 `channelOrderNo` / `success` / `message`；
  - 失败时 `ChannelRouter` 退回 MOCK 重试 1 次（避免渠道瞬时故障导致订单卡死）；
  - 失败累计到 `[channel-retry=N]` 写进 `error_msg`，与 `SupplierCompensationJob` 同构。
- `ota-channel` 查单 / 取消：同步 HTTP 调用，失败抛 `BusinessException(REMOTE_ERROR)`，由调用方捕获降级。

**紧急止血开关**：

- `order.channel-push-job.enabled=false`（参考 `order.supplier-compensation-job.enabled`）关闭查询任务；
- `channel.yeste.enabled=false`（参考 `supplier.huazhu.b2b.enabled`）关闭 YESTE 真实链路。

**完成判定**：

- `MockChannelAdapterTest` 扩展：push→query→cancel 全链路；requestNo 幂等。
- `ChannelOrderServiceTest` 覆盖：同步 push / 失败重试 / 取消 / 错误累计。
- smoke 步骤 3：创建订单 → 触发 push → 验证 `channel_order_no` 写入 → 取消 → 验证渠道侧 cancel 调用。

## Phase 4：Kafka 事件总线（订单状态变更 → 渠道回执） ✅ 已完成

**目标**：本地订单状态变更通过 Kafka 事件驱动 `ota-channel` 动作，**避免同步调用**。本阶段把 Phase 3 的同步调用改成事件驱动。

**实现**：

- `ota-order` 现有 outbox 投递 `order-paid` / `order-cancelled` 事件（Chapter 6 已有），不改动；
- `ota-channel` 订阅：
  - [OrderPaidEventConsumer](file:///Users/fuxinwei/work/ota-cloud/ota-channel/src/main/java/com/ota/cloud/channel/consumer/OrderPaidEventConsumer.java)：收到 `order-paid` 事件 → 调 `ChannelOrderService.confirmByOrderId(orderId)`（如果渠道支持 confirm 流程，否则记录日志）；
  - [OrderCancelledEventConsumer](file:///Users/fuxinwei/work/ota-cloud/ota-channel/src/main/java/com/ota/cloud/channel/consumer/OrderCancelledEventConsumer.java)：收到 `order-cancelled` 事件 → 调 `ChannelOrderService.cancelByOrderId(orderId)`；
- [ChannelOrderService](file:///Users/fuxinwei/work/ota-cloud/ota-channel/src/main/java/com/ota/cloud/channel/service/ChannelOrderService.java) 编排：先通过 [OrderInternalFeignClient](file:///Users/fuxinwei/work/ota-cloud/ota-channel/src/main/java/com/ota/cloud/channel/feign/OrderInternalFeignClient.java) 拉订单的 `channelCode` / `channelOrderNo`，再走 Router 选 Adapter 执行；cancel 在缺 `channelOrderNo` 时用 `MockChannelAdapter.cancelByRequestNo(requestNo)` 兜底。
- MockChannelAdapter 新增 [cancelByRequestNo](file:///Users/fuxinwei/work/ota-cloud/ota-channel/src/main/java/com/ota/cloud/channel/adapter/mock/MockChannelAdapter.java#L168-L198)（Phase 4 新增，与 `cancelOrder` 双轨），事件取消用此方法。
- 消费配置手动 ack（参考 Chapter 6）；失败 ACK + warn 日志，不阻塞后续事件。
- 开关 `channel.events.order-paid-enabled` / `channel.events.order-cancelled-enabled`（Nacos 可动态切换），Phase 3 的同步调用保留为"快速通道"（避免支付后用户等事件投递延迟），事件作为兜底与对账。

**反模式**：

- 不让 `ota-channel` 订阅 `order-paid` 后**重复**调 `pushOrder`（已 push 不能再 push）；改用 `confirmOrder` 渠道语义。
- 不在 `ota-channel` 监听 `ota-order` 数据库 binlog；走 outbox 事件保持架构一致性。

**完成判定**：

- `OrderPaidEventConsumerTest` 覆盖：事件解析 / confirm 调用 / 失败入 DLQ。
- `OrderCancelledEventConsumerTest` 覆盖：cancel 调用 / 幂等。
- smoke 步骤 4：本地订单 PAID → 等待 1s → 验证 `channel_status` 进入 `CONFIRMED`（mock）。

## Phase 5：admin 视角"渠道推单追踪"页 ✅ 已完成

**目标**：平台 admin 可以看到每条本地订单的渠道推单轨迹：推了哪些渠道、远端状态、是否对账成功。

**实现**：

- [OrderSupplierReconcileDTO](file:///Users/fuxinwei/work/ota-cloud/ota-admin/src/main/java/com/ota/cloud/admin/feign/dto/OrderSupplierReconcileDTO.java)（admin 侧）：扩展 4 个 channel 字段；
- [ChannelOrderStatusDTO](file:///Users/fuxinwei/work/ota-cloud/ota-admin/src/main/java/com/ota/cloud/admin/feign/dto/ChannelOrderStatusDTO.java)：定义 admin → ota-channel 反查的渠道状态 DTO；
- [ChannelReconcileFeignClient](file:///Users/fuxinwei/work/ota-cloud/ota-admin/src/main/java/com/ota/cloud/admin/feign/ChannelReconcileFeignClient.java)：调 `/channel/internal/query/{channelOrderNo}`；
- [OrderReconcileFeignClient](file:///Users/fuxinwei/work/ota-cloud/ota-admin/src/main/java/com/ota/cloud/admin/feign/OrderReconcileFeignClient.java)：新增 `channelPage` 方法；
- [OrderInternalController](file:///Users/fuxinwei/work/ota-cloud/ota-order/src/main/java/com/ota/cloud/order/controller/OrderInternalController.java#L159-L181)：新增 `/order/internal/channel/page` 端点；
- [OrderMapper.selectChannelPage](file:///Users/fuxinwei/work/ota-cloud/ota-order/src/main/resources/mapper/OrderMapper.xml#L134-L150)：按 `channelCode / status / missingChannelOrderNo` 过滤；
- [ChannelReconcileService](file:///Users/fuxinwei/work/ota-cloud/ota-admin/src/main/java/com/ota/cloud/admin/service/ChannelReconcileService.java)：编排 `listOrders`（仅本地） / `reconcilePage`（调 ota-channel） / `reconcileOne`（详情）；
- [ChannelReconcileVO](file:///Users/fuxinwei/work/ota-cloud/ota-admin/src/main/java/com/ota/cloud/admin/vo/ChannelReconcileVO.java)：与 `SupplierReconcileVO` 同构，字段名对齐 channel 语义；
- [ChannelReconcileController](file:///Users/fuxinwei/work/ota-cloud/ota-admin/src/main/java/com/ota/cloud/admin/controller/ChannelReconcileController.java)：三个端点（orders / diff / {orderId}），路由 `/admin/channel-reconcile/**`；
- 新增 [ChannelReconcileServiceTest](file:///Users/fuxinwei/work/ota-cloud/ota-admin/src/test/java/com/ota/cloud/admin/service/ChannelReconcileServiceTest.java)（6 个场景）/ [ChannelReconcileControllerTest](file:///Users/fuxinwei/work/ota-cloud/ota-admin/src/test/java/com/ota/cloud/admin/controller/ChannelReconcileControllerTest.java)（3 个场景）；
- 新增 [chapter11-phase5-channel-reconcile-smoke.sh](file:///Users/fuxinwei/work/ota-cloud/scripts/chapter11-phase5-channel-reconcile-smoke.sh)。

**反模式**：不复用 `SupplierReconcileVO`；channel 对账是独立 `ChannelReconcileVO`，字段语义对齐 `channelOrderNo` vs `supplierOrderNo`。

**完成判定**：

- `ChannelTraceController` 单元测试：分页 / 详情 / merchantId 过滤（admin 视角全量）。
- smoke 步骤 5：admin token 调 `/api/admin/channel-trace/page?channelCode=MOCK` 返回 mock 推单记录。

## Phase 6：测试 + Smoke + 文档 ✅ 已完成

**目标**：所有新代码配套测试 + 真实环境 smoke + 文档收口。

**测试成果**：

- `ChannelRouterTest` / `MockChannelAdapterTest` / `YesteChannelAdapterTest`
- `ChannelOrderServiceTest` / `OrderPaidEventConsumerTest` / `OrderCancelledEventConsumerTest`
- `ChannelReconcileControllerTest` / `ChannelReconcileServiceTest`（admin 侧）
- `ChannelCompensationJobTest` / `SupplierCompensationJobTest`（ota-order 侧）

**统计**：80 个 JUnit 全部通过
- ota-channel：43（Router 8 + MockAdapter 13 + YesteAdapter 5 + ChannelOrderService 9 + OrderPaid 4 + OrderCancelled 4）
- ota-order：24（ChannelCompensationJob 6 + SupplierCompensationJob 5 + OrderService 5 + Outbox 3 + 其它 5）
- ota-admin：13（ChannelReconcileController 3 + ChannelReconcileService 6 + 其它 4）

**Smoke 脚本**：

- `scripts/chapter11-channel-smoke.sh`（Phase 1 健康 + defaultAdapter + Gateway 白名单）
- `scripts/chapter11-phase2-pricing-smoke.sh`（加价规则）
- `scripts/chapter11-phase3-channel-push-smoke.sh`（推单 + 取消 + 补偿）
- `scripts/chapter11-phase4-kafka-events-smoke.sh`（Kafka 事件总线）
- `scripts/chapter11-phase5-channel-reconcile-smoke.sh`（admin 渠道追踪页）
- `scripts/chapter11-smoke-all.sh`（一键串跑所有 Phase）

**架构修正**：

测试阶段发现 `ota-channel` 直接 import `ota-order` 的事件类与 DTO 违反微服务边界，重构为：
- `OrderPaidEvent` / `OrderCancelledEvent` → `com.ota.cloud.common.event.*`
- `OrderSupplierReconcileDTO` / `OrderPageDTO` / `SupplierOrderStatusDTO` → `com.ota.cloud.common.feign.dto.*`
- 统一在 `ota-common` 维护跨服务契约，`ota-order` / `ota-supplier` / `ota-merchant` / `ota-admin` / `ota-channel` 一致引用。

**文档**：

- `cloud-tutorial-progress.md` Chapter 11 行已标记 All 6 Phases Done
- `ota-channel/README.md`：API 表 + Nacos 配置 + 5 个 phase 脚本 + Phase 完成情况
- `legacy-otaapi-reuse.md`：增补"第十一章下游渠道分发"小节
- `AGENTS.md`：增补端口约定 + 加价规则只在 ota-admin 实现

**完成判定**：

- ✅ `mvn -pl ota-channel,ota-order,ota-admin -am test` 全绿（80/80）
- ✅ `chapter11-smoke-all.sh` 已编写（一键串跑 5 个 Phase）
- ✅ 文档 5 处全部就绪

## 风险与回退

| 风险 | 应对 |
| --- | --- |
| 现有 `inventory_price_stock` 写入未带 channelCode | Phase 2 加价只读不改，存量数据走 `coe=0`（不加价）兜底 |
| 多渠道同时推单触发幂等问题 | `requestNo = orderId`，下游必须按 orderId 幂等；YESTE 适配器预留 `Idempotency-Key` header |
| 渠道侧推单失败导致本地订单卡死 | 失败累计 `[channel-retry=N]` + 紧急止血 `order.channel-push-job.enabled=false` |
| YESTE 真实接入需要密钥 | `channel.yeste.enabled=false` 时不连，留到申请密钥后切换 |
| 加价缓存与配置变更不一致 | TTL 5 分钟；管理员改 `channel_data_range` 后等 5 分钟生效；后续可加"配置变更广播" |
| ota-channel 出现上游问题（被渠道限流） | Sentinel 限流 + 退避重试（参考 ota-supplier 限流模式） |

## 简历视角（Chapter 11 价值）

- **架构对称性**：能讲清 "OTA 平台 = 供应适配 + 渠道分发" 的六边形架构闭合；
- **复用老系统**：能讲清从 `hoa_channel_data_range` 5 类 rule_type 升级到 6 类，加价系数累加的工程实现；
- **可观测性**：Kafka 事件驱动 + Outbox 模式 + 死信 + 补偿任务，能讲清分布式事务与最终一致性；
- **可测试性**：端口模式 + 路由器 + MOCK 适配器，每个组件可独立 mock 测试。
