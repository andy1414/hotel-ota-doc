# Chapter 12：可观测性 / 监控告警

> Last updated: 2026-06-12
> Status: Done — 6 个 Phase 全部完成

## 阶段总览

| Phase | 名称 | 状态 | 关键交付 |
| --- | --- | --- | --- |
| 1 | Prometheus 端口对账 + 11 服务接入 | Done | `prometheus/prometheus.yml` 按 `AGENTS.md` 端口对齐；`Chapter07ConfigurationTest` 端口对账 case |
| 2 | 业务指标埋点 | Done | `BusinessMetricsConstants` + `AbstractBusinessMetrics` + 5 个 Metrics 子类（Order/Channel/Supplier/Merchant/Inventory）+ 单元测试 |
| 3 | Grafana 仪表盘 | Done | `grafana/dashboards/{infra-overview,order-funnel,channel-distribution,supplier-integration}.json` |
| 4 | 告警规则 + Alertmanager | Done | `prometheus/alerts.yml`（8 条规则）+ `prometheus/alertmanager.yml`（3 个 receiver + 抑制规则） |
| 5 | 集中式日志（Logback → Logstash → ES） | Done | `ota-common/logback-base.xml` 模板 + 11 个服务 `logback-spring.xml` + `logstash/pipeline/ota-log.conf` |
| 6 | Kafka traceId 透传 + 测试 + smoke + 文档 | Done | `OrderPaidEvent`/`OrderCancelledEvent` 增加 `traceId`；`OutboxService` 写入 headers；消费者回填 MDC；`scripts/chapter12-observability-smoke.sh` + `scripts/chapter12-smoke-all.sh` |

## 关键代码定位

- 业务指标常量：[BusinessMetricsConstants.java](file:///Users/fuxinwei/work/ota-cloud/ota-common/src/main/java/com/ota/cloud/common/metrics/BusinessMetricsConstants.java)
- 业务指标基类：[AbstractBusinessMetrics.java](file:///Users/fuxinwei/work/ota-cloud/ota-common/src/main/java/com/ota/cloud/common/metrics/AbstractBusinessMetrics.java)
- 日志模板：[logback-base.xml](file:///Users/fuxinwei/work/ota-cloud/ota-common/src/main/resources/logback-base.xml)
- Logstash pipeline：[ota-log.conf](file:///Users/fuxinwei/work/ota-cloud/logstash/pipeline/ota-log.conf)
- 告警规则：[alerts.yml](file:///Users/fuxinwei/work/ota-cloud/prometheus/alerts.yml)
- Alertmanager 配置：[alertmanager.yml](file:///Users/fuxinwei/work/ota-cloud/prometheus/alertmanager.yml)
- Kafka 事件 traceId 透传：[OrderPaidEvent.java](file:///Users/fuxinwei/work/ota-cloud/ota-common/src/main/java/com/ota/cloud/common/event/OrderPaidEvent.java) / [OrderCancelledEvent.java](file:///Users/fuxinwei/work/ota-cloud/ota-common/src/main/java/com/ota/cloud/common/event/OrderCancelledEvent.java)
- Outbox traceId 写入：[OutboxService.java](file:///Users/fuxinwei/work/ota-cloud/ota-order/src/main/java/com/ota/cloud/order/outbox/OutboxService.java)
- 消费者回填 MDC：[OrderPaidEventConsumer.java](file:///Users/fuxinwei/work/ota-cloud/ota-channel/src/main/java/com/ota/cloud/channel/consumer/OrderPaidEventConsumer.java) / [OrderCancelledEventConsumer.java](file:///Users/fuxinwei/work/ota-cloud/ota-channel/src/main/java/com/ota/cloud/channel/consumer/OrderCancelledEventConsumer.java)
- 端口对账测试：[Chapter07ConfigurationTest.java](file:///Users/fuxinwei/work/ota-cloud/ota-common/src/test/java/com/ota/cloud/common/Chapter07ConfigurationTest.java)

## 冒烟脚本

- [chapter12-observability-smoke.sh](file:///Users/fuxinwei/work/ota-cloud/scripts/chapter12-observability-smoke.sh) — 4 个观测面独立校验
- [chapter12-smoke-all.sh](file:///Users/fuxinwei/work/ota-cloud/scripts/chapter12-smoke-all.sh) — 5 步串跑（actuator/prometheus + 4 个观测面）

## 旧版本规划

以下为原始 6 个 Phase 详细规划，作为本次实施的过程记录保留。

## 背景与目标

OTA-Cloud 在 Chapter 7 已经把"基础设施"层面的可观测性铺开（Prometheus + Grafana + Zipkin + ELK），但经过 Chapter 8-11 一系列新模块（ota-supplier / ota-merchant / ota-channel）与新业务（Outbox 事件总线、多供应商、多渠道）落地后，存在以下缺口：

1. **端口错位 + 缺服务**：`prometheus.yml` 仍然按 Chapter 7 时期的 8 个服务配置端口，与当前 `AGENTS.md` 的端口表（`ota-hotel:8083` / `ota-inventory:8084` / `ota-order:8085` / `ota-admin:8087` / `ota-ai:8088` / `ota-supplier:8089` / `ota-merchant:8086` / `ota-channel:8090`）大量错位，且缺 ota-supplier / ota-merchant / ota-channel 三个新服务。
2. **零业务指标**：除 Spring Boot 内置的 HTTP / JVM 指标外，订单创建/支付/取消、供应商预订单/确认单/取消、渠道推单/确认/取消、商家登录、库存价库查询等关键业务事件没有任何自定义 Micrometer Counter / Timer，无法在 Grafana 中观察业务漏斗。
3. **无 Grafana 仪表盘**：`grafana/dashboards/` 是空目录，没有现成的可视化。
4. **无告警能力**：没有 Alertmanager，没有 Prometheus alerting rules，运营只能人肉盯。
5. **日志未集中化**：logstash pipeline 监听 tcp/5044 但 12 个服务都未配置 Logback appender，Kibana 实际是空的。
6. **Kafka 消息无 traceId 透传**：Chapter 4 Outbox、Chapter 9-11 的 `order-paid` / `order-cancelled` 主题跨进程时 traceId 不可见，难以在 Kibana 关联全链路。

本章按 6 个 Phase 闭合以上缺口，全程遵守 `AGENTS.md` 的端口与命名约定。

## Phase 1：Prometheus 端口对账 + 新服务接入

**目标**：`prometheus.yml` 按 `AGENTS.md` 的端口表修对，并补全 11 个服务的 scrape target。

**已知错误**（来自当前 [prometheus/prometheus.yml](file:///Users/fuxinwei/work/ota-cloud/prometheus/prometheus.yml)）：

| 服务 | 当前配置端口 | 实际端口（AGENTS.md） |
| --- | --- | --- |
| ota-hotel | 8084 | **8083** |
| ota-inventory | 8085 | **8084** |
| ota-order | 8084 | **8085** |
| ota-admin | 8086 | **8087** |
| ota-ai | 8087 | **8088** |
| ota-supplier | (缺) | **8089** |
| ota-merchant | (缺) | **8086** |
| ota-channel | (缺) | **8090** |

**新增任务**：

- 重写 `prometheus/prometheus.yml`，按 `AGENTS.md` 端口表对齐 11 个服务的 scrape target（含 ota-supplier / ota-merchant / ota-channel）。
- 给 `Chapter07ConfigurationTest.prometheusScrapesCoreServices` 扩 case，断言每个 service name 出现的端口与 `AGENTS.md` 一致（用 `BusinessMetricsConstants` 常量做单一来源）。
- 文档：写明"以后新增服务必须在 `prometheus/prometheus.yml` 与 `AGENTS.md` 同时登记"。

**完成判定**：

- `mvn -pl ota-common -am test` 全绿。
- `docker compose config` 解析后 prometheus 启动 11 个 scrape job。

## Phase 2：业务指标埋点

**目标**：定义业务指标命名规范，在 ota-order / ota-supplier / ota-channel / ota-merchant / ota-inventory 五大业务服务落地 Micrometer Counter / Timer，命名遵循 `ota.<domain>.<action>` 模式。

**命名规范**（写进 `AGENTS.md`）：

- 业务 Counter：`ota.<domain>.<action>_total{tag1,tag2,...}`
- 业务 Timer：`ota.<domain>.<action>_duration_seconds{tag1,tag2,...}`
- 业务 Gauge：`ota.<domain>.<state>`（如 `ota.outbox.pending_count`）

**待落地指标**（节选关键项）：

| 服务 | 指标名 | 类型 | tag 维度 |
| --- | --- | --- | --- |
| ota-order | `ota.order.create_total` | Counter | `status` (success/fail), `merchantId` |
| ota-order | `ota.order.pay_total` | Counter | `status` (success/fail) |
| ota-order | `ota.order.cancel_total` | Counter | `status` (success/fail) |
| ota-order | `ota.order.create_duration_seconds` | Timer | `merchantId` |
| ota-order | `ota.outbox.pending_count` | Gauge | `topic` |
| ota-order | `ota.outbox.send_failure_total` | Counter | `topic` |
| ota-supplier | `ota.supplier.prebook_total` | Counter | `supplierCode`, `result` |
| ota-supplier | `ota.supplier.book_total` | Counter | `supplierCode`, `result` |
| ota-supplier | `ota.supplier.cancel_total` | Counter | `supplierCode`, `result` |
| ota-supplier | `ota.supplier.hz_token_refresh_total` | Counter | `trigger` (scheduled/545) |
| ota-channel | `ota.channel.push_total` | Counter | `channelCode`, `result` |
| ota-channel | `ota.channel.confirm_total` | Counter | `channelCode`, `result` |
| ota-channel | `ota.channel.cancel_total` | Counter | `channelCode`, `result` |
| ota-channel | `ota.channel.push_duration_seconds` | Timer | `channelCode` |
| ota-channel | `ota.channel.kafka_consume_total` | Counter | `topic`, `result` |
| ota-merchant | `ota.merchant.login_total` | Counter | `result` |
| ota-merchant | `ota.merchant.rbac_check_total` | Counter | `result` (allow/deny) |
| ota-inventory | `ota.inventory.price_query_total` | Counter | `channelCode` |
| ota-inventory | `ota.inventory.stock_freeze_total` | Counter | `result` |
| ota-inventory | `ota.inventory.stock_deduct_total` | Counter | `result` |

**新增任务**：

- `ota-common/src/main/java/com/ota/cloud/common/metrics/BusinessMetricsConstants.java`：常量集中托管所有指标名。
- `ota-common/src/main/java/com/ota/cloud/common/metrics/OrderMetrics.java` / `SupplierMetrics.java` / `ChannelMetrics.java`：Micrometer Counter/Timer 注入包装类（不强制各服务自己拼字符串）。
- 在 ota-order 的 `OrderService.createOrder` / `pay` / `cancel`、`OutboxSendJob` 关键路径打点。
- 在 ota-supplier 的 `HzSupplierAdapter.prebook/book/cancel`、`HzTokenService.refresh` 打点。
- 在 ota-channel 的 `ChannelOrderService.push/confirm/cancel`、`OrderPaidEventConsumer` / `OrderCancelledEventConsumer` 打点。
- 在 ota-merchant 的 `MerchantAuthService.login`、`MerchantPermissionInterceptor` 打点。
- 在 ota-inventory 的 `InventoryService.queryPrice/freezeStock/deductStock` 打点。

**完成判定**：

- `mvn -pl ota-order,ota-supplier,ota-channel,ota-merchant,ota-inventory,ota-common -am test` 全绿（按 `AGENTS.md` 一次最多 3 个模块，分批跑）。
- 真实环境触发一次下单 → 验证 `ota.order.create_total` + 1。

## Phase 3：Grafana 仪表盘

**目标**：落地 4 个业务仪表盘 JSON，让运维能直接观察。

**仪表盘清单**：

1. **infra-overview.json**（基础设施总览）
   - JVM Heap Used / Max（`jvm_memory_used_bytes`）
   - CPU Usage（`process_cpu_usage`）
   - HTTP 5xx Rate（`http_server_requests_seconds_count{status=~"5.."}`）
   - GC Pause（`jvm_gc_pause_seconds`）
   - 11 个服务的 UP 状态（`up`）

2. **order-funnel.json**（订单业务漏斗）
   - `ota_order_create_total` 5m rate
   - `ota_order_pay_total` 5m rate
   - `ota_order_cancel_total` 5m rate
   - 订单支付率（pay/create 比例）
   - 订单平均创建耗时（`ota_order_create_duration_seconds` p50/p95/p99）

3. **channel-distribution.json**（渠道分发）
   - 按 `channelCode` 拆分的 `ota_channel_push_total` 速率
   - `ota_channel_push_total{result="fail"}` 5m rate（推单失败率）
   - `ota_channel_push_duration_seconds` p95
   - `ota_channel_kafka_consume_total{result="fail"}`（Kafka 消费失败）
   - 当前 `ota_outbox_pending_count`（未发送 Outbox 积压）

4. **supplier-integration.json**（供应商接入）
   - 按 `supplierCode` 拆分的 `ota_supplier_prebook_total` / `book_total` / `cancel_total` 速率
   - `ota_supplier_hz_token_refresh_total` 趋势（545 触发次数）
   - 供应商补偿任务执行次数（读 `SupplierCompensationJob` 暴露的 Gauge）

**新增任务**：

- 在 `grafana/dashboards/` 写 4 个 JSON。
- 给 `Chapter07ConfigurationTest` 加新 case：4 个 dashboard 文件存在且是合法 JSON，`apiVersion` 字段为 Grafana 8+ 格式。
- `docker-compose.yml` 不变（Grafana 11.6 自动 provisioning 目录），但需要确认 `/etc/grafana/provisioning/dashboards` 的 default provider 开启。

**完成判定**：

- `mvn -pl ota-common -am test` 全绿。
- 真实环境：登录 `http://localhost:3000`（admin/admin123），4 个 dashboard 自动出现在 "General" 文件夹。

## Phase 4：告警规则 + Alertmanager

**目标**：部署 Alertmanager，落地 8 条核心告警规则，让"出事时有人能收到通知"。

**告警规则**（写到 `prometheus/alerts.yml`）：

| 规则名 | 表达式 | for 时长 | severity |
| --- | --- | --- | --- |
| `ServiceDown` | `up{application="ota-..."} == 0` | 2m | critical |
| `HighHttpErrorRate` | `sum(rate(http_server_requests_seconds_count{status=~"5.."}[5m])) by (application) > 0.5` | 5m | warning |
| `ChannelPushFailureRate` | `sum(rate(ota_channel_push_total{result="fail"}[5m])) by (channelCode) / sum(rate(ota_channel_push_total[5m])) by (channelCode) > 0.1` | 5m | warning |
| `SupplierBookFailureRate` | `sum(rate(ota_supplier_book_total{result="fail"}[5m])) by (supplierCode) / sum(rate(ota_supplier_book_total[5m])) by (supplierCode) > 0.1` | 5m | warning |
| `OutboxBacklog` | `ota_outbox_pending_count > 1000` | 5m | critical |
| `JVMHeapHigh` | `jvm_memory_used_bytes{area="heap"} / jvm_memory_max_bytes{area="heap"} > 0.85` | 5m | warning |
| `KafkaConsumerBacklog` | 自定义 Gauge `ota_channel_kafka_consume_lag{topic="order-paid"} > 1000` | 5m | warning |
| `OrderCreateBacklog` | 业务侧新增 Gauge `ota_order_unpaid_count` 持续大于阈值 | 5m | warning |

**Alertmanager 配置**（写到 `prometheus/alertmanager.yml`）：

- 1 个 webhook receiver（URL 占位 `http://webhook:5001/alerts`，本地可换为文件输出）
- 路由按 `severity` 拆分：critical → 立即推送；warning → 5m 内合并推送
- 抑制规则：`ServiceDown` 抑制该 application 的 `HighHttpErrorRate`（服务挂了之后所有 HTTP 5xx 都不再告警，避免风暴）
- `inhibit_rules` + `group_by` + `group_wait` 完整

**新增任务**：

- `docker-compose.yml` 新增 `alertmanager` 服务（image `prom/alertmanager:v0.27.0`）。
- `prometheus/prometheus.yml` 加 `rule_files: ["alerts.yml"]` 和 `alerting.alertmanagers` 段。
- `prometheus/alerts.yml` 写 8 条规则。
- `prometheus/alertmanager.yml` 写完整 receiver/route/inhibit。
- `Chapter07ConfigurationTest` 加 case：assertThat `alerts.yml` 8 条规则都存在；`alertmanager.yml` 含 `webhook` receiver。

**完成判定**：

- `mvn -pl ota-common -am test` 全绿。
- `docker compose up -d alertmanager` 启动健康，`http://localhost:9093` 能访问。
- `http://localhost:9090/alerts` 页面能加载规则（启动时为 inactive）。

## Phase 5：集中式日志（Logback → Logstash → ES → Kibana）

**目标**：12 个服务的 Logback 接入 logstash appender，让 Kibana 能直接搜业务日志。

**方案选型**：选 A（logstash-logback-encoder 直发 logstash）。

- 原因：服务已经在 `host.docker.internal` 跑 Prometheus scraping，logstash 同网段可达；filebeat 需要读容器日志路径，跨平台差异大。

**待落地**：

- 12 个服务（gateway/auth/user/hotel/inventory/order/admin/ai/supplier/merchant/channel/common）统一加 `logback-spring.xml` 模板：
  - 控制台：保留人类可读 pattern（`%5p [%X{traceId:-},%X{spanId:-}]`）
  - 文件：JSON 格式，按 `appender.RollingFile` 输出 `/var/log/ota-cloud/<service>.log`
  - logstash：`LogstashTcpSocketAppender`，地址 `${LOGSTASH_HOST:logstash}:${LOGSTASH_PORT:5044}`
- 引入依赖：每个服务 `pom.xml` 加 `net.logstash.logback:logstash-logback-encoder:7.4`（ota-common 通过 `dependencyManagement` 统一版本）
- 调整 `logstash/pipeline/ota-log.conf`：把 input 从 `tcp` 改为更稳健的 `tcp { codec => json_lines }` 并加 grok 解析 `traceId` 字段；ES index 改 `ota-cloud-logs-YYYY.MM.dd`
- Kibana 端：通过 `kibana/dashboards/ota-cloud-logs.ndjson`（如不存在则跳过）

**新增任务**：

- `ota-common/pom.xml`：`logstash-logback-encoder` 依赖管理。
- `ota-common/src/main/resources/logback-spring.xml`：通用模板（被各服务复用或复制）。
- 12 个服务 `<service>/src/main/resources/logback-spring.xml`：引用 common 模板（如不支持 include 语法，则各服务复制一份并把 `spring.application.name` 占位）。
- 12 个服务 `pom.xml` 加 `logstash-logback-encoder`（非 management，是 dependency）。
- 修 `logstash/pipeline/ota-log.conf`：保留 tcp 5044 输入（兼容老 logstash 启动参数），加 grok 提取 traceId。

**完成判定**：

- `mvn -pl ota-common,ota-gateway,ota-auth -am test` 等分批跑全绿（logback 配置文件改动不影响 Java 编译）。
- 真实环境：下单一次 → Kibana 索引 `ota-cloud-logs-*` 能搜到带 `traceId` 的 JSON 日志。

## Phase 6：Kafka 消息 traceId 透传 + 测试 + smoke + 文档收口

**目标**：把 traceId 从 HTTP 延伸到 Kafka 消息体；落地 6 个 Phase 的 JUnit + smoke + 文档收口。

**Kafka traceId 透传方案**：

- `OutboxService.send()` 把当前线程的 `TraceIdUtil.get()` 写入 `OutboxEvent.headers`（JSON map）。
- `OrderPaidEvent` / `OrderCancelledEvent` 增加 `traceId` 字段（保持向后兼容，缺省 `null`）。
- 消费者端 `OrderPaidEventConsumer` / `OrderCancelledEventConsumer` 解析 `traceId`，调用 `TraceIdUtil.set(traceId)`，确保日志输出 traceId 一致。
- 单元测试：`OutboxServiceTest` 断言写入 outbox 包含 traceId；消费者端断言回填。

**测试覆盖**：

- `BusinessMetricsTest`：OrderMetrics / ChannelMetrics / SupplierMetrics 注入 MeterRegistry 后，调用业务方法能让 Counter 自增。
- `PrometheusScrapeTest`：读 `prometheus/prometheus.yml`，断言 11 个服务 scrape target、端口与 `AGENTS.md` 一致。
- `AlertRuleTest`：读 `prometheus/alerts.yml`，断言 8 条告警规则名都在，每条规则的 `expr` 字段非空。
- `OutboxTraceIdTest`：Outbox 写入的 headers 含 traceId；消费者回填。
- `ChannelKafkaConsumerTest`：order-paid 空消息跳过、有消息时 `ChannelOrderService.confirmByOrderId(orderId)` 被调用。

**Smoke 脚本**：`scripts/chapter12-observability-smoke.sh`

1. 健康：12 个服务 `/actuator/prometheus` 都能返回 200。
2. Prometheus：访问 `http://localhost:9090/targets`，断言 11 个服务都在 `up` 状态。
3. 业务指标：触发一次下单 → 拉 `http://localhost:8080/actuator/prometheus`（ota-order）→ 搜 `ota_order_create_total` → 计数器 ≥ 1。
4. Grafana：访问 `http://localhost:3000/api/dashboards/uid/order-funnel` → 返回 200。
5. Alertmanager：访问 `http://localhost:9093/-/healthy` → 200。
6. Kibana：访问 `http://localhost:5601/api/status` → 200。
7. Logstash → ES 链路：触发的订单日志在 ES 索引 `ota-cloud-logs-*` 出现。

**一键串跑**：`scripts/chapter12-smoke-all.sh` 串起 5 个 smoke 步骤。

**文档收口**：

- `cloud-tutorial-progress.md` Chapter 12 行填入 Done + 6 个 Phase 摘要。
- `docs/superpowers/plans/2026-06-11-chapter12-observability.md`（本文件）保持最新。
- `AGENTS.md` 增补两节：
  - **业务指标命名规范**：`ota.<domain>.<action>_<unit>{tags}` 模板
  - **可观测性部署约定**：12 个服务的 `/actuator/prometheus` 必须暴露；新服务必须同步在 `prometheus/prometheus.yml` 与 `AGENTS.md` 登记
- `prometheus/README.md`：新增（说明 alerts.yml / alertmanager.yml 维护方式）
- `grafana/dashboards/README.md`：新增（说明 4 个 dashboard 的拆解维度）

**反模式**：

- 不接商业 APM（Datadog / New Relic），继续用开源栈
- 不写 E2E Selenium 测 Grafana
- 不引入 ELK Alerting（让 Prometheus 主导告警，Kibana 只做查询）
- 不重写 Spring Boot Actuator 配置，每个服务只暴露 `health,info,prometheus`

**完成判定**：

- `mvn -pl ota-common,ota-order,ota-channel,ota-supplier,ota-merchant,ota-inventory -am test` 全绿（按 3 模块分批跑）。
- `chapter12-observability-smoke.sh` + `chapter12-smoke-all.sh` 在 Windows Docker 真实环境跑通。
- 文档 5 处全部就绪。

## 风险与回退

| 风险 | 应对 |
| --- | --- |
| Grafana 11.6 provisioning 路径不识别 | 已在 docker-compose 显式 mount `grafana/dashboards`，确认 provider `allowUiUpdates=true` |
| Alertmanager 启动时无 receiver 报错 | 写一个占位 webhook receiver，URL 用 `http://host.docker.internal:5001/alerts` 留空 |
| 业务指标维度爆炸（如 `merchantId` 高基数） | tag 维度限制在 5 个以内，merchantId 仅在 ota-order 业务 Counter 用，prometheus 端不做 max_series 调优 |
| Logstash 直连丢失 traceId | 在 logback encoder 显式 includeMdcKeyName=traceId |
| Kafka 消息体变更影响下游 | OrderPaidEvent / OrderCancelledEvent 增字段为可选，向后兼容 |
| 12 个服务 logback 模板不一致 | ota-common 抽 `logback-base.xml` 模板，各服务用 `<include>`（Spring Boot 默认支持） |

## 端口与命名速查（来自 AGENTS.md）

```
ota-gateway:8080   ota-auth:8081   ota-user:8082   ota-hotel:8083
ota-inventory:8084  ota-order:8085  ota-merchant:8086  ota-admin:8087
ota-ai:8088   ota-supplier:8089  ota-channel:8090
```

11 个 scrape target（prometheus.yml）必须严格按此端口对齐。
