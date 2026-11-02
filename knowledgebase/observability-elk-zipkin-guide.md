# ota-cloud 可观测性全景架构指南 (ELK 日志中心 + Zipkin 链路追踪)

> **定位**：系统性梳理 `ota-cloud` 中基于 ELK 栈（Elasticsearch 8.x + Logstash + Kibana）与 OpenZipkin 3.x 构建的**全链路可观测性闭环**。深入剖析 W3C 标准 TraceId 全链路跨线程与跨进程透传、Logstash TCP JSON 直推、ES 索引按天滚动以及 Kibana 与 Zipkin 联合排障实践。

---

## 一、架构定位与设计模式：Distributed Tracing & Unified Logging

在数十个服务协作的复杂微服务场景下，用户下单失败报错时，开发人员往往陷入“人肉找机器翻日志”的困境：
- **全链路染色（MDC TraceId Injection）**：请求自进入网关开始分配唯一的 `traceId`，通过 HTTP Header（`X-Trace-Id`）与 Kafka 消息体跨网络透传，注入业务日志框架（SLF4J MDC）；
- **JSON 结构化直推（Logstash TCP）**：淘汰传统低效的本地磁盘打日志与 Filebeat 扫文件机制，采用 `logstash-logback-encoder` 通过 TCP 异步直推 Logstash（5044 端口），减轻宿主机磁盘 I/O 磨损；
- **链路拓扑与耗时火焰图（Zipkin）**：基于 Micrometer Tracing 自动上报 Span 生命周期，直观呈现网关 ➔ 认证 ➔ 酒店 ➔ 价库 ➔ 订单的调用拓扑与慢方法定位。

```
【HTTP 客户端请求 / 异步 Kafka 消息】
              │
              ▼ (携带 TraceId)
┌────────────────────────────────────────────────────────┐
│   微服务业务进程 (Logback + Micrometer Tracing)        │
│                                                        │
│   • 格式化为 JSON: @timestamp, level, message, traceId  │
│   • 异步 Socket 直推 Logstash (TCP :5044)              │
│   • 异步 HTTP 上报 Trace Spans 到 Zipkin (HTTP :9411)  │
└──────────────┬─────────────────────────┬───────────────┘
               │ (TCP 5044 JSON Lines)   │ (HTTP 9411 Spans)
               ▼                         ▼
┌───────────────────────────┐  ┌─────────────────────────┐
│      Logstash Pipeline    │  │      OpenZipkin 3.x     │
│  - traceId 缺失兜底填 '-' │  │  - 跨进程链路依赖拓扑图 │
│  - 提取 service 动态路由   │  │  - 单次调用各阶段耗时瀑布│
└──────────────┬────────────┘  └─────────────────────────┘
               │
               ▼ (按天滚动写入: ota-cloud-logs-YYYY.MM.dd)
┌───────────────────────────┐
│     Elasticsearch (9200)  │
└──────────────┬────────────┘
               │
               ▼
┌───────────────────────────┐
│       Kibana (5601)       │ 开发者在检索框直接输入 TraceId，
│   可视化集中日志快速检索  │ 0.1 秒定位全链路所有微服务报错日志！
└───────────────────────────┘
```

---

## 二、微服务拓扑与角色分工

1. **客户端日志收集（各微服务）**：
   - 依赖 `logstash-logback-encoder:7.4`，统一配置 `logback-spring.xml`；
   - 依赖 `micrometer-tracing-bridge-brave` + `zipkin-reporter-brave` 负责分布式链路抓取。
2. **Logstash 数据管道（端口 5044）**：
   - 管道配置：`logstash/pipeline/ota-log.conf`，校验并标准化 `traceId`，追加 `project: ota-cloud` 标识。
3. **Elasticsearch 8.x（端口 9200）**：
   - 建立索引生命周期管理（ILM），单索引按天切分（`ota-cloud-logs-YYYY.MM.dd`），保障查询响应在 20ms 以内。
4. **Kibana 8.x（端口 5601）**：
   - 日志交互查询大屏，支持针对 `service`、`level: ERROR`、`traceId` 组合过滤。
5. **Zipkin 3.x（端口 9411）**：
   - 链路可视化平台，监控各微服务 RPC 调用的耗时分位数（P95、P99）。

---

## 三、关键落地技术细节

### 1. Logstash 核心处理管道（`ota-log.conf`）

```ruby
input {
  tcp {
    port => 5044
    codec => json_lines
  }
}

filter {
  # 兜底：若请求未染色 traceId，自动补充 '-' 避免 Kibana 聚合字段缺失
  if ![traceId] {
    mutate { add_field => { "traceId" => "-" } }
  }
  if ![spanId] {
    mutate { add_field => { "spanId" => "-" } }
  }

  mutate { uppercase => ["level"] }
  mutate { add_field => { "project" => "ota-cloud" } }
}

output {
  elasticsearch {
    hosts => ["http://elasticsearch:9200"]
    index => "ota-cloud-logs-%{+YYYY.MM.dd}"
  }
}
```

### 2. 微服务 Logback 异步直推配置（`logback-spring.xml` 核心骨架）

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <springProperty scope="context" name="appName" source="spring.application.name"/>

    <appender name="LOGSTASH" class="net.logstash.logback.appender.LogstashTcpSocketAppender">
        <destination>${LOGSTASH_HOST:100.99.175.108}:5044</destination>
        <encoder class="net.logstash.logback.encoder.LogstashEncoder">
            <customFields>{"service":"${appName}"}</customFields>
            <includeMdcKeyName>traceId</includeMdcKeyName>
            <includeMdcKeyName>spanId</includeMdcKeyName>
        </encoder>
        <keepAliveDuration>5 minutes</keepAliveDuration>
    </appender>

    <root level="INFO">
        <appender-ref ref="LOGSTASH"/>
    </root>
</configuration>
```

---

## 四、排障实战指引：从用户报错到代码行定位

1. **获取 TraceId**：
   - 前端或接口报错时，网关返回体中均包含：`{"code":500,"message":"...","traceId":"61dbbc9dd9950cfc"}`；
2. **Kibana 秒级定位日志**：
   - 登录 Kibana，在 Discover 查询栏中输入：`traceId: "61dbbc9dd9950cfc"`；
   - 屏幕上立即按照时间先后顺序列出：Gateway 路由记录 ➔ Order 接收参数 ➔ Inventory 乐观锁抛错堆栈，精确定位故障代码行！
3. **Zipkin 分析链路耗时瓶颈**：
   - 在 Zipkin 搜索该 TraceId，直观查看耗时最长的 Span（例如发现是下游某供应商适配接口耗时 3200ms），快速厘清责任归属。

---

## 五、基础设施与运维部署

- **镜像与组件清单**：
  - Elasticsearch: `elasticsearch:8.17.2`（单节点，禁用安全认证开发模式，端口 9200）
  - Logstash: `logstash:8.17.2`（挂载 `ota-log.conf`，端口 5044）
  - Kibana: `kibana:8.17.2`（端口 5601）
  - Zipkin: `openzipkin/zipkin:3`（端口 9411）
- **可视化入口**：
  - Kibana 日志看板：[http://localhost:5601](http://localhost:5601)
  - Zipkin 链路追踪：[http://localhost:9411](http://localhost:9411)
