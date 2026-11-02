# 第14章：分布式高并发韧性（Sentinel 生产级流控与热点熔断体系）

> **章节目标**：构建生产级微服务高可用流量防护与容灾体系，涵盖网关层前置流量整形、日历价库核心业务热点参数防护（Hotspot Param Flow）、上游异构供应商慢调用比例与异常熔断降级（Circuit Breaking），以及 Sentinel 规则在 Nacos 的动态持久化，彻底杜绝单点爆款请求引发的系统级联雪崩。

---

## 14.1 生产痛点与设计目标

在大型酒旅在线预订平台中，流量具有极强的“突发性”与“局部聚集性”：
1. **网关流控不规范**：原生 Sentinel Gateway 拦截时默认抛出纯文本或非结构化错误，导致前端或上游渠道解析异常；
2. **局部爆款击穿全局**：节假日或大促秒杀期间，极个别爆款热门酒店的查价与预占请求瞬间放大千百倍，极易耗尽全局数据库连接池，导致其他普通酒店全部无法下单；
3. **外部供应商级联雪崩**：上游酒店集团 CRS/B2B 直连接口网络抖动或响应迟缓（RT 达 3~5 秒）时，微服务的工作线程与连接池被严重挂起，迅速形成级联雪崩；
4. **配置脆弱性**：Dashboard 推送的内存规则在微服务重启或弹性扩缩容后全部丢失。

```
【客户端/渠道请求】
       │
       ▼
┌──────────────────────────────────────────────┐
│  ota-gateway (端口 8080)                     │
│  - SentinelGatewayFilter 前置流量整形        │
│  - 客户端 IP 频次限制                        │
│  - 统一 HTTP 429 JSON 结构响应               │
└──────────────────────┬───────────────────────┘
                       │
       ┌───────────────┴───────────────┐
       ▼                               ▼
┌──────────────────────────────┐┌──────────────────────────────┐
│  ota-hotel (端口 8083)       ││  ota-inventory (端口 8084)   │
│  - hotelDetail 接口单机限流   ││  - inventoryPriceQuery 热点   │
│  - 返回 429 提示稍后再试     ││    参数防护 (hotelId=101)   │
│                              ││  - inventoryFreeze 并发写限流 │
└──────────────────────────────┘└──────────────┬───────────────┘
                                               │ 下单预占/出票
                                               ▼
                                ┌──────────────────────────────┐
                                │  ota-supplier (端口 8089)     │
                                │  - supplierBookingBook       │
                                │  - 慢调用熔断 (RT>2000ms)    │
                                │  - 异常比例熔断 (>40%)       │
                                │  - 快速失败并转入后台补偿 Job│
                                └──────────────────────────────┘
```

---

## 14.2 网关前置流控与 429 统一响应（`ota-gateway`）

在 `ota-gateway` 模块中，通过 `SentinelGatewayFilter` 拦截全部外部入口流量，并在 `GatewayCallbackManager` 中定制 `BlockRequestHandler`，将限流拦截统一封装为符合工程规范的标准 JSON 格式：

```java
package com.ota.cloud.gateway.config;

import com.alibaba.csp.sentinel.adapter.gateway.common.rule.GatewayFlowRule;
import com.alibaba.csp.sentinel.adapter.gateway.common.rule.GatewayParamFlowItem;
import com.alibaba.csp.sentinel.adapter.gateway.common.rule.GatewayRuleManager;
import com.alibaba.csp.sentinel.adapter.gateway.sc.SentinelGatewayFilter;
import com.alibaba.csp.sentinel.adapter.gateway.sc.callback.BlockRequestHandler;
import com.alibaba.csp.sentinel.adapter.gateway.sc.callback.GatewayCallbackManager;
import com.alibaba.csp.sentinel.adapter.gateway.sc.exception.SentinelGatewayBlockExceptionHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ota.cloud.common.model.Result;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerCodecConfigurer;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.reactive.result.view.ViewResolver;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 网关 Sentinel 流量整形与限流配置。
 *
 * <p>核心职责：</p>
 * <ol>
 *     <li>注册 SentinelGatewayFilter，作为全局过滤器拦截外部入口流量并实施整形；</li>
 *     <li>注册 SentinelGatewayBlockExceptionHandler，捕获网关层 BlockException；</li>
 *     <li>定制 GatewayCallbackManager 的 BlockRequestHandler，被限流时统一返回标准 HTTP 429 规范的 JSON 结构；</li>
 *     <li>初始化默认的 Route 级与 IP 维度的防护规则。</li>
 * </ol>
 */
@Slf4j
@Configuration
public class GatewaySentinelConfig {

    private final ObjectMapper objectMapper;
    private final List<ViewResolver> viewResolvers;
    private final ServerCodecConfigurer serverCodecConfigurer;

    public GatewaySentinelConfig(
            ObjectMapper objectMapper,
            ObjectProvider<List<ViewResolver>> viewResolversProvider,
            ServerCodecConfigurer serverCodecConfigurer) {
        this.objectMapper = objectMapper != null ? objectMapper : new ObjectMapper();
        this.viewResolvers = viewResolversProvider != null
                ? viewResolversProvider.getIfAvailable(Collections::emptyList)
                : Collections.emptyList();
        this.serverCodecConfigurer = serverCodecConfigurer;
    }

    /**
     * 注册 Sentinel 网关全局过滤器。
     * 最高优先级之一（HIGHEST_PRECEDENCE），在路由转发前实施限流统计与校验。
     */
    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    public GlobalFilter sentinelGatewayFilter() {
        return new SentinelGatewayFilter();
    }

    /**
     * 注册 Sentinel 网关异常处理器。
     * 当触发限流规则时，接管异常抛出链路并转入自定义回调。
     */
    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    public SentinelGatewayBlockExceptionHandler sentinelGatewayBlockExceptionHandler() {
        return new SentinelGatewayBlockExceptionHandler(viewResolvers, serverCodecConfigurer);
    }

    /**
     * 初始化自定义网关限流返回与规则。
     */
    @PostConstruct
    public void initBlockHandlerAndRules() {
        // 1. 定制限流触发时的统一 JSON 响应体
        GatewayCallbackManager.setBlockHandler(new BlockRequestHandler() {
            @Override
            public Mono<ServerResponse> handleRequest(ServerWebExchange exchange, Throwable ex) {
                String requestPath = exchange.getRequest().getURI().getPath();
                log.warn("网关入口流量被 Sentinel 限流拦截: path={}, ex={}", requestPath, ex.getClass().getSimpleName());

                Result<Void> errorResult = Result.error(429, "当前访问过于频繁，已被网关流控拦截，请稍后再试");

                String jsonBody;
                try {
                    jsonBody = objectMapper.writeValueAsString(errorResult);
                } catch (JsonProcessingException e) {
                    jsonBody = "{\"code\":429,\"message\":\"请求过于频繁，请稍后再试\"}";
                }

                return ServerResponse.status(HttpStatus.TOO_MANY_REQUESTS)
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-RateLimit-Action", "Blocked-By-Sentinel")
                        .body(BodyInserters.fromValue(jsonBody));
            }
        });

        // 2. 初始化核心业务路由的默认流控规则（例如针对库存、酒店大促查询）
        initDefaultGatewayRules();
    }

    /**
     * 初始化网关路由级默认限流规则。
     * 针对高频且易被打爆的 ota-hotel 与 ota-inventory 路由，配置基于客户端 IP 的限流。
     */
    private void initDefaultGatewayRules() {
        Set<GatewayFlowRule> rules = new HashSet<>();

        // 针对 ota-hotel 路由配置默认限流：单机 QPS 上限 2000
        GatewayFlowRule hotelRule = new GatewayFlowRule("ota-hotel")
                .setCount(2000)
                .setIntervalSec(1);
        rules.add(hotelRule);

        // 针对 ota-inventory 路由配置基于 Remote IP 维度的频次整形：单一 IP 每秒最多允许 200 次查价预占
        GatewayFlowRule inventoryRule = new GatewayFlowRule("ota-inventory")
                .setCount(200)
                .setIntervalSec(1)
                .setParamItem(new GatewayParamFlowItem()
                        .setParseStrategy(com.alibaba.csp.sentinel.adapter.gateway.common.SentinelGatewayConstants.PARAM_PARSE_STRATEGY_CLIENT_IP)
                );
        rules.add(inventoryRule);

        GatewayRuleManager.loadRules(rules);
        log.info("网关 Sentinel 默认路由规则初始化完成，已加载规则数: {}", rules.size());
    }
}
```

---

## 14.3 核心业务热点参数防护（`ota-inventory`）

针对日历价库范围查询接口（`inventoryPriceQuery`）和下单库存冻结写接口（`inventoryFreeze`），增加 `@SentinelResource` 注解，并在 `InventorySentinelConfig` 中配置针对第 0 个参数 `hotelId` 的细粒度热点限流：

### 1. 控制器注解埋点（`InventoryController.java`）

```java
    /**
     * 查询价库范围。外部经 Gateway 访问路径为 /api/inventory/price/query。
     * <p>
     * 具备 Sentinel 流量防护与热点参数流控，防止爆款酒店突发请求冲垮缓存与数据库。
     * </p>
     *
     * @param hotelId     酒店主键 ID（作为热点参数索引 0 重点流控）
     * @param roomType    房型代码
     * @param startDate   入住日期
     * @param endDate     离店日期
     * @param channelCode 可选下游渠道代码（触发动态加价）
     * @param saleCode    可选售卖策略代码
     * @return 日历价库列表
     */
    @GetMapping("/price/query")
    @SentinelResource(
            value = "inventoryPriceQuery",
            blockHandler = "queryPriceBlockHandler",
            fallback = "queryPriceFallback"
    )
    public Result<List<InventoryPriceVO>> queryPrice(@RequestParam Long hotelId,
                                                     @RequestParam String roomType,
                                                     @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate startDate,
                                                     @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate endDate,
                                                     @RequestParam(required = false) String channelCode,
                                                     @RequestParam(required = false) String saleCode) {
        return Result.success(inventoryService.queryPriceAndStock(
                hotelId, roomType, startDate, endDate, channelCode, saleCode));
    }

    /**
     * 价库范围查询触发 Sentinel 限流（QPS 超标或热点酒店参数超标）时的回调处理器。
     */
    public Result<List<InventoryPriceVO>> queryPriceBlockHandler(Long hotelId,
                                                                 String roomType,
                                                                 LocalDate startDate,
                                                                 LocalDate endDate,
                                                                 String channelCode,
                                                                 String saleCode,
                                                                 BlockException ex) {
        String ruleDesc = ex != null ? ex.getClass().getSimpleName() : "unknown";
        log.warn("价库查询触发 Sentinel 限流拦截: hotelId={}, roomType={}, ruleDesc={}",
                hotelId, roomType, ruleDesc);
        return Result.error(429, "当前酒店日历价库查询过于频繁，请稍后再试");
    }

    /**
     * 价库范围查询发生系统异常时的 Fallback 降级处理器。
     */
    public Result<List<InventoryPriceVO>> queryPriceFallback(Long hotelId,
                                                             String roomType,
                                                             LocalDate startDate,
                                                             LocalDate endDate,
                                                             String channelCode,
                                                             String saleCode,
                                                             Throwable ex) {
        log.error("价库查询发生服务异常，hotelId={}, roomType={}", hotelId, roomType, ex);
        return Result.error(500, "日历价库服务暂时不可用，请稍后重试");
    }

    /**
     * 冻结库存，下单阶段调用。
     * 配置 SentinelResource，防止秒杀场景高并发并发争抢乐观锁导致线程池耗尽。
     */
    @PostMapping("/freeze")
    @SentinelResource(
            value = "inventoryFreeze",
            blockHandler = "freezeBlockHandler"
    )
    public Result<InventoryResultVO> freeze(@Valid @RequestBody InventoryOperateDTO dto) {
        return Result.success(inventoryService.freeze(dto));
    }

    /**
     * 冻结库存触发 Sentinel 限流拦截的回调函数。
     */
    public Result<InventoryResultVO> freezeBlockHandler(InventoryOperateDTO dto, BlockException ex) {
        String ruleDesc = ex != null ? ex.getClass().getSimpleName() : "unknown";
        log.warn("库存冻结接口被 Sentinel 限流: orderId={}, hotelId={}, ruleDesc={}",
                dto != null ? dto.getOrderId() : null,
                dto != null ? dto.getHotelId() : null,
                ruleDesc);
        return Result.error(429, "当前房型下单抢购过于激烈，请稍后再试");
    }
```

### 2. 热点参数规则与写限流规则配置（`InventorySentinelConfig.java`）

```java
package com.ota.cloud.inventory.config;

import com.alibaba.csp.sentinel.slots.block.RuleConstant;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRuleManager;
import com.alibaba.csp.sentinel.slots.block.flow.param.ParamFlowItem;
import com.alibaba.csp.sentinel.slots.block.flow.param.ParamFlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.param.ParamFlowRuleManager;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 库存服务 Sentinel 规则管理与热点防护配置。
 */
@Slf4j
@Configuration
public class InventorySentinelConfig {

    @Value("${ota.sentinel.inventory.query-qps:1000}")
    private double defaultQueryQps;

    @Value("${ota.sentinel.inventory.hot-hotel-qps:100}")
    private double hotHotelQps;

    @Value("${ota.sentinel.inventory.freeze-qps:500}")
    private double freezeQps;

    @PostConstruct
    public void initRules() {
        initParamFlowRules();
        initFlowRules();
    }

    public void initParamFlowRules() {
        List<ParamFlowRule> rules = new ArrayList<>();

        ParamFlowRule hotelParamRule = new ParamFlowRule("inventoryPriceQuery")
                .setParamIdx(0) // 第 0 个参数即 hotelId
                .setGrade(RuleConstant.FLOW_GRADE_QPS)
                .setCount(defaultQueryQps)
                .setDurationInSec(1);

        // 为核心爆款酒店设置特化例外项（ParamFlowItem）
        ParamFlowItem hotHotelItem = new ParamFlowItem()
                .setObject("101")
                .setClassType(Long.class.getName())
                .setCount((int) hotHotelQps);

        hotelParamRule.setParamFlowItemList(Collections.singletonList(hotHotelItem));
        rules.add(hotelParamRule);

        ParamFlowRuleManager.loadRules(rules);
        log.info("库存服务 Sentinel 热点参数规则加载完成，规则数: {}", rules.size());
    }

    public void initFlowRules() {
        List<FlowRule> rules = new ArrayList<>();

        FlowRule freezeRule = new FlowRule("inventoryFreeze")
                .setGrade(RuleConstant.FLOW_GRADE_QPS)
                .setCount(freezeQps)
                .setControlBehavior(RuleConstant.CONTROL_BEHAVIOR_DEFAULT);

        rules.add(freezeRule);

        FlowRuleManager.loadRules(rules);
        log.info("库存服务 Sentinel 基础流控规则加载完成，规则数: {}", rules.size());
    }
}
```

---

## 14.4 上游供应慢调用与异常熔断（`ota-supplier`）

外部供应商（如华住 CRS/B2B）接口经常发生超时或抛 5xx。在 `ota-supplier` 模块中配置慢调用比例熔断与异常比例熔断，触发熔断时快速失败并返回明确降级状态，交由第 9 章构建的 `SupplierCompensationJob` 在后台异步重试：

### 1. 熔断配置类（`SupplierSentinelConfig.java`）

```java
package com.ota.cloud.supplier.config;

import com.alibaba.csp.sentinel.slots.block.RuleConstant;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRule;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRuleManager;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * 上游供应商适配模块 Sentinel 熔断降级规则配置。
 */
@Slf4j
@Configuration
public class SupplierSentinelConfig {

    @Value("${ota.sentinel.supplier.slow-rt-ms:2000}")
    private double slowRtMs;

    @Value("${ota.sentinel.supplier.slow-ratio:0.5}")
    private double slowRatio;

    @Value("${ota.sentinel.supplier.recovery-timeout-sec:10}")
    private int recoveryTimeoutSec;

    @PostConstruct
    public void initDegradeRules() {
        List<DegradeRule> rules = new ArrayList<>();

        // 1. 慢调用比例熔断规则：当响应时间超过 2000ms 的比例达到 50% 时触发熔断
        DegradeRule slowCallRule = new DegradeRule("supplierBookingBook")
                .setGrade(RuleConstant.DEGRADE_GRADE_RT)
                .setCount(slowRtMs)
                .setSlowRatioThreshold(slowRatio)
                .setMinRequestAmount(5)
                .setStatIntervalMs(10000)
                .setTimeWindow(recoveryTimeoutSec);
        rules.add(slowCallRule);

        // 2. 异常比例熔断规则：当异常比例达到 40% 时触发熔断
        DegradeRule exceptionRatioRule = new DegradeRule("supplierBookingBook")
                .setGrade(RuleConstant.DEGRADE_GRADE_EXCEPTION_RATIO)
                .setCount(0.4)
                .setMinRequestAmount(5)
                .setStatIntervalMs(10000)
                .setTimeWindow(recoveryTimeoutSec);
        rules.add(exceptionRatioRule);

        DegradeRuleManager.loadRules(rules);
        log.info("供应商适配服务 Sentinel 熔断降级规则加载完成，已加载规则数: {}", rules.size());
    }
}
```

### 2. 订房出票接口熔断降级（`SupplierInternalController.java`）

```java
    /**
     * 确认供应商订单出票。
     * 配置 Sentinel 慢调用与异常比例熔断，防止外部供应商超时拖垮系统。
     */
    @PostMapping("/book")
    @SentinelResource(
            value = "supplierBookingBook",
            blockHandler = "bookBlockHandler",
            fallback = "bookFallback"
    )
    public Result<BookingResponseDTO> book(@Valid @RequestBody BookingRequestDTO dto) {
        SupplierAdapter adapter = router.route(dto.getSupplierCode());
        BookingResult result = adapter.booking().book(toRequest(dto));
        return Result.success(toResponse(result));
    }

    /**
     * 外部供应商出票接口触发 Sentinel 熔断降级或流控时的回调处理。
     */
    public Result<BookingResponseDTO> bookBlockHandler(BookingRequestDTO dto, BlockException ex) {
        String ruleDesc = ex != null ? ex.getClass().getSimpleName() : "unknown";
        log.warn("外部供应商出票接口被 Sentinel 熔断/限流拦截: requestNo={}, supplierCode={}, ruleDesc={}",
                dto != null ? dto.getRequestNo() : null,
                dto != null ? dto.getSupplierCode() : null,
                ruleDesc);
        BookingResponseDTO fallbackResponse = new BookingResponseDTO();
        fallbackResponse.setSuccess(false);
        fallbackResponse.setSupplierCode(dto != null ? dto.getSupplierCode() : null);
        fallbackResponse.setMessage("外部供应商接口响应缓慢已触发熔断保护，单据将由系统后台补偿任务自动处理重试");
        return Result.success(fallbackResponse);
    }

    /**
     * 外部供应商出票接口发生未捕获系统异常时的兜底回调。
     */
    public Result<BookingResponseDTO> bookFallback(BookingRequestDTO dto, Throwable ex) {
        log.error("外部供应商出票接口调用发生非预期系统异常: requestNo={}, supplierCode={}",
                dto != null ? dto.getRequestNo() : null,
                dto != null ? dto.getSupplierCode() : null,
                ex);
        BookingResponseDTO fallbackResponse = new BookingResponseDTO();
        fallbackResponse.setSuccess(false);
        fallbackResponse.setSupplierCode(dto != null ? dto.getSupplierCode() : null);
        fallbackResponse.setMessage("外部供应商网络异常，将由后台补偿任务自动重试");
        return Result.success(fallbackResponse);
    }
```

---

## 14.5 Sentinel 动态规则 Nacos 持久化设计

在微服务容器化或多实例集群环境下，通过接入 `com.alibaba.csp:sentinel-datasource-nacos`，将规则发布至 Nacos 配置中心，微服务启动时自动拉取并建立长轮询监听：

### Nacos 配置模型

- **Namespace**：`public`（或平台统一配置命名空间）
- **Group**：`SENTINEL_GROUP`
- **Data ID 约定**：
  - 流控规则：`${spring.application.name}-flow-rules`
  - 熔断规则：`${spring.application.name}-degrade-rules`
  - 热点规则：`${spring.application.name}-param-flow-rules`

### 生产级配置（`application.yml`）

```yaml
spring:
  cloud:
    sentinel:
      transport:
        dashboard: ${SENTINEL_DASHBOARD:localhost:8088}
        port: 8719
      eager: true
      datasource:
        flow:
          nacos:
            server-addr: ${NACOS_SERVER:localhost:8848}
            dataId: ${spring.application.name}-flow-rules
            groupId: SENTINEL_GROUP
            data-type: json
            rule-type: flow
        degrade:
          nacos:
            server-addr: ${NACOS_SERVER:localhost:8848}
            dataId: ${spring.application.name}-degrade-rules
            groupId: SENTINEL_GROUP
            data-type: json
            rule-type: degrade
        param-flow:
          nacos:
            server-addr: ${NACOS_SERVER:localhost:8848}
            dataId: ${spring.application.name}-param-flow-rules
            groupId: SENTINEL_GROUP
            data-type: json
            rule-type: param-flow
```

---

## 14.6 自动化测试与验证

执行各微服务中针对 Sentinel 前置限流、热点参数控制与慢调用熔断的自动化单元测试：

```bash
# 1. 验证网关 Sentinel 前置流控与 429 统一 JSON 拦截
mvn test -pl ota-gateway -Dtest=GatewaySentinelConfigTest

# 2. 验证库存日历价库热点参数流控与并发写防护
mvn test -pl ota-inventory -Dtest=InventorySentinelFlowTest

# 3. 验证外部供应商订房慢调用与异常熔断降级
mvn test -pl ota-supplier -Dtest=SupplierSentinelDegradeTest

# 4. 执行全模块回归测试
mvn test -pl ota-gateway,ota-inventory,ota-supplier
```
