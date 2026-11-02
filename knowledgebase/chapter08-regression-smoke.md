# 第8章：回归测试与真实环境 Smoke 自动化

> **目标**：不新增业务主线，把第 1~7 章构建的 API 网关、认证中心、用户中心、酒店服务、价库库存中心、订单中心、Kafka Outbox 事务发件箱与 AI 降级链路，通过层级化单元测试（Focused Unit Tests）与真实中间件自动化脚本进行严格的质量收敛、断言对账与基准固化。

---

## 8.1 架构收敛背景与质量基准

在微服务拆分后，系统由单一进程转变为分布式网络协作拓扑。在进入第 9 章上游供应方适配与第 10 章商家中心之前，必须在 P0 阶段完成严格的回归与质量固化。

```
                    ┌───────────────────────────────┐
                    │     HTTP 客户端 / 冒烟脚本     │
                    └───────────────┬───────────────┘
                                    │ (8080)
                                    ▼
                    ┌───────────────────────────────┐
                    │      ota-gateway 统一网关     │
                    │   JWT 鉴权 / 白名单 / 链路染色 │
                    └───────┬───────────────┬───────┘
                            │               │
            ┌───────────────┘               └───────────────┐
            ▼ (8081)                                        ▼ (8082)
┌───────────────────────┐                       ┌───────────────────────┐
│       ota-auth        │                       │       ota-user        │
│    登录 / 签发 Token   │                       │    用户注册 / 信息查询 │
└───────────────────────┘                       └───────────────────────┘
            │                                               │
            └───────────────────────┬───────────────────────┘
                                    │ Feign RPC / Gateway 路由
            ┌───────────────────────┼───────────────────────┐
            ▼ (8083)                ▼ (8084)                ▼ (8085)
┌───────────────────────┐ ┌───────────────────────┐ ┌───────────────────────┐
│       ota-hotel       │ │     ota-inventory     │ │       ota-order       │
│   详情 / Redis 缓存   │ │   乐观锁 / 库存流水   │ │  订单状态机 / Outbox  │
└───────────────────────┘ └───────────────────────┘ └───────────┬───────────┘
                                                                │
                                                                ▼
                                                    ┌───────────────────────┐
                                                    │     Kafka Cluster     │
                                                    │      order-paid       │
                                                    └───────────────────────┘
```

### 核心质量风险点

1. **网关与下游 Header 透传丢失**：网关解析 JWT 后向下游请求注入 `X-OTA-UserId` 与 `TraceId`，下游 Controller 与 Feign 拦截器能否稳定获取。
2. **库存状态机单向原子流转**：在高并发下，防超卖乐观锁（`CAS`）是否能保证 `available`、`frozen`、`booked` 严格守恒，且流水表记录完整。
3. **本地事务与 Outbox 事件一致性**：创建订单与支付更新时，本地订单更新与待发送事件持久化是否在同一事务内成功，后台任务是否能在 5 秒内可靠投递至 Kafka。
4. **第三方依赖故障降级**：在未配置外网大模型 API Key 时，AI 服务能否平稳降级回退默认推荐，不向前端抛出 500 异常。

---

## 8.2 核心单元测试集（Focused Tests）

为了在开发阶段无需启动数十个进程即可验证业务核心逻辑，项目建立了轻量级层级化测试集。

### 8.2.1 模块测试矩阵

| 模块 | 测试类 | 核心验证能力与断言边界 |
| :--- | :--- | :--- |
| `ota-gateway` | `AuthGlobalFilterTest` | 验证登录接口白名单跳过鉴权；受保护接口缺失 Token 拦截 401；有效 Token 解析并在下游 Header 中注入 `X-OTA-UserId` 与 `X-OTA-Username`。 |
| `ota-gateway` | `TraceIdFilterTest` | 验证无 TraceId 时自动生成 UUID 并注入响应头；已有 TraceId 时原样继承透传。 |
| `ota-auth` | `AuthControllerTest` | 验证密码哈希校验成功签发 Token；密码错误抛出业务异常；登出请求将 JTI 写入 Redis 黑名单。 |
| `ota-user` | `UserControllerTest` | 验证基于网关注入的 Header 获取当前用户资料；未携带 Header 时拒绝未授权请求。 |
| `ota-hotel` | `HotelServiceTest` | 验证基于 Cache-Aside 模式的酒店详情缓存读穿；缓存命中时直接返回，不触发数据库 Mapper 调用。 |
| `ota-hotel` | `HotelSentinelTest` | 验证 Sentinel 热点参数流控配置，并发高频触发时平稳返回 429 降级状态。 |
| `ota-inventory` | `InventoryServiceTest` | 验证冻结（`freeze`）、释放（`release`）、扣减（`deduct`）三阶段版本号 CAS 更新，断言流水表 `inventory_record` 正确写入。 |
| `ota-order` | `OrderServiceTest` | 验证创单防重 Token；订单超时未支付状态机流转；订单支付成功时同事务落库 `outbox_event`。 |
| `ota-ai` | `AiServiceTest` | 验证在缺失 OpenAI API Key 时的静默降级逻辑，兜底返回默认结构化推荐结果。 |

### 8.2.2 网关鉴权过滤器核心测试实现

网关作为流量入口，鉴权过滤器的正确性是系统安全的第一道防线：

```java
package com.ota.cloud.gateway.filter;

import com.ota.cloud.common.constant.CommonConstant;
import com.ota.cloud.gateway.config.WhiteListConfig;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AuthGlobalFilterTest {

    private WhiteListConfig whiteListConfig;
    private StringRedisTemplate redisTemplate;
    private ValueOperations<String, String> valueOperations;
    private AuthGlobalFilter filter;

    @BeforeEach
    void setUp() {
        whiteListConfig = new WhiteListConfig();
        whiteListConfig.setPaths(List.of("/api/auth/login", "/actuator/health"));
        redisTemplate = mock(StringRedisTemplate.class);
        valueOperations = mock(ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        filter = new AuthGlobalFilter(whiteListConfig, redisTemplate);
    }

    /**
     * 测试白名单路由放行场景：
     * 1. 构造访问白名单路径（/api/auth/login）的响应式 MockServerWebExchange；
     * 2. 执行响应式过滤器链，通过 block() 阻塞订阅并等待 Mono 异步流执行完成；
     * 3. 断言下游 FilterChain 成功被调用（即请求被正常放行）；
     * 4. 严格断言未触发 Redis 操作，避免不必要的 I/O 开销。
     */
    @Test
    void allowsWhiteListedRequestWithoutJwtOrRedisLookup() {
        // 构建白名单路径的模拟响应式 HTTP 请求上下文
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/auth/login").build());
        AtomicBoolean called = new AtomicBoolean(false);

        // 模拟响应式下游过滤器链：
        // 接收 ServerWebExchange，将 called 状态置为 true，并返回 Mono.empty() 响应式空流表示下游处理完成
        GatewayFilterChain chain = ex -> {
            called.set(true);
            return Mono.empty();
        };

        // 触发响应式网关过滤流：
        // filter.filter() 返回 Project Reactor 的 Mono<Void> 异步流，
        // 在单元测试中调用 .block() 阻塞等待异步流执行结束
        filter.filter(exchange, chain).block();

        // 断言下游链路已被放行执行
        assertThat(called).isTrue();
        // 验证未进行 Redis 黑名单查验
        verify(redisTemplate, never()).opsForValue();
    }

    /**
     * 测试受保护接口缺失 Token 拦截场景：
     * 1. 访问需要登录认证的接口（/api/user/info），请求头中未携带 Authorization；
     * 2. 响应式流处理后直接短路返回，不流转至下游 FilterChain；
     * 3. 断言响应状态码被置为 401 Unauthorized。
     */
    @Test
    void rejectsProtectedRequestWithoutAuthorizationHeader() {
        // 构建无认证请求头的受保护资源请求
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/user/info").build());

        // 执行网关鉴权过滤：下游 Chain 即使被调用也只返回 Mono.empty()，但预期在 filter 内部已被短路阻断
        filter.filter(exchange, ex -> Mono.empty()).block();

        // 断言网关层直接向客户端返回 401 未授权 HTTP 状态码
        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    /**
     * 测试有效 Token 鉴权通过并向下游服务注入用户上下文 Header 场景：
     * 1. 模拟生成合法的 HMAC-SHA256 签名 JWT；
     * 2. 模拟 Redis 黑名单未命中（Token 状态正常未作废）；
     * 3. 拦截下游 FilterChain，提取请求头中的 X-OTA-UserId 与 X-OTA-Username 并进行严格断言。
     */
    @Test
    void injectsUserHeadersWhenJwtIsValidAndNotBlacklisted() {
        // 生成测试专用有效 JWT Token
        String token = generateToken(1001L, "alice");
        // 模拟 Redis 中不存在该 Token 的作废黑名单标记
        when(valueOperations.get(startsWith(CommonConstant.REDIS_KEY_JWT_BLACKLIST))).thenReturn(null);

        // 构造携带合法 Bearer Token 的网关请求
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/user/info")
                        .header(CommonConstant.JWT_HEADER, CommonConstant.JWT_PREFIX + token)
                        .build());

        AtomicBoolean called = new AtomicBoolean(false);
        // 验证下游过滤器链收到的请求已被网关装饰并注入用户身份请求头
        GatewayFilterChain chain = ex -> {
            called.set(true);
            // 校验网关鉴权过滤器是否正确从 JWT Payload 中解析并注入了 X-OTA-UserId
            assertThat(ex.getRequest().getHeaders().getFirst(CommonConstant.HEADER_USER_ID)).isEqualTo("1001");
            // 校验是否注入了 X-OTA-Username
            assertThat(ex.getRequest().getHeaders().getFirst(CommonConstant.HEADER_USERNAME)).isEqualTo("alice");
            return Mono.empty();
        };

        // 阻塞等待响应式异步过滤流执行完成
        filter.filter(exchange, chain).block();

        // 断言下游过滤器链被正常触发
        assertThat(called).isTrue();
    }

    /**
     * 生成测试专用的合法 HMAC-SHA256 JWT Token。
     * <p>
     * 流式构建逻辑：
     * <ol>
     *   <li>调用 {@code Jwts.builder()} 初始化流式构建器；</li>
     *   <li>注入自定义 Claims（userId, username）；</li>
     *   <li>指定 JTI、Subject、签发时间与过期时间；</li>
     *   <li>使用密钥执行签名并压缩成紧凑 URL 安全字符串。</li>
     * </ol>
     *
     * @param userId   模拟用户主键 ID
     * @param username 模拟用户名
     * @return 紧凑格式的 JWT 字符串
     */
    private String generateToken(Long userId, String username) {
        long now = System.currentTimeMillis();
        return Jwts.builder()
                .claims(Map.of("userId", userId, "username", username))
                .id(UUID.randomUUID().toString())
                .subject(String.valueOf(userId))
                .issuedAt(new Date(now))
                .expiration(new Date(now + CommonConstant.JWT_EXPIRATION * 1000))
                .signWith(Keys.hmacShaKeyFor(CommonConstant.JWT_SECRET.getBytes()))
                .compact();
    }
}
```

### 8.2.3 价库与库存服务状态机单元测试

验证库存冻结、扣减与释放的原子操作及流水生成：

```java
package com.ota.cloud.inventory.service;

import com.ota.cloud.inventory.cache.HotKeyClickRecorder;
import com.ota.cloud.inventory.dto.InventoryOperateDTO;
import com.ota.cloud.inventory.entity.InventoryPriceStock;
import com.ota.cloud.inventory.entity.InventoryRecord;
import com.ota.cloud.inventory.mapper.InventoryPriceStockMapper;
import com.ota.cloud.inventory.mapper.InventoryRecordMapper;
import com.ota.cloud.inventory.metrics.InventoryMetrics;
import com.ota.cloud.inventory.vo.InventoryResultVO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;
import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class InventoryServiceTest {

    private InventoryPriceStockMapper priceStockMapper;
    private InventoryRecordMapper recordMapper;
    private InventoryCacheService cacheService;
    private ChannelPricingService channelPricingService;
    private InventoryMetrics inventoryMetrics;
    private HotKeyClickRecorder hotKeyClickRecorder;
    private InventoryService inventoryService;

    @BeforeEach
    void setUp() {
        priceStockMapper = mock(InventoryPriceStockMapper.class);
        recordMapper = mock(InventoryRecordMapper.class);
        cacheService = mock(InventoryCacheService.class);
        channelPricingService = mock(ChannelPricingService.class);
        when(channelPricingService.queryEffectiveCoe(any(), any(), any())).thenReturn(BigDecimal.ZERO);
        when(channelPricingService.applyChannelCoe(any(), any())).thenAnswer(inv -> inv.getArgument(0));
        inventoryMetrics = mock(InventoryMetrics.class);
        hotKeyClickRecorder = mock(HotKeyClickRecorder.class);
        inventoryService = new InventoryService(priceStockMapper, recordMapper, cacheService, channelPricingService, inventoryMetrics, hotKeyClickRecorder);
    }

    /**
     * 测试跨多晚订单库存冻结成功场景：
     * 1. 模拟入离跨度为 2 晚的订单（6月1日与6月2日）；
     * 2. 准备每晚充足的待售库存并模拟乐观锁 CAS 冻结成功；
     * 3. 验证返回结果成功，并使用 AssertJ 流式提取（extracting）断言每晚日期；
     * 4. 验证缓存失效触发及流水表落库状态。
     */
    @Test
    void freezeUpdatesEveryStayNightAndWritesRecords() {
        // 构造连续入住 2 晚的库存操作 DTO（数量为 1）
        InventoryOperateDTO dto = operateDto("ORDER001", 1);
        LocalDate firstNight = LocalDate.of(2026, 6, 1);
        LocalDate secondNight = LocalDate.of(2026, 6, 2);

        // 模拟两晚库存充足（available=30, frozen=0, version=0）
        when(priceStockMapper.selectByUniqueKey(1L, "D", firstNight)).thenReturn(stock(101L, firstNight, 30, 0, 0, 0));
        when(priceStockMapper.selectByUniqueKey(1L, "D", secondNight)).thenReturn(stock(102L, secondNight, 30, 0, 0, 0));
        // 模拟数据库乐观锁更新命中 1 行（CAS 成功）
        when(priceStockMapper.freezeById(101L, 0, 1)).thenReturn(1);
        when(priceStockMapper.freezeById(102L, 0, 1)).thenReturn(1);

        // 执行冻结操作
        InventoryResultVO result = inventoryService.freeze(dto);

        // 验证整体操作成功
        assertThat(result.isSuccess()).isTrue();

        // 流式断言解析：
        // 1. result.getDetails() 获取每日处理结果集合；
        // 2. extracting(DateResult::getBizDate)：利用方法引用执行流式属性提取投影，将 List<DateResult> 映射为日期流；
        // 3. containsExactly(...)：严格按顺序断言提取出来的日期集合精确匹配入住的两晚。
        assertThat(result.getDetails()).extracting(InventoryResultVO.DateResult::getBizDate)
                .containsExactly(firstNight, secondNight);

        // 验证针对两晚分别触发了 CAS 乐观锁扣减与 Redis 缓存淘汰
        verify(priceStockMapper).freezeById(101L, 0, 1);
        verify(priceStockMapper).freezeById(102L, 0, 1);
        verify(cacheService).evict("inventory:price-stock:1:D:2026-06-01");
        verify(cacheService).evict("inventory:price-stock:1:D:2026-06-02");

        // 捕获写入库存流水表的 InventoryRecord 实体
        ArgumentCaptor<InventoryRecord> recordCaptor = ArgumentCaptor.forClass(InventoryRecord.class);
        verify(recordMapper, org.mockito.Mockito.times(2)).insertRecord(recordCaptor.capture());

        // 流式断言解析：
        // 1. recordCaptor.getAllValues() 获取两次插入流水的实体集合；
        // 2. extracting(InventoryRecord::getStatus)：流式映射提取流水状态；
        // 3. containsExactly("FROZEN", "FROZEN")：严格断言两笔流水均为 FROZEN 冻结状态。
        assertThat(recordCaptor.getAllValues()).extracting(InventoryRecord::getStatus)
                .containsExactly("FROZEN", "FROZEN");
    }

    /**
     * 测试可售库存不足时冻结失败场景：
     * 1. 模拟第一晚可用库存仅剩 1 间，而订单请求预占 2 间；
     * 2. 验证业务立即失败并返回友好提示；
     * 3. 严格验证未执行任何数据库更新或流水记录插入（避免脏数据产生）。
     */
    @Test
    void freezeFailsWithoutWritingWhenAvailableStockIsInsufficient() {
        // 请求预占 2 间房
        InventoryOperateDTO dto = operateDto("ORDER002", 2);
        LocalDate firstNight = LocalDate.of(2026, 6, 1);
        // 第一晚可用库存仅有 1 间，触发库存不足
        when(priceStockMapper.selectByUniqueKey(1L, "D", firstNight)).thenReturn(stock(101L, firstNight, 1, 0, 0, 0));

        // 执行冻结操作
        InventoryResultVO result = inventoryService.freeze(dto);

        // 断言业务失败
        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getDetails()).hasSize(2);
        // 断言失败首日包含明确的“库存不足”错误文案
        assertThat(result.getDetails().getFirst().getMessage()).contains("库存不足");

        // 验证从未调用过 CAS 冻结语句，也未曾插入任何流水记录
        verify(priceStockMapper, never()).freezeById(any(), any(), any());
        verify(recordMapper, never()).insertRecord(any(InventoryRecord.class));
    }

    /**
     * 构造用于测试的库存操作 DTO 对象。
     *
     * @param orderId  关联的模拟订单 ID
     * @param quantity 预订房间数量
     * @return 组装完成的 InventoryOperateDTO 实例
     */
    private InventoryOperateDTO operateDto(String orderId, int quantity) {
        InventoryOperateDTO dto = new InventoryOperateDTO();
        dto.setOrderId(orderId);
        dto.setHotelId(1L);
        dto.setRoomType("D");
        dto.setStartDate(LocalDate.of(2026, 6, 1));
        dto.setEndDate(LocalDate.of(2026, 6, 3));
        dto.setQuantity(quantity);
        return dto;
    }

    /**
     * 构造用于测试的单日库存价格实体。
     *
     * @param id        记录主键 ID
     * @param date      营业日
     * @param available 可用库存
     * @param frozen    已冻结库存
     * @param booked    实销库存
     * @param version   CAS 乐观锁版本号
     * @return 完整的 InventoryPriceStock 实体实例
     */
    private InventoryPriceStock stock(Long id, LocalDate date, int available, int frozen, int booked, int version) {
        InventoryPriceStock stock = new InventoryPriceStock();
        stock.setId(id);
        stock.setHotelId(1L);
        stock.setRoomType("D");
        stock.setBizDate(date);
        stock.setSellPrice(BigDecimal.valueOf(88800, 2));
        stock.setCostPrice(BigDecimal.valueOf(60000, 2));
        stock.setCurrency("CNY");
        stock.setTotal(30);
        stock.setAvailable(available);
        stock.setFrozen(frozen);
        stock.setBooked(booked);
        stock.setVersion(version);
        stock.setSourceType("GROUP");
        stock.setSourceCode("huazhu");
        stock.setSourceRefId("seed");
        return stock;
    }
}
```

---

## 8.3 分批并发规避策略与执行命令

由于微服务数量达到 10 个以上，一次性执行 `mvn test` 会在本地并发启动数十个 Spring 容器，引发 JVM Metaspace 溢出、内嵌 H2/Redis 端口冲突。因此建立严格的**按层分批执行规范**：

```bash
# 1. 认证鉴权基础层（网关路由 + 认证 + 用户）
mvn test -pl ota-gateway,ota-auth,ota-user

# 2. 交易数据支撑层（酒店缓存 + 库存乐观锁 + AI 降级）
mvn test -pl ota-hotel,ota-inventory,ota-ai

# 3. 核心交易与事件驱动层（订单状态机 + Outbox 发件箱）
mvn test -pl ota-order
```

---

## 8.4 真实环境 Smoke 自动化脚本解析

为确保系统在连接真实 MySQL 8.0、Redis 7.0 和 Kafka 集群时能完全闭环，编写了端到端冒烟脚本 `scripts/chapter8-real-smoke.sh`。

### 8.4.1 冒烟执行流程

```
[步骤 1: GET /api/auth/health] ──> 健康检查
  ↓
[步骤 2: POST /api/auth/login] ──> 获取 Bearer JWT Token
  ↓
[步骤 3: GET /api/user/info] ──> 校验 Gateway 鉴权与 Header 透传
  ↓
[步骤 4: GET /api/hotel/1] ──> 验证酒店与房型静态数据
  ↓
[步骤 5: GET /api/inventory/price/query] ──> 验证当前可售房量足够
  ↓
[步骤 6: POST /api/order/create] ──> 创建订单并预占库存 (WAIT_PAY)
  ↓
[步骤 7: POST /api/order/pay] ──> 支付订单 (PAID) 并扣减库存
  ↓
[步骤 8: 校验 MySQL outbox_event] ──> 验证 order-paid 事件已标记为 SENT
  ↓
[步骤 9: 校验 inventory_record] ──> 校验冻结与扣减两条流水落库
```

### 8.4.2 冒烟执行与环境变量配置

```bash
# 默认指向标准网关与测试账号执行冒烟
bash scripts/chapter8-real-smoke.sh

# 自定义网关地址与房型参数执行
GATEWAY_URL="http://127.0.0.1:8080" \
SMOKE_HOTEL_ID="1" \
SMOKE_ROOM_TYPE="D" \
SMOKE_RUN_PAYMENT="1" \
bash scripts/chapter8-real-smoke.sh
```

---

## 8.5 真实环境排障与避坑复盘

在实际部署与冒烟测试中收敛的典型微服务生产问题如下：

1. **Spring Cloud Gateway 缺失 LoadBalancer 依赖**：
   - *问题表现*：网关配置 `lb://ota-order` 报 `503 Service Unavailable`。
   - *解决方案*：在 `ota-gateway/pom.xml` 中引入 `spring-cloud-starter-loadbalancer`。
2. **多网卡环境 Nacos 注册 IP 偏移**：
   - *问题表现*：服务启动后将 Docker 内部虚拟网卡 IP（如 `172.17.0.1`）注册到 Nacos，导致宿主机网关无法寻址。
   - *解决方案*：在服务配置文件中配置 `spring.cloud.nacos.discovery.ip` 或启用网卡正则匹配 `spring.cloud.inetutils.preferred-networks`。
3. **OpenFeign 跨服务调用缺失请求头**：
   - *问题表现*：网关向 `ota-order` 注入了 `X-OTA-UserId`，但 `ota-order` 通过 Feign 调用 `ota-inventory` 时 Header 丢失。
   - *解决方案*：在 `ota-common` 中提供 `FeignHeaderInterceptor`，从 `RequestContextHolder` 抓取请求头并自动中继转发至下游 Feign 请求。
4. **MyBatis 字段映射别名问题**：
   - *问题表现*：库存表 `room_type_code` 在实体类映射为 `roomType` 导致查库为空。
   - *解决方案*：严格对齐 Mapper XML 查询 SQL，显式声明 `room_type_code AS room_type`，杜绝字段映射遗漏。

