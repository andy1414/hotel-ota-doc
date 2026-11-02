# 第15章：Seata AT 模式生产级分布式事务实战演练

> **定位**：在微服务架构下解决跨库强一致性难题。本章以“订单创建 + 远程价库库存预占”为业务场景，落地基于 **Apache Seata 2.6.x AT 模式**的两阶段提交（2PC）分布式全局事务，实现数据变更前置/后置镜像解析、`undo_log` 反向补偿与异常回滚机制，并彻底厘清 OTA 真实生产中“Seata 强一致性 vs. 最终一致性与外部补偿”的架构取舍。

---

## 一、业务背景与架构选型思考

### 1. 为什么需要分布式事务演练？
在 OTA 交易链路中，用户提交订单需要同时触达两大核心域：
1. **订单域（ota-order）**：在本地数据库生成待支付订单（`orders` 表状态为 `WAIT_PAY`）；
2. **库存域（ota-inventory）**：在库存数据库冻结对应间夜的库存（`inventory_price_stock` 表 `available - 1, frozen + 1`）。

在分布式环境下，若订单服务落库成功，但调用远程库存服务发生网络超时，或创单后续逻辑发生严重业务异常，传统的 Spring 本地事务 `@Transactional` 只能回滚本地订单库，无法驱动远端库存微服务撤销冻结，导致出现“幽灵预占”漏洞。

### 2. Seata AT 模式 vs. Outbox 最终一致性 vs. 外部供应方补偿
老系统（`otaapi`）与生产实践的重要原则：
- **平台内部微服务（同构 MySQL 库）**：采用 **Seata AT 模式**，通过代理数据源与 `undo_log` 自动实现跨库原子回滚，业务无侵入；
- **异步广播与下游通知**：采用 **Transactional Outbox + Kafka**，确保支付事件解耦投递；
- **外部酒店集团/供应商（华住、锦江等外部 API）**：**绝对不可纳入 Seata/XA 事务**！外部第三方无法被平台 TC 协调，必须由状态机与定时补偿任务执行反向 API 取消（`cancelBooking`）。

---

## 二、Seata AT 核心原理与两阶段提交（2PC）

```
                     ┌───────────────────────────────────┐
                     │   Seata TC (Transaction Coord)    │
                     │   全局事务协调者 (8091 端口)        │
                     └────────▲─────────────────▲────────┘
                              │                 │
                (1. 注册分支事务)                 (4. 协调全局二阶段提交/回滚)
                              │                 │
              ┌───────────────┴─────────────────┴───────────────┐
              │                                                 │
    【TM / RM: ota-order】                             【RM: ota-inventory】
    @GlobalTransactional                               远程 Feign 调用 (带 TX_XID)
    1. 代理数据源解析 SQL                               1. 拦截器提取并绑定 RootContext XID
    2. 生成 Before-Image 与 After-Image                 2. 解析 update 语句并生成镜像
    3. 写入 orders 表与 undo_log 表                     3. 写入 inventory 表与 undo_log 表
    4. 本地事务提交并释放锁                             4. 本地事务提交并释放锁
```

- **Phase 1（执行与记录）**：各微服务执行业务 SQL，Seata 自动提取前后镜像写入各库 `undo_log` 表，立即提交本地事务释放物理数据库锁，上报分支结果给 TC；
- **Phase 2（正常提交）**：若全局全部分支均成功，TC 异步通知各 RM 清理 `undo_log` 记录，零回滚开销；
- **Phase 2（异常回滚）**：若任一分支失败，TC 驱动各 RM 读取 `undo_log` 中的 Before-Image，生成逆向补偿 SQL 并更新数据库，恢复数据原始状态。

---

## 三、核心落地改造

### 1. Feign 跨微服务 XID 透传拦截器（ota-order）
```java
@Configuration
public class SeataXidFeignRequestInterceptor implements RequestInterceptor {
    @Override
    public void apply(RequestTemplate template) {
        String xid = RootContext.getXID();
        if (StringUtils.hasText(xid)) {
            template.header(RootContext.KEY_XID, xid);
        }
    }
}
```

### 2. 下游 RM 接收端 XID 自动绑定（ota-inventory）
```java
public class SeataXidHandlerInterceptor implements HandlerInterceptor {
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        String xid = request.getHeader(RootContext.KEY_XID);
        if (StringUtils.hasText(xid) && !StringUtils.hasText(RootContext.getXID())) {
            RootContext.bind(xid);
        }
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) {
        RootContext.unbind();
    }
}
```

### 3. 全局事务演示服务（OrderSeataService）
在 `ota-order` 中开启 `@GlobalTransactional`，并提供 `simulateError` 演练参数控制回滚：
```java
@Service
@RequiredArgsConstructor
public class OrderSeataService {

    private final OrderMapper orderMapper;
    private final InventoryFeignClient inventoryFeignClient;

    @GlobalTransactional(name = "ota-seata-create-order", rollbackFor = Exception.class)
    public OrderSeataResultVO createOrderWithGlobalTx(OrderSeataCreateDTO dto) {
        String xid = RootContext.getXID();

        // 1. 本地分支订单插入（遵循 Mapper XML 规范）
        orderMapper.insertOrder(order);

        // 2. 远程 Feign 冻结库存（跨进程自动携带 TX_XID）
        inventoryFeignClient.freeze(operateDTO);

        // 3. 模拟异常触发全局回滚
        if (Boolean.TRUE.equals(dto.getSimulateError())) {
            throw new BusinessException(BusinessException.ErrorCode.INTERNAL_ERROR, "触发 Seata AT 全局回滚演练异常");
        }

        return OrderSeataResultVO.builder().orderId(orderId).xid(xid).success(true).build();
    }
}
```

---

## 四、验证与演练

1. **单元与集成测试**：
   - `OrderSeataServiceTest`：覆盖提交流程、模拟异常回滚流程、远程失败回滚与非法参数拦截；
   - `SeataXidFeignRequestInterceptorTest`：验证 XID 请求头透传；
   - `SeataXidHandlerInterceptorTest`：验证 downstream 线程绑定与释放。
2. **自动化冒烟验证**：
   - 执行脚本：`bash scripts/chapter15-seata-smoke.sh`。
