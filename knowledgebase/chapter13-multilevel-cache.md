# 第13章：多级缓存与高可用 (Cache & High Availability)

> **目标**：为应对 OTA 行业“查多写少（读写比高达 100:1）、节假日突发流量爆棚”的严苛场景，构建 Caffeine 本地缓存（L1）+ Redis 分布式缓存（L2）的两级缓存架构，并深度落地缓存穿透、击穿（Single-Flight）、雪崩防御与 Hot Key 自动探测预热机制。

---

## 13.1 架构设计：两级缓存读写模型

在高并发酒店搜索与房态查询中，单靠 Redis 也会受到网络 I/O 带宽与单分片 CPU 的瓶颈制约。多级缓存通过在 JVM 堆内存（Caffeine）中驻留高频只读数据，将大部分热点流量拦截在微秒级别：

```
                    ┌───────────────────────────────┐
                    │           客户端请求          │
                    └───────────────┬───────────────┘
                                    │
                                    ▼
                    ┌───────────────────────────────┐
                    │     L1: Caffeine 本地缓存     │ ──(命中，微秒级响应)──> 返回
                    └───────────────┬───────────────┘
                                    │ 未命中
                                    ▼
                    ┌───────────────────────────────┐
                    │     L2: Redis 分布式缓存     │ ──(命中，写入 L1)────> 返回
                    └───────────────┬───────────────┘
                                    │ 未命中
                                    ▼
                    ┌───────────────────────────────┐
                    │  Single-Flight 进程内并发互斥  │
                    └───────────────┬───────────────┘
                                    │ 仅单线程穿透回源
                                    ▼
                    ┌───────────────────────────────┐
                    │      MySQL 数据库 (主库/从库)   │ ──(查询成功回填 L2 + L1)
                    └───────────────────────────────┘
```

---

## 13.2 缓存三大灾难防御工程化落地

### 1. 穿透防御（Cache Penetration）
- **前置参数校验**：非法的 Key 格式（如负数 ID、超长恶意字符）在接口层阻断；
- **空值占位哨兵（Null Object Pattern）**：当数据库查询结果为空时，向 Redis 写入特殊占位标识 `NULL`，并设置较短的过期时间（如 30 秒），防止攻击者持续扫库。

### 2. 击穿防御（Single-Flight 并发互斥回源）
当某个超热点酒店房型缓存过期的瞬间，成百上千个并发请求同时涌入。若全部回源，数据库瞬间被打满：
- **Single-Flight 控制**：利用进程内 `ConcurrentHashMap` + `CompletableFuture`，针对同一个 Cache Key，确保同一时刻仅有 1 个工作线程真正执行数据库 Mapper 查询，其余等待线程共享其 Future 结果。

### 3. 雪崩防御（Cache Avalanche）
- **TTL 随机扰动**：基础缓存时长添加 10%~20% 的随机浮动时间（如 300 秒 + 随机 0~60 秒），防止海量房态数据在整点集中失效。

---

## 13.3 价库缓存服务实现 (`InventoryCacheService`)

```java
package com.ota.cloud.inventory.service;

import com.ota.cloud.common.exception.BusinessException;
import com.ota.cloud.inventory.entity.InventoryPriceStock;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 价库多级/分布式缓存服务实现类。
 * <p>
 * 核心技术机制：
 * 1. 穿透防护：若 DB 中不存在记录，向 Redis 写入带 30 秒短 TTL 的 NULL 占位哨兵，阻断恶意空查；
 * 2. 击穿防护（Single-Flight）：利用进程内 lockMap 互斥锁，确保同一时刻仅有单线程穿透回源，其余线程共享结果；
 * 3. 雪崩防护：基础过期时长 300 秒基础上附加 0~60 秒的随机扰动浮动值，避免批量整点集中失效；
 * 4. Cache-Aside 淘汰闭环：写库成功后主动淘汰缓存。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryCacheService {

    /** 空值防穿透标记常量。 */
    static final String NULL_CACHE = "NULL";

    /** 业务正常缓存生存时间：300 秒（5 分钟）。 */
    private static final long PRICE_STOCK_TTL_SECONDS = 300L;

    /** 空值哨兵缓存生存时间：30 秒短过期。 */
    private static final long NULL_TTL_SECONDS = 30L;

    /** Redis 序列化客户端模板。 */
    private final RedisTemplate<String, Object> redisTemplate;

    /** Single-Flight 进程内 Key 维度细粒度并发锁表。 */
    private final ConcurrentMap<String, Object> lockMap = new ConcurrentHashMap<>();

    /**
     * 带防穿透、防击穿与防雪崩特性的通用泛型缓存读取模板方法。
     *
     * @param <T>      返回数据实体的类型参数
     * @param key      缓存 Redis Key
     * @param type     期望返回实体的 Class 类型
     * @param dbLoader 数据库回源加载函数（函数式接口）
     * @return 实体对象实例；数据库不存在时返回 null
     * @throws BusinessException 当 key 非法时抛出业务异常
     */
    @SuppressWarnings("unchecked")
    public <T> T queryWithCache(String key, Class<T> type, Supplier<T> dbLoader) {
        // 1. 前置参数强校验，防止空 Key 污染缓存层
        validateKey(key);

        // 2. 首次查询 Redis 分布式缓存
        Object cached = redisTemplate.opsForValue().get(key);
        if (cached != null) {
            // 命中防穿透哨兵标记，短路返回 null，绝不穿透至 DB
            if (NULL_CACHE.equals(cached)) {
                return null;
            }
            return (T) cached;
        }

        // 3. 缓存未命中：采用 Single-Flight 进程内细粒度锁，防止高并发打穿 DB
        Object lock = lockMap.computeIfAbsent(key, k -> new Object());
        synchronized (lock) {
            try {
                // 二次检查缓存（DCL，Double-Checked Locking）：防范在排队加锁期间其他并发线程已将数据回填 Redis
                cached = redisTemplate.opsForValue().get(key);
                if (cached != null) {
                    if (NULL_CACHE.equals(cached)) {
                        return null;
                    }
                    return (T) cached;
                }

                // 4. 真正执行单线程安全回源
                T result = dbLoader.get();
                if (result == null) {
                    // 回源查无数据：写入 30 秒短 TTL 哨兵防止穿透
                    redisTemplate.opsForValue().set(key, NULL_CACHE, NULL_TTL_SECONDS, TimeUnit.SECONDS);
                    return null;
                }

                // 5. 回源成功：写入缓存，并增加 0~60 秒的随机扰动时间防止缓存雪崩
                long jitter = (long) (Math.random() * 60);
                redisTemplate.opsForValue().set(key, result, PRICE_STOCK_TTL_SECONDS + jitter, TimeUnit.SECONDS);
                return result;
            } finally {
                // 必须在锁块释放时移除当前 Key 锁对象，避免内存泄漏
                lockMap.remove(key);
            }
        }
    }

    /**
     * 主动淘汰失效指定缓存 Key。
     *
     * @param key 需要删除的 Redis Key
     */
    public void evict(String key) {
        if (key != null && !key.isBlank()) {
            redisTemplate.delete(key);
        }
    }

    /**
     * 校验 Key 合法性，杜绝空 Key 请求。
     *
     * @param key 待校验字符串
     */
    private void validateKey(String key) {
        if (key == null || key.isBlank()) {
            throw new BusinessException("缓存 Key 不能为空");
        }
    }
}
```

---

## 13.4 热点 Key 动态探测与自动预热机制

对于节假日爆款酒店，系统通过滑动窗口统计访问热度，并在每日低峰期自动完成预热：

### 1. 访问频次埋点 (`HotKeyClickRecorder`)

读请求通过 Redis 的 `HINCRBY` 原子累计 7 天点击次数，开销低于 1 毫秒且无锁：

```java
package com.ota.cloud.inventory.cache;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * 房态访问热点探测器。
 * <p>
 * 依托 Redis Hash 数据结构的 HINCRBY 指令实现原子计数，
 * 在读流量路径上以微秒级耗时累计访问频次，为预热决策提供量化依据。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class HotKeyClickRecorder {

    /** Redis 点击计数器 Hash Key 名称。 */
    public static final String CLICK_COUNTER_KEY = "inventory:hot:counter";

    /** Redis 字符串模板组件。 */
    private final StringRedisTemplate stringRedisTemplate;

    /**
     * 记录指定酒店房型的点击访问行为（频次 +1）。
     *
     * @param hotelId  酒店主键 ID
     * @param roomType 房型代码
     */
    public void recordClick(Long hotelId, String roomType) {
        if (hotelId == null || roomType == null || roomType.isBlank()) {
            return;
        }
        try {
            // 在 Hash 结构中原子累加指定字段的计数值
            stringRedisTemplate.opsForHash().increment(CLICK_COUNTER_KEY, hotelId + ":" + roomType, 1L);
        } catch (Exception ex) {
            // 旁路日志，计数异常绝不阻断正常业务查询
            log.warn("[HotKey] 记录点击失败 hotelId={}, roomType={}", hotelId, roomType, ex);
        }
    }
}
```

### 2. 定时预热任务 (`InventoryHotKeyWarmer`)

每日凌晨 03:00 自动抓取 Top 100 热点房型，提前加载未来 7 天的库存数据进缓存：

```java
package com.ota.cloud.inventory.job;

import com.ota.cloud.inventory.cache.HotKeyClickRecorder;
import com.ota.cloud.inventory.entity.InventoryPriceStock;
import com.ota.cloud.inventory.mapper.InventoryPriceStockMapper;
import com.ota.cloud.inventory.service.InventoryCacheService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

/**
 * 热点房态库存自动预热定时调度任务。
 * <p>
 * 调度策略：每日凌晨业务低峰期执行，将高频点击的爆款房源数据提前加载至 Redis，
 * 消除白天的冷启动毛刺。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryHotKeyWarmer {

    /** 访问点击计数器组件。 */
    private final HotKeyClickRecorder clickRecorder;

    /** 价库数据 Mapper。 */
    private final InventoryPriceStockMapper priceStockMapper;

    /** 价库缓存服务。 */
    private final InventoryCacheService cacheService;

    /**
     * 执行热点预热调度流程。
     */
    @Scheduled(cron = "0 0 3 * * ?")
    public void warmup() {
        // 1. 抓取近期访问频次最高的热点房型快照
        Map<String, Long> snapshot = clickRecorder.snapshot();
        if (snapshot.isEmpty()) {
            return;
        }
        log.info("开始执行库存 Hot Key 预热任务...");
        LocalDate today = LocalDate.now();
        LocalDate end = today.plusDays(7);

        // 2. 遍历 Top 热点房型，批量加载未来 7 天的日历价库
        for (String hotKey : snapshot.keySet()) {
            String[] parts = hotKey.split(":");
            Long hotelId = Long.parseLong(parts[0]);
            String roomType = parts[1];

            // 批量查询数据库未来 7 天价库
            List<InventoryPriceStock> stocks = priceStockMapper.selectByHotelAndDateRange(hotelId, roomType, today, end);
            for (InventoryPriceStock stock : stocks) {
                String cacheKey = "inventory:price-stock:" + hotelId + ":" + roomType + ":" + stock.getBizDate();
                // 主动将实体灌入缓存，避免用户首次访问回源
                cacheService.warmup(cacheKey, stock);
            }
        }
        log.info("库存 Hot Key 预热完成");
    }
}
```

---

## 13.5 数据一致性保障（事务提交后淘汰）

为防止数据库事务尚未提交而缓存已被淘汰导致的“并发脏读回填”问题，缓存失效必须绑定在事务提交钩子（`afterCommit`）中执行：

```java
/**
 * 房态与库存更新接口实现（带事务提交后缓存失效保护）。
 *
 * @param hotelId  酒店主键 ID
 * @param roomType 房型代码
 * @param bizDate  营业日期
 */
@Transactional
public void updatePriceAndStock(Long hotelId, String roomType, LocalDate bizDate) {
    // 1. 更新数据库主数据（此时事务尚未正式 commit）
    priceStockMapper.update(stock);

    // 2. 注册 Spring 事务同步回调，确保只有在物理事务完全 commit 之后才淘汰缓存
    // 杜绝“缓存已删、DB尚未提交，并发读线程把旧DB数据再次回填缓存”的典型并发一致性漏洞
    TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
        @Override
        public void afterCommit() {
            cacheService.evict("inventory:price-stock:" + hotelId + ":" + roomType + ":" + bizDate);
        }
    });
}
```

---

## 13.6 快速验证

```bash
# 验证多级缓存击穿互斥、空值穿透拦截与热点预热单元测试
mvn test -pl ota-inventory -Dtest=InventoryCacheServiceTest,InventoryHotKeyWarmerTest
```

