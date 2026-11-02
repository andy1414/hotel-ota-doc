# ota-cloud Redis 分布式缓存与多级缓存全景架构指南

> **定位**：系统性梳理 `ota-cloud` 中基于 Redis 7.x 与 Caffeine 的**高性能分布式缓存与多级缓存体系**。深入剖析 Cache-Aside 读写模式、Redis 分布式互斥锁、缓存三大典型问题（穿透、击穿、雪崩）的工业级工程防御、Lua 脚本原子操作以及热点数据预热机制。

---

## 一、架构定位与设计模式：Cache-Aside & Multi-Level Cache

在 OTA 酒店预订业务中，详情读请求量通常是下单写请求的 50~100 倍。若所有查询均直接穿透到底层 MySQL，数据库连接池瞬间耗尽并引发级联宕机：
- **Cache-Aside 旁路缓存模式**：业务优先读取缓存，未命中时回源查库并回填缓存；数据更新时采用“先写数据库、再淘汰缓存”策略；
- **防穿透（Cache Penetration）**：对数据库中不存在的非法 ID（如不存在的酒店 ID: 999999）写入标记为 `"NULL"` 的短寿命缓存（60s），阻断恶意请求；
- **防击穿（Cache Breakdown）**：针对突发过期的爆款热点 Key，采用 `setIfAbsent` 互斥锁（Mutex Lock）保证同一时刻仅 1 个线程回源查库，其余线程自旋等待后重读缓存；
- **防雪崩（Cache Avalanche）**：基础过期时间（如 30 分钟）叠加动态随机抖动秒数（0~300s），避免大批量 Key 在同一秒集体失效；
- **多级缓存（Multi-Level Cache）**：本地进程内 JVM 缓存（Caffeine L1，纳秒级响应，抗热点单点洪峰）+ 分布式缓存（Redis L2，跨服务共享状态）。

```
【客户端 HTTP 查询请求】
          │
          ▼
┌──────────────────────────────────────────────┐
│           L1 本地缓存 (Caffeine)             │  命中率 ~70% (极速，无网络 I/O 开销)
└─────────────────────┬────────────────────────┘
                      │ (未命中)
                      ▼
┌──────────────────────────────────────────────┐
│           L2 分布式缓存 (Redis 7.x)          │  命中率 ~25% (单次网络 RTT ~1-2ms)
└─────────────────────┬────────────────────────┘
                      │ (未命中)
         ┌────────────┴────────────┐
         │ 抢占 Redis 分布式互斥锁  │
         │ (SETNX lock:hotel:101)  │
         └────────────┬────────────┘
                      │ 仅 1 个线程抢到锁回源查库
                      ▼
┌──────────────────────────────────────────────┐
│           MySQL 关系型持久化存储              │  回源率 < 5% (高可靠持久化)
└──────────────────────────────────────────────┘
```

---

## 二、微服务拓扑与角色分工

1. **`ota-hotel`（酒店中心，端口 8083）**：
   - 核心读场景：酒店基本信息、房型列表详情、设施标签；
   - 实现了工业级 `CacheService` 统一抽象层，涵盖锁争抢、自旋重试与回填逻辑。
2. **`ota-inventory`（价库与库存中心，端口 8084）**：
   - 读高频：日历房态、实时加价后可售状态缓存；
   - 配合分布式锁与乐观锁保证库存扣减前后数据一致性。
3. **`ota-order`（订单中心，端口 8085）**：
   - 订单详情短暂只读缓存，提高用户支付结果刷新体验；防重复提交 Token。

---

## 三、核心 Key 设计与过期策略矩阵

| 业务场景 | Redis Key 格式 | 数据结构 | TTL 过期策略 | 防御机制 |
| :--- | :--- | :--- | :--- | :--- |
| **酒店基础详情** | `hotel:detail:{hotelId}` | String (JSON) | 30m + Random(0~300s) | 防穿透 (空值60s) + 防雪崩 |
| **酒店房型列表** | `hotel:rooms:{hotelId}` | String (JSON) | 30m + Random(0~300s) | 互斥锁回源 (Lock) |
| **互斥回源锁** | `lock:{cacheKey}` | String (nanoTime) | 2000ms (自动安全释放) | 防止查库异常锁死 |
| **防重复下单 Token** | `order:token:{uid}:{token}` | String | 5m | 提交订单时原子校验并删除 |
| **热点房态预热** | `inventory:daily:{hotelId}:{date}` | Hash | 10m + 动态抖动 | 热点数据启动预热 |

---

## 四、关键落地技术细节

### 1. 工业级 Cache-Aside 防穿透/击穿/雪崩服务（`CacheService.java`）

```java
package com.ota.cloud.hotel.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Slf4j
@Service
@RequiredArgsConstructor
public class CacheService {

    static final String NULL_CACHE = "NULL";
    private static final String LOCK_PREFIX = "lock:";
    private static final long DEFAULT_TTL_MINUTES = 30L;
    private static final long MAX_JITTER_SECONDS = 300L;
    private static final long NULL_TTL_SECONDS = 60L;
    private static final long LOCK_TIMEOUT_MS = 2000L;
    private static final int SPIN_RETRY_TIMES = 3;
    private static final long SPIN_SLEEP_MS = 200L;

    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;

    /**
     * Cache-Aside 防御式查询骨架
     */
    public <T> T queryWithCacheAside(String key, Class<T> type, Supplier<T> dbLoader, Long ttlMinutes) {
        // 1. 查缓存
        Object cached = redisTemplate.opsForValue().get(key);
        if (cached != null) {
            return readCachedValue(key, cached, type);
        }

        // 2. 缓存未命中，抢占分布式互斥锁防击穿
        String lockKey = LOCK_PREFIX + key;
        String lockValue = String.valueOf(System.nanoTime());
        boolean lockOwned = false;

        try {
            Boolean locked = redisTemplate.opsForValue()
                    .setIfAbsent(lockKey, lockValue, LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            lockOwned = Boolean.TRUE.equals(locked);

            if (lockOwned) {
                // Double-Check 双重检查，防止排队等待期间前序线程已回填
                cached = redisTemplate.opsForValue().get(key);
                if (cached != null) {
                    return readCachedValue(key, cached, type);
                }
                return loadAndCache(key, dbLoader, ttlMinutes);
            }

            // 3. 未抢到锁的线程自旋重试读取
            for (int i = 0; i < SPIN_RETRY_TIMES; i++) {
                sleepQuietly();
                cached = redisTemplate.opsForValue().get(key);
                if (cached != null) {
                    return readCachedValue(key, cached, type);
                }
            }
            log.warn("[Cache] 互斥锁等待超时，兜底查库: key={}", key);
            return dbLoader.get();
        } finally {
            if (lockOwned) {
                releaseLock(lockKey, lockValue);
            }
        }
    }

    private <T> T loadAndCache(String key, Supplier<T> dbLoader, Long ttlMinutes) {
        T data = dbLoader.get();
        if (data == null) {
            // 防穿透：数据库不存在则存入带短 TTL 的空标记
            redisTemplate.opsForValue().set(key, NULL_CACHE, NULL_TTL_SECONDS, TimeUnit.SECONDS);
            return null;
        }

        // 防雪崩：基础过期时间叠加 0~300 秒随机散列
        long baseMinutes = ttlMinutes != null ? ttlMinutes : DEFAULT_TTL_MINUTES;
        long jitterSeconds = ThreadLocalRandom.current().nextLong(MAX_JITTER_SECONDS + 1);
        long totalSeconds = TimeUnit.MINUTES.toSeconds(baseMinutes) + jitterSeconds;

        redisTemplate.opsForValue().set(key, data, totalSeconds, TimeUnit.SECONDS);
        return data;
    }

    /**
     * 基于 Lua 脚本的原子锁释放（防止误删其他线程同名锁）
     */
    private void releaseLock(String lockKey, String lockValue) {
        String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
        redisTemplate.execute(new DefaultRedisScript<>(script, Long.class),
                Collections.singletonList(lockKey), lockValue);
    }

    private void sleepQuietly() {
        try {
            Thread.sleep(SPIN_SLEEP_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
```

---

## 五、监控指标与运维排查

### 1. Prometheus 业务指标采集
- `redis_cache_hits_total`：缓存命中总次数。
- `redis_cache_misses_total`：缓存未命中总次数。
- `redis_cache_hit_ratio`：缓存命中率（指标期望保持在 95% 以上）。

### 2. 常见故障排查
- **排查大 Key 与热 Key**：
  ```bash
  redis-cli -a redis123456 --bigkeys
  redis-cli -a redis123456 --hotkeys
  ```
- **内存碎片率监控**：关注 `info memory` 中的 `mem_fragmentation_ratio`，超过 1.5 需手动整理碎片。

---

## 六、基础设施与运维部署

- **镜像与版本**：`redis:7`（配置 `AOF` 持久化与强密码）。
- **Docker 容器配置**：
  ```yaml
  redis:
    image: redis:7
    container_name: ota-redis
    restart: unless-stopped
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
    command: redis-server --requirepass redis123456 --appendonly yes
  ```
- **健康检查**：`redis-cli -a redis123456 ping` 返回 `PONG`。
