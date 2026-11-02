# ota-cloud Spring Cloud Gateway 全景架构与生产实践指南

> **定位**：系统性梳理 `ota-cloud` 中基于 Spring Cloud Gateway（响应式 WebFlux 体系）作为**微服务统一边界与安全路由枢纽**的架构设计、动态路由、JWT 全局认证鉴权、全链路 TraceId 注入与跨域高可用治理。

---

## 一、架构定位与设计模式：API Gateway Pattern

在复杂的 OTA 微服务分布式集群中，前端客户端（Web 演示站、小程序、商家中台）不应直接暴露访问数十个内部服务实例：
- **安全边界**：将鉴权、防重放、加解密等非业务逻辑前置收敛，内部微服务只聚焦领域逻辑；
- **协议剥离与映射**：统一外部访问前缀（如 `/api/hotel/**`、`/api/merchant/**`），通过 `StripPrefix=1` 动态解耦外部契约与微服务内部上下文路径；
- **全局链路染色**：进入网关的第一时间生成全局唯一的 `traceId`，挂载请求头透传给所有下游微服务与 ELK 日志。

```
【客户端请求 (Web / H5 / 渠道商)】
              │
              ▼ :8080
┌─────────────────────────────────────────────────────────┐
│               Spring Cloud Gateway 核心流水线           │
│                                                         │
│  1. TraceIdFilter (全局请求染色，注入 X-Trace-Id)        │
│  2. GlobalCorsFilter (跨域白名单与响应头收敛去重)         │
│  3. SentinelGatewayFilter (全局流量整形与突发洪峰防护)    │
│  4. AuthGlobalFilter (白名单放行 / JWT 解析 / 角色鉴权)  │
│  5. ReactiveLoadBalancerClientFilter (基于 Nacos 动态寻址)│
└───────────────────────────┬─────────────────────────────┘
                            │ lb://<service-name> (StripPrefix=1)
       ┌────────────────────┼────────────────────┐
       ▼                    ▼                    ▼
【ota-auth:8081】     【ota-hotel:8083】   【ota-merchant:8086】
```

---

## 二、微服务拓扑与角色分工

1. **`ota-gateway`（服务端口 8080）**：
   - **对外唯一暴露端口**：所有流量必须通过 8080 进入，禁止外部客户端直连底层业务端口（8081~8090）；
   - **动态路由与负载均衡**：通过 `lb://` 协议借助 Nacos 实现基于服务实例健康状态的权重负载均衡；
   - **商家中心与平台隔离**：严格匹配 `/api/merchant/**` 并安全转发至商家中心（`ota-merchant`）。

---

## 三、核心路由拓扑矩阵

网关在 `application.yml` 中定义的完整核心路由矩阵如下：

| Route ID            | 外部匹配路径 (Path)       | 目标服务 (URI)           | 过滤器动作           | 权限要求            |
| :------------------ | :------------------ | :------------------- | :-------------- | :-------------- |
| **`ota-auth`**      | `/api/auth/**`      | `lb://ota-auth`      | `StripPrefix=1` | 白名单放行登录与注册      |
| **`ota-user`**      | `/api/user/**`      | `lb://ota-user`      | `StripPrefix=1` | JWT 校验（普通用户及以上） |
| **`ota-hotel`**     | `/api/hotel/**`     | `lb://ota-hotel`     | `StripPrefix=1` | 公开查询放行 / 变更鉴权   |
| **`ota-inventory`** | `/api/inventory/**` | `lb://ota-inventory` | `StripPrefix=1` | 严格校验 Token      |
| **`ota-order`**     | `/api/order/**`     | `lb://ota-order`     | `StripPrefix=1` | 绑定当前登录用户的 UID   |
| **`ota-merchant`**  | `/api/merchant/**`  | `lb://ota-merchant`  | `StripPrefix=1` | 强校验 MERCHANT 身份 |
| **`ota-admin`**     | `/api/admin/**`     | `lb://ota-admin`     | `StripPrefix=1` | 强校验 ADMIN 超管权限  |
| **`ota-supplier`**  | `/api/supplier/**`  | `lb://ota-supplier`  | `StripPrefix=1` | 内部直连或供应商鉴权      |
| **`ota-channel`**   | `/api/channel/**`   | `lb://ota-channel`   | `StripPrefix=1` | 渠道分销独立通道        |
| **`ota-ai`**        | `/api/ai/**`        | `lb://ota-ai`        | `StripPrefix=1` | 智能助手会话路由        |

---

## 四、关键落地技术细节

### 1. 全链路 TraceId 前置生成与染色（TraceIdFilter）

在网关层使用 WebFlux 过滤器拦截所有进入请求，若客户端未携带 `X-Trace-Id` 则生成 32 位 Hex UUID，并将其分别向下游请求头（Request Header）与客户端响应头（Response Header）中透传：

```java
package com.ota.cloud.gateway.filter;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Component
public class TraceIdFilter implements GlobalFilter, Ordered {

    public static final String TRACE_ID_HEADER = "X-Trace-Id";

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        ServerHttpRequest request = exchange.getRequest();
        String traceId = request.getHeaders().getFirst(TRACE_ID_HEADER);

        if (traceId == null || traceId.isBlank()) {
            traceId = UUID.randomUUID().toString().replace("-", "");
        }

        // 重新构建 Request Header 传递给下游
        ServerHttpRequest mutatedRequest = request.mutate()
                .header(TRACE_ID_HEADER, traceId)
                .build();

        // 响应头回填客户端
        exchange.getResponse().getHeaders().add(TRACE_ID_HEADER, traceId);

        return chain.filter(exchange.mutate().request(mutatedRequest).build());
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE; // 最高优先级执行，确保所有后续过滤器均可获取 TraceId
    }
}
```

### 2. 响应式 JWT 认证与身份透传（AuthGlobalFilter）

由于 Gateway 运行在 Netty 反应式编程模型上，不可使用 `ThreadLocal`，需直接在请求头中回填已解析的用户 ID、角色信息（`X-User-Id`、`X-User-Role`）：

```java
package com.ota.cloud.gateway.filter;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Slf4j
@Component
public class AuthGlobalFilter implements GlobalFilter, Ordered {

    @Value("${gateway.white-list.paths:}")
    private List<String> whiteListPaths;

    @Value("${jwt.secret}")
    private String jwtSecret;

    private final AntPathMatcher pathMatcher = new AntPathMatcher();

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        ServerHttpRequest request = exchange.getRequest();
        String path = request.getURI().getPath();

        // 1. 白名单免密放行检测
        for (String whitePath : whiteListPaths) {
            if (pathMatcher.match(whitePath, path)) {
                return chain.filter(exchange);
            }
        }

        // 2. 提取 Authorization Bearer Token
        String authHeader = request.getHeaders().getFirst(HttpHeaders.AUTHORIZATION);
        if (authHeader == null || !authHeader.startsWith("Bearer ")) {
            return unauthorizedResponse(exchange, "未携带认证令牌");
        }

        String token = authHeader.substring(7);
        try {
            SecretKey key = Keys.hmacShaKeyFor(jwtSecret.getBytes(StandardCharsets.UTF_8));
            Claims claims = Jwts.parser()
                    .verifyWith(key)
                    .build()
                    .parseSignedClaims(token)
                    .getPayload();

            String userId = claims.getSubject();
            String role = claims.get("role", String.class);

            // 3. 将验证通过的身份上下文作为安全 Header 透传给内部服务
            ServerHttpRequest mutatedRequest = request.mutate()
                    .header("X-User-Id", userId)
                    .header("X-User-Role", role != null ? role : "USER")
                    .build();

            return chain.filter(exchange.mutate().request(mutatedRequest).build());
        } catch (Exception e) {
            log.warn("网关鉴权令牌校验失败: path={}, error={}", path, e.getMessage());
            return unauthorizedResponse(exchange, "令牌无效或已过期");
        }
    }

    private Mono<Void> unauthorizedResponse(ServerWebExchange exchange, String message) {
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(HttpStatus.UNAUTHORIZED);
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        String body = "{\"code\":401,\"message\":\"" + message + "\",\"data\":null}";
        DataBuffer buffer = response.bufferFactory().wrap(body.getBytes(StandardCharsets.UTF_8));
        return response.writeWith(Mono.just(buffer));
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 10;
    }
}
```

---

## 五、监控指标与运维告警

### 1. Prometheus 业务指标采集
- `gateway_requests_seconds_count`：网关收到的总 HTTP 请求数。
- `gateway_requests_seconds_sum`：网关转发耗时总量。
- `gateway_status`（200 / 401 / 500 / 503）：网关各状态码分布。

### 2. 网关故障排查排查清单（Troubleshooting）
- **503 Service Unavailable**：目标微服务尚未在 Nacos 注册成功，或网关与 Nacos 的通信元数据未同步。
- **401 Unauthorized**：客户端请求 Header 中缺失 `Authorization: Bearer <token>` 或 Token 已经超过 2 小时有效期。
- **CORS 跨域失败**：需在 `application.yml` 的 `spring.cloud.gateway.globalcors` 声明跨域策略，并开启 `DedupeResponseHeader=Access-Control-Allow-Origin` 防止重复返回导致浏览器报错。

---

## 六、基础设施与运维部署

- **运行模式**：基于 Netty 反应式无阻塞架构，内存占用低（默认分配 256m~512m 堆内存）。
- **标准启动入口**：
  ```bash
  mvn spring-boot:run -pl ota-gateway
  ```
- **健康检查**：`GET http://localhost:8080/actuator/health` 检查网关与下层组件状态。
