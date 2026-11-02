# 第2章：Spring Cloud Gateway 与 Auth 认证鉴权

> **工期估算：** 1天（P0-2）
>
> **目标读者：** 有Java经验的开发者
>
> **本章产出：** 3个可运行的微服务 + 完整的 JWT 认证流程

---

## 2.1 本章目标

- **ota-auth（端口8081）**：Spring Security 6 + JWT + BCrypt，提供登录/校验/登出接口
- **ota-user（端口8082）**：用户注册与信息查询
- **ota-gateway（端口8080）**：路由转发 + 全局 JWT 校验过滤器 + TraceId 透传
- 验证：完整的 curl 测试流程

---

## 2.2 交互流程

```
┌──────────┐     POST /api/auth/login      ┌──────────┐
│          │ ──────────────────────────────► │          │
│   curl   │     ← {"token":"eyJ..."}       │ ota-auth │
│ (客户端)  │                                │  :8081   │
│          │     GET /api/user/info         │          │
│          │ ──────────────────────────────► │          │
│          │     Header: Authorization      └────┬─────┘
│          │          Bearer eyJ...              │
│          │                                     │
│          │     ┌───────────────────────────────┤
│          │     │   ota-gateway                 │
│          │     │   :8080                       │
│          │     │   ├─ 白名单检查                │
│          │     │   ├─ JWT 本地验签+有效期        │
│          │     │   ├─ (必要时调auth/validate)    │
│          │     │   ├─ 透传 userId/username      │
│          │     │   └─ 生成 TraceId             │
│          │     └───────────────────────────────┘
```

**流程详解：**

1. 客户端调用 `POST /api/auth/login`（白名单，网关直接放行）
2. 网关转发到 ota-auth:8081，auth 校验用户名密码，生成 JWT 返回
3. 客户端携带 JWT 调用 `GET /api/user/info`（受保护接口）
4. 网关 `JwtAuthGlobalFilter` 本地解析 JWT（校验签名 + 有效期）
   - 优先本地完成，不做额外 RPC
   - **仅在必要时**（Redis 黑名单检查、权限上下文解析异常时）回退调用 auth 的 `/auth/validate`
5. 校验通过后，网关将 `userId`, `username` 放入请求头转发给下游
6. user 服务从请求头中提取用户信息返回

### 2.2.1 架构要点

#### 🔑 Gateway 本地解析 vs auth/validate 远程调用

| 维度 | 本地解析（Gateway） | 远程调用（auth/validate） |
|------|-------------------|------------------------|
| **做什么** | 验签 + 过期时间（JWT 签名算法 HMAC-SHA256） | 黑名单检查（Redis） + 权限上下文解析 |
| **何时用** | 每次请求必经之路 | **仅在需要时** — 例如首次请求或本地解析异常回退 |
| **性能** | 无网络开销，毫秒级 | 多一次 RTT（< 5ms 内网） |
| **依赖** | 仅需 JWT Secret Key | 依赖 auth 服务可用 |

**设计原则：** 网关优先本地完成签名校验，不把请求过滤变成一个同步 RPC 链。`/auth/validate` 接口保留用于 Redis 黑名单检查、权限上下文补充等非核心路径。

#### 🔒 生产环境：防伪造 X-User-Id Header

Gateway 透传 `X-OTA-UserId`、`X-OTA-Username` 到下游服务，生产环境必须防止恶意客户端直接构造这些 Header 绕过网关：

1. **内网隔离** — 下游服务不对外暴露端口，仅允许 Gateway 所在网段访问
2. **网关签名头** — Gateway 在转发时用内部签名密钥对 `X-OTA-UserId` 加签，下游服务验签后信任
3. **mTLS** — 服务间通信使用双向 TLS 认证，Gateway 与下游互验证书

> **✅ 本章教程（开发环境）** 暂使用最简单方式：通过内网隔离 + 下游信任网关 IP。生产环境建议实施签名头或 mTLS。

#### 🗄️ JWT 黑名单 Redis 降级策略

`AuthGlobalFilter` 中的黑名单检查依赖 Redis。当 Redis 宕机时：

```
Redis 不可用 → 黑名单查询抛出异常 → catch 块默认放行
                 ↓
         依赖短 TTL（15分钟）兜底
         即使黑名单失效，过期 Token 也会自然失效
```

**生产建议：**
- Redis 主从 + Sentinel 高可用（自动故障转移）
- 降级期间只校验 JWT 签名和有效期，不检查黑名单
- 15 分钟 TTL 是安全窗口与用户体验的平衡

#### 🔄 refresh_token rotation（P1 待实现）

当前本章仅实现了 access_token（单 Token 模式）。**P1 优先级** 计划支持双 Token 模式：

- `access_token`：有效期 15 分钟，用于 API 鉴权
- `refresh_token`：有效期 7 天，用于获取新的 access_token
- **Rotation 机制**：每次使用 refresh_token 时，服务端签发新的 access_token **+** 新的 refresh_token，旧 refresh_token 立即失效
- 防止 refresh_token 泄露后的重放攻击

详见 §2.8 面试追问「双 Token 设计」部分。

---


> 路径约定：外部访问统一走 Gateway 的 `/api/...` 前缀，例如 `/api/auth/login`；服务内部 Controller 仍可以保留 `/auth/login`、`/user/profile` 这类本地路径，由 Gateway 的 `StripPrefix=1` 转发后访问。

## 2.3 ota-auth 认证服务

### 2.3.1 POM 配置

**文件：`ota-auth/pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.ota.cloud</groupId>
        <artifactId>ota-cloud</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>

    <artifactId>ota-auth</artifactId>
    <description>OTA Cloud Auth Service</description>

    <dependencies>
        <!-- 内部模块 -->
        <dependency>
            <groupId>com.ota.cloud</groupId>
            <artifactId>ota-common</artifactId>
        </dependency>

        <!-- Spring Boot Web -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>

        <!-- Spring Security -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-security</artifactId>
        </dependency>

        <!-- Nacos 服务发现 -->
        <dependency>
            <groupId>com.alibaba.cloud</groupId>
            <artifactId>spring-cloud-starter-alibaba-nacos-discovery</artifactId>
        </dependency>

        <!-- Nacos 配置中心 -->
        <dependency>
            <groupId>com.alibaba.cloud</groupId>
            <artifactId>spring-cloud-starter-alibaba-nacos-config</artifactId>
        </dependency>

        <!-- MyBatis-Plus -->
        <dependency>
            <groupId>com.baomidou</groupId>
            <artifactId>mybatis-plus-spring-boot3-starter</artifactId>
        </dependency>

        <!-- MySQL -->
        <dependency>
            <groupId>com.mysql</groupId>
            <artifactId>mysql-connector-j</artifactId>
        </dependency>

        <!-- Druid -->
        <dependency>
            <groupId>com.alibaba</groupId>
            <artifactId>druid-spring-boot-3-starter</artifactId>
        </dependency>

        <!-- Redis -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-redis</artifactId>
        </dependency>

        <!-- JWT -->
        <dependency>
            <groupId>io.jsonwebtoken</groupId>
            <artifactId>jjwt-api</artifactId>
        </dependency>
        <dependency>
            <groupId>io.jsonwebtoken</groupId>
            <artifactId>jjwt-impl</artifactId>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>io.jsonwebtoken</groupId>
            <artifactId>jjwt-jackson</artifactId>
            <scope>runtime</scope>
        </dependency>

        <!-- Lombok -->
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
        </dependency>

        <!-- Test -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

### 2.3.2 启动类

**文件：`ota-auth/src/main/java/com/ota/cloud/auth/AuthApplication.java`**

```java
package com.ota.cloud.auth;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@SpringBootApplication
@EnableDiscoveryClient
@MapperScan("com.ota.cloud.auth.mapper")
public class AuthApplication {

    public static void main(String[] args) {
        SpringApplication.run(AuthApplication.class, args);
    }
}
```

### 2.3.3 bootstrap.yml

**文件：`ota-auth/src/main/resources/bootstrap.yml`**

```yaml
spring:
  application:
    name: ota-auth
  cloud:
    nacos:
      config:
        server-addr: 127.0.0.1:8848
        namespace: dev
        group: DEFAULT_GROUP
        file-extension: yml
        shared-configs:
          - data-id: ota-shared.yml
            group: DEFAULT_GROUP
            refresh: true
      discovery:
        server-addr: 127.0.0.1:8848
        namespace: dev
```

### 2.3.4 数据库实体

**文件：`ota-auth/src/main/java/com/ota/cloud/auth/entity/User.java`**

```java
package com.ota.cloud.auth.entity;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 用户实体
 */
@Data
@TableName("ota_user")
public class User {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String username;

    private String password;

    private String phone;

    private String email;

    private String nickname;

    @TableLogic
    private Integer deleted;

    private Integer status;

    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createTime;

    @TableField(fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updateTime;
}
```

### 2.3.5 MyBatis-Plus Mapper

**文件：`ota-auth/src/main/java/com/ota/cloud/auth/mapper/UserMapper.java`**

```java
package com.ota.cloud.auth.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.ota.cloud.auth.entity.User;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface UserMapper extends BaseMapper<User> {
}
```

### 2.3.6 MyBatis-Plus 自动填充处理器

**文件：`ota-auth/src/main/java/com/ota/cloud/auth/config/MyMetaObjectHandler.java`**

```java
package com.ota.cloud.auth.config;

import com.baomidou.mybatisplus.core.handlers.MetaObjectHandler;
import org.apache.ibatis.reflection.MetaObject;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

/**
 * MyBatis-Plus 自动填充处理
 * 自动填充 createTime / updateTime
 */
@Component
public class MyMetaObjectHandler implements MetaObjectHandler {

    @Override
    public void insertFill(MetaObject metaObject) {
        this.strictInsertFill(metaObject, "createTime", LocalDateTime::now, LocalDateTime.class);
        this.strictInsertFill(metaObject, "updateTime", LocalDateTime::now, LocalDateTime.class);
    }

    @Override
    public void updateFill(MetaObject metaObject) {
        this.strictUpdateFill(metaObject, "updateTime", LocalDateTime::now, LocalDateTime.class);
    }
}
```

### 2.3.7 JWT 工具类

**文件：`ota-auth/src/main/java/com/ota/cloud/auth/util/JwtUtil.java`**

```java
package com.ota.cloud.auth.util;

import com.ota.cloud.common.constant.CommonConstant;
import io.jsonwebtoken.*;
import io.jsonwebtoken.security.Keys;
import org.springframework.stereotype.Component;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * JWT 工具类
 *
 * 使用 HMAC-SHA256 对称签名算法。
 * 生产环境建议替换为 RSA/EC 非对称密钥。
 */
@Component
public class JwtUtil {

    private final SecretKey secretKey;

    public JwtUtil() {
        this.secretKey = Keys.hmacShaKeyFor(
                CommonConstant.JWT_SECRET.getBytes(StandardCharsets.UTF_8)
        );
    }

    /**
     * 生成 JWT Token
     *
     * access_token 有效期 15 分钟（短 TTL 降低泄露风险）
     * 在黑名单查不到时，过期时间是最終的安全兜底
     *
     * @param userId   用户ID
     * @param username 用户名
     * @return JWT token 字符串
     */
    public String generateToken(Long userId, String username) {
        Map<String, Object> claims = new HashMap<>();
        claims.put("userId", userId);
        claims.put("username", username);

        long now = System.currentTimeMillis();

        return Jwts.builder()
                .claims(claims)
                .subject(String.valueOf(userId))
                .issuedAt(new Date(now))
                .expiration(new Date(now + CommonConstant.JWT_EXPIRATION * 1000))
                .signWith(secretKey)
                .compact();
    }

    /**
     * 解析 Token，返回 Claims
     */
    public Claims parseToken(String token) {
        return Jwts.parser()
                .verifyWith(secretKey)
                .build()
                .parseSignedClaims(token)
                .getPayload();
    }

    /**
     * 从 Token 中提取用户ID
     */
    public Long getUserId(String token) {
        return parseToken(token).get("userId", Long.class);
    }

    /**
     * 从 Token 中提取用户名
     */
    public String getUsername(String token) {
        return parseToken(token).get("username", String.class);
    }

    /**
     * 从 Token 中提取 JWT ID
     */
    public String getJti(String token) {
        return parseToken(token).getId();
    }

    /**
     * 校验 Token 是否有效（未过期且签名正确）
     */
    public boolean validateToken(String token) {
        try {
            parseToken(token);
            return true;
        } catch (JwtException | IllegalArgumentException e) {
            return false;
        }
    }
}
```

### 2.3.8 Spring Security 配置

关键设计：Auth 服务面向的是"内部调用 + 部分对外接口"。
- `/auth/login`、`/auth/register` 公开访问
- `/auth/validate` 仅供 Gateway 内部调用
- 其他接口需要认证

**文件：`ota-auth/src/main/java/com/ota/cloud/auth/config/SecurityConfig.java`**

```java
package com.ota.cloud.auth.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.configuration.AuthenticationConfiguration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Spring Security 配置
 *
 * 使用无状态（无 session）模式，基于 JWT 认证。
 */
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
            // 禁用 CSRF（无状态 API 不需要）
            .csrf(csrf -> csrf.disable())
            // 无 Session 模式
            .sessionManagement(session ->
                session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            // 请求授权
            .authorizeHttpRequests(auth -> auth
                // 公开接口
                .requestMatchers("/auth/login", "/auth/register").permitAll()
                // 内部接口（仅 Gateway 可调用）
                .requestMatchers("/auth/validate", "/auth/logout").permitAll()
                // 其他请求需认证
                .anyRequest().authenticated()
            )
            // 禁用表单登录
            .formLogin(form -> form.disable())
            // 禁用 HTTP Basic
            .httpBasic(basic -> basic.disable());

        return http.build();
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    public AuthenticationManager authenticationManager(
            AuthenticationConfiguration authConfig) throws Exception {
        return authConfig.getAuthenticationManager();
    }
}
```

### 2.3.9 DTO 定义

**文件：`ota-auth/src/main/java/com/ota/cloud/auth/dto/LoginRequest.java`**

```java
package com.ota.cloud.auth.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

/**
 * 登录请求 DTO
 */
@Data
public class LoginRequest {

    @NotBlank(message = "用户名不能为空")
    private String username;

    @NotBlank(message = "密码不能为空")
    private String password;
}
```

**文件：`ota-auth/src/main/java/com/ota/cloud/auth/dto/LoginResponse.java`**

```java
package com.ota.cloud.auth.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 登录响应 DTO
 *
 * 当前仅返回 access_token（15 分钟）。
 * P1 计划：增加 refresh_token + rotation，详见 §2.2.1 和 §2.8 Q3。
 */
@Data
@AllArgsConstructor
public class LoginResponse {

    private String token;
    private String tokenType;
    private long expiresIn;
    /** P1: refresh_token，当前返回 null，未来支持 rotation 后填充 */
    private String refreshToken;

    public static LoginResponse of(String token) {
        // access_token 有效期 15 分钟 = 900 秒
        return new LoginResponse(token, "Bearer", 900L, null);
    }
}
```

**文件：`ota-auth/src/main/java/com/ota/cloud/auth/dto/ValidateResponse.java`**

```java
package com.ota.cloud.auth.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Token 校验响应 DTO
 */
@Data
@AllArgsConstructor
public class ValidateResponse {

    private boolean valid;
    private Long userId;
    private String username;
}
```

### 2.3.10 AuthService

**文件：`ota-auth/src/main/java/com/ota/cloud/auth/service/AuthService.java`**

```java
package com.ota.cloud.auth.service;

import com.ota.cloud.auth.dto.LoginRequest;
import com.ota.cloud.auth.dto.LoginResponse;
import com.ota.cloud.auth.dto.ValidateResponse;
import com.ota.cloud.auth.entity.User;
import com.ota.cloud.auth.mapper.UserMapper;
import com.ota.cloud.auth.util.JwtUtil;
import com.ota.cloud.common.constant.CommonConstant;
import com.ota.cloud.common.exception.BusinessException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * 认证服务
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AuthService {

    private final UserMapper userMapper;
    private final PasswordEncoder passwordEncoder;
    private final JwtUtil jwtUtil;
    private final StringRedisTemplate redisTemplate;

    /**
     * 用户登录
     *
     * @param request 登录请求（用户名 + 密码）
     * @return JWT token
     */
    public LoginResponse login(LoginRequest request) {
        // 1. 查询用户
        User user = userMapper.selectOne(
                com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper
                        .<User>lambdaQuery()
                        .eq(User::getUsername, request.getUsername())
                        .eq(User::getDeleted, 0)
        );

        if (user == null) {
            throw new BusinessException(
                    BusinessException.ErrorCode.UNAUTHORIZED, "用户名或密码错误");
        }

        // 2. 校验密码（BCrypt）
        if (!passwordEncoder.matches(request.getPassword(), user.getPassword())) {
            throw new BusinessException(
                    BusinessException.ErrorCode.UNAUTHORIZED, "用户名或密码错误");
        }

        // 3. 检查用户状态
        if (user.getStatus() == 0) {
            throw new BusinessException(
                    BusinessException.ErrorCode.FORBIDDEN, "账户已被禁用");
        }

        // 4. 生成 JWT
        String token = jwtUtil.generateToken(user.getId(), user.getUsername());
        log.info("用户登录成功: username={}, userId={}", user.getUsername(), user.getId());

        return LoginResponse.of(token);
    }

    /**
     * 校验 Token（供 Gateway 内部调用）
     *
     * @param token JWT token
     * @return 校验结果
     */
    public ValidateResponse validateToken(String token) {
        // 1. 检查 Token 是否在黑名单中（已登出）
        String jti = null;
        try {
            jti = jwtUtil.getJti(token);
            String blacklisted = redisTemplate.opsForValue()
                    .get(CommonConstant.REDIS_KEY_JWT_BLACKLIST + jti);
            if (blacklisted != null) {
                log.warn("Token 已在黑名单中: jti={}", jti);
                return new ValidateResponse(false, null, null);
            }
        } catch (Exception e) {
            return new ValidateResponse(false, null, null);
        }

        // 2. 解析 Token
        boolean valid = jwtUtil.validateToken(token);
        if (!valid) {
            return new ValidateResponse(false, null, null);
        }

        // 3. 提取用户信息
        Long userId = jwtUtil.getUserId(token);
        String username = jwtUtil.getUsername(token);

        return new ValidateResponse(true, userId, username);
    }

    /**
     * 用户登出
     *
     * 将 Token 的 jti 加入 Redis 黑名单，TTL 与 Token 过期时间一致（15 分钟）
     * 当 Redis 不可用时，依赖短 TTL 兜底
     */
    public void logout(String token) {
        try {
            String jti = jwtUtil.getJti(token);
            // 将 Token 加入 Redis 黑名单，TTL 与 Token 过期时间一致（15 分钟）
            redisTemplate.opsForValue().set(
                    CommonConstant.REDIS_KEY_JWT_BLACKLIST + jti,
                    "1",
                    CommonConstant.JWT_BLACKLIST_TTL,
                    TimeUnit.SECONDS
            );
            log.info("用户登出成功: jti={}", jti);
        } catch (Exception e) {
            log.warn("登出处理异常", e);
        }
    }
}
```

### 2.3.11 AuthController

**文件：`ota-auth/src/main/java/com/ota/cloud/auth/controller/AuthController.java`**

```java
package com.ota.cloud.auth.controller;

import com.ota.cloud.auth.dto.LoginRequest;
import com.ota.cloud.auth.dto.LoginResponse;
import com.ota.cloud.auth.dto.ValidateResponse;
import com.ota.cloud.auth.service.AuthService;
import com.ota.cloud.common.model.Result;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

/**
 * 认证控制器
 */
@RestController
@RequestMapping("/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;

    /**
     * 用户登录
     */
    @PostMapping("/login")
    public Result<LoginResponse> login(@Valid @RequestBody LoginRequest request) {
        LoginResponse response = authService.login(request);
        return Result.success(response);
    }

    /**
     * 校验 Token（供 Gateway 内部调用）
     */
    @PostMapping("/validate")
    public Result<ValidateResponse> validate(@RequestParam("token") String token) {
        ValidateResponse response = authService.validateToken(token);
        return Result.success(response);
    }

    /**
     * 用户登出
     */
    @PostMapping("/logout")
    public Result<Void> logout(@RequestHeader("Authorization") String authHeader) {
        if (authHeader != null && authHeader.startsWith("Bearer ")) {
            String token = authHeader.substring(7);
            authService.logout(token);
        }
        return Result.success();
    }

    /**
     * 健康检查
     */
    @GetMapping("/health")
    public Result<String> health() {
        return Result.success("auth-service is running");
    }
}
```

### 2.3.12 application.yml（本地回退配置）

Nacos 未就绪时，此配置作为本地回退。

**文件：`ota-auth/src/main/resources/application.yml`**

```yaml
server:
  port: 8081

spring:
  datasource:
    url: jdbc:mysql://127.0.0.1:3306/ota_cloud?useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai&useSSL=false&allowPublicKeyRetrieval=true
    username: root
    password: root123456
    driver-class-name: com.mysql.cj.jdbc.Driver
    type: com.alibaba.druid.pool.DruidDataSource
    druid:
      initial-size: 5
      min-idle: 5
      max-active: 20
      max-wait: 60000
  data:
    redis:
      host: 127.0.0.1
      port: 6379
      password: redis123456
      timeout: 3000ms

mybatis-plus:
  global-config:
    banner: false
    db-config:
      id-type: auto
      logic-delete-field: deleted
      logic-delete-value: 1
      logic-not-delete-value: 0
  configuration:
    map-underscore-to-camel-case: true
    log-impl: org.apache.ibatis.logging.stdout.StdOutImpl

logging:
  level:
    com.ota.cloud: DEBUG
```

---

## 2.4 ota-user 用户服务

### 2.4.1 POM 配置

**文件：`ota-user/pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.ota.cloud</groupId>
        <artifactId>ota-cloud</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>

    <artifactId>ota-user</artifactId>
    <description>OTA Cloud User Service</description>

    <dependencies>
        <!-- 内部模块 -->
        <dependency>
            <groupId>com.ota.cloud</groupId>
            <artifactId>ota-common</artifactId>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>

        <dependency>
            <groupId>com.alibaba.cloud</groupId>
            <artifactId>spring-cloud-starter-alibaba-nacos-discovery</artifactId>
        </dependency>
        <dependency>
            <groupId>com.alibaba.cloud</groupId>
            <artifactId>spring-cloud-starter-alibaba-nacos-config</artifactId>
        </dependency>

        <dependency>
            <groupId>com.baomidou</groupId>
            <artifactId>mybatis-plus-spring-boot3-starter</artifactId>
        </dependency>
        <dependency>
            <groupId>com.mysql</groupId>
            <artifactId>mysql-connector-j</artifactId>
        </dependency>
        <dependency>
            <groupId>com.alibaba</groupId>
            <artifactId>druid-spring-boot-3-starter</artifactId>
        </dependency>

        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

### 2.4.2 启动类

**文件：`ota-user/src/main/java/com/ota/cloud/user/UserApplication.java`**

```java
package com.ota.cloud.user;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@SpringBootApplication
@EnableDiscoveryClient
@MapperScan("com.ota.cloud.user.mapper")
public class UserApplication {

    public static void main(String[] args) {
        SpringApplication.run(UserApplication.class, args);
    }
}
```

### 2.4.3 bootstrap.yml

**文件：`ota-user/src/main/resources/bootstrap.yml`**

```yaml
spring:
  application:
    name: ota-user
  cloud:
    nacos:
      config:
        server-addr: 127.0.0.1:8848
        namespace: dev
        group: DEFAULT_GROUP
        file-extension: yml
        shared-configs:
          - data-id: ota-shared.yml
            group: DEFAULT_GROUP
            refresh: true
      discovery:
        server-addr: 127.0.0.1:8848
        namespace: dev
```

### 2.4.4 实体、Mapper

**文件：`ota-user/src/main/java/com/ota/cloud/user/entity/User.java`**

```java
package com.ota.cloud.user.entity;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("ota_user")
public class User {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String username;

    private String password;

    private String phone;

    private String email;

    private String nickname;

    @TableLogic
    private Integer deleted;

    private Integer status;

    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createTime;

    @TableField(fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updateTime;
}
```

**文件：`ota-user/src/main/java/com/ota/cloud/user/mapper/UserMapper.java`**

```java
package com.ota.cloud.user.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.ota.cloud.user.entity.User;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface UserMapper extends BaseMapper<User> {
}
```

### 2.4.5 自动填充

**文件：`ota-user/src/main/java/com/ota/cloud/user/config/MyMetaObjectHandler.java`**

```java
package com.ota.cloud.user.config;

import com.baomidou.mybatisplus.core.handlers.MetaObjectHandler;
import org.apache.ibatis.reflection.MetaObject;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class MyMetaObjectHandler implements MetaObjectHandler {

    @Override
    public void insertFill(MetaObject metaObject) {
        this.strictInsertFill(metaObject, "createTime", LocalDateTime::now, LocalDateTime.class);
        this.strictInsertFill(metaObject, "updateTime", LocalDateTime::now, LocalDateTime.class);
    }

    @Override
    public void updateFill(MetaObject metaObject) {
        this.strictUpdateFill(metaObject, "updateTime", LocalDateTime::now, LocalDateTime.class);
    }
}
```

### 2.4.6 DTO

**文件：`ota-user/src/main/java/com/ota/cloud/user/dto/RegisterRequest.java`**

```java
package com.ota.cloud.user.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import lombok.Data;

/**
 * 注册请求 DTO
 */
@Data
public class RegisterRequest {

    @NotBlank(message = "用户名不能为空")
    @Size(min = 3, max = 64, message = "用户名长度需在3-64之间")
    private String username;

    @NotBlank(message = "密码不能为空")
    @Size(min = 6, max = 128, message = "密码长度需在6-128之间")
    private String password;

    @Pattern(regexp = "^1[3-9]\\d{9}$", message = "手机号格式不正确")
    private String phone;

    private String email;

    private String nickname;
}
```

**文件：`ota-user/src/main/java/com/ota/cloud/user/dto/UserInfoResponse.java`**

```java
package com.ota.cloud.user.dto;

import lombok.Data;

import java.time.LocalDateTime;

/**
 * 用户信息响应 DTO
 */
@Data
public class UserInfoResponse {

    private Long id;
    private String username;
    private String phone;
    private String email;
    private String nickname;
    private Integer status;
    private LocalDateTime createTime;
}
```

### 2.4.7 UserService

**文件：`ota-user/src/main/java/com/ota/cloud/user/service/UserService.java`**

```java
package com.ota.cloud.user.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.ota.cloud.common.exception.BusinessException;
import com.ota.cloud.user.dto.RegisterRequest;
import com.ota.cloud.user.dto.UserInfoResponse;
import com.ota.cloud.user.entity.User;
import com.ota.cloud.user.mapper.UserMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

/**
 * 用户服务
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class UserService {

    private final UserMapper userMapper;
    private final PasswordEncoder passwordEncoder = new BCryptPasswordEncoder();

    /**
     * 用户注册
     */
    public UserInfoResponse register(RegisterRequest request) {
        // 1. 检查用户名是否已存在
        Long count = userMapper.selectCount(
                LambdaQueryWrapper.<User>lambdaQuery()
                        .eq(User::getUsername, request.getUsername())
                        .eq(User::getDeleted, 0)
        );
        if (count > 0) {
            throw new BusinessException(
                    BusinessException.ErrorCode.CONFLICT, "用户名已存在");
        }

        // 2. 检查手机号是否已存在
        if (request.getPhone() != null) {
            count = userMapper.selectCount(
                    LambdaQueryWrapper.<User>lambdaQuery()
                            .eq(User::getPhone, request.getPhone())
                            .eq(User::getDeleted, 0)
            );
            if (count > 0) {
                throw new BusinessException(
                        BusinessException.ErrorCode.CONFLICT, "手机号已注册");
            }
        }

        // 3. 创建用户
        User user = new User();
        BeanUtils.copyProperties(request, user);
        user.setPassword(passwordEncoder.encode(request.getPassword()));
        user.setStatus(1);

        userMapper.insert(user);
        log.info("用户注册成功: username={}, id={}", user.getUsername(), user.getId());

        // 4. 返回用户信息
        return toUserInfoResponse(user);
    }

    /**
     * 根据ID查询用户
     */
    public UserInfoResponse getUserById(Long userId) {
        User user = userMapper.selectById(userId);
        if (user == null || user.getDeleted() == 1) {
            throw new BusinessException(
                    BusinessException.ErrorCode.NOT_FOUND, "用户不存在");
        }
        return toUserInfoResponse(user);
    }

    private UserInfoResponse toUserInfoResponse(User user) {
        UserInfoResponse response = new UserInfoResponse();
        BeanUtils.copyProperties(user, response);
        return response;
    }
}
```

### 2.4.8 UserController

**文件：`ota-user/src/main/java/com/ota/cloud/user/controller/UserController.java`**

```java
package com.ota.cloud.user.controller;

import com.ota.cloud.common.constant.CommonConstant;
import com.ota.cloud.common.model.Result;
import com.ota.cloud.user.dto.RegisterRequest;
import com.ota.cloud.user.dto.UserInfoResponse;
import com.ota.cloud.user.service.UserService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

/**
 * 用户控制器
 *
 * 注意：用户上下文由 Gateway 透传（Header: X-OTA-UserId, X-OTA-Username）
 */
@RestController
@RequestMapping("/user")
@RequiredArgsConstructor
public class UserController {

    private final UserService userService;

    /**
     * 用户注册（公开接口）
     */
    @PostMapping("/register")
    public Result<UserInfoResponse> register(@Valid @RequestBody RegisterRequest request) {
        UserInfoResponse response = userService.register(request);
        return Result.success(response);
    }

    /**
     * 获取当前用户信息（需登录）
     *
     * 从 Request Header 中获取用户ID（由 Gateway 注入）
     */
    @GetMapping("/info")
    public Result<UserInfoResponse> info(
            @RequestHeader(CommonConstant.HEADER_USER_ID) Long userId) {
        UserInfoResponse response = userService.getUserById(userId);
        return Result.success(response);
    }

    /**
     * 根据ID查询用户（需登录）
     */
    @GetMapping("/{id}")
    public Result<UserInfoResponse> getById(@PathVariable Long id) {
        UserInfoResponse response = userService.getUserById(id);
        return Result.success(response);
    }

    /**
     * 健康检查
     */
    @GetMapping("/health")
    public Result<String> health() {
        return Result.success("user-service is running");
    }
}
```

### 2.4.9 application.yml

**文件：`ota-user/src/main/resources/application.yml`**

```yaml
server:
  port: 8082

spring:
  datasource:
    url: jdbc:mysql://127.0.0.1:3306/ota_cloud?useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai&useSSL=false&allowPublicKeyRetrieval=true
    username: root
    password: root123456
    driver-class-name: com.mysql.cj.jdbc.Driver
    type: com.alibaba.druid.pool.DruidDataSource
    druid:
      initial-size: 5
      min-idle: 5
      max-active: 20
  data:
    redis:
      host: 127.0.0.1
      port: 6379
      password: redis123456
      timeout: 3000ms

mybatis-plus:
  global-config:
    banner: false
    db-config:
      id-type: auto
      logic-delete-field: deleted
      logic-delete-value: 1
      logic-not-delete-value: 0
  configuration:
    map-underscore-to-camel-case: true
    log-impl: org.apache.ibatis.logging.stdout.StdOutImpl

logging:
  level:
    com.ota.cloud: DEBUG
```

---

## 2.5 ota-gateway 网关服务

### 2.5.1 POM 配置

**文件：`ota-gateway/pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.ota.cloud</groupId>
        <artifactId>ota-cloud</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>

    <artifactId>ota-gateway</artifactId>
    <description>OTA Cloud Gateway</description>

    <dependencies>
        <!-- 内部模块 -->
        <dependency>
            <groupId>com.ota.cloud</groupId>
            <artifactId>ota-common</artifactId>
            <exclusions>
                <exclusion>
                    <groupId>org.springframework.boot</groupId>
                    <artifactId>spring-boot-starter-web</artifactId>
                </exclusion>
            </exclusions>
        </dependency>

        <!-- Spring Cloud Gateway（基于 WebFlux） -->
        <dependency>
            <groupId>org.springframework.cloud</groupId>
            <artifactId>spring-cloud-starter-gateway</artifactId>
        </dependency>

        <!-- Nacos 服务发现 -->
        <dependency>
            <groupId>com.alibaba.cloud</groupId>
            <artifactId>spring-cloud-starter-alibaba-nacos-discovery</artifactId>
        </dependency>

        <!-- Nacos 配置中心 -->
        <dependency>
            <groupId>com.alibaba.cloud</groupId>
            <artifactId>spring-cloud-starter-alibaba-nacos-config</artifactId>
        </dependency>

        <!-- Redis（用于 JWT 黑名单校验） -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-redis-reactive</artifactId>
        </dependency>

        <!-- JWT -->
        <dependency>
            <groupId>io.jsonwebtoken</groupId>
            <artifactId>jjwt-api</artifactId>
        </dependency>
        <dependency>
            <groupId>io.jsonwebtoken</groupId>
            <artifactId>jjwt-impl</artifactId>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>io.jsonwebtoken</groupId>
            <artifactId>jjwt-jackson</artifactId>
            <scope>runtime</scope>
        </dependency>

        <!-- Lombok -->
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
        </dependency>

        <!-- Test -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

> **⚠️ 关键注意：** Gateway 基于 WebFlux（Reactive），不能引入 `spring-boot-starter-web`。因此 ota-common 中 web 依赖声明为 `provided`，且在 Gateway 的 common 依赖中需要排除 Web 模块。

### 2.5.2 启动类

**文件：`ota-gateway/src/main/java/com/ota/cloud/gateway/GatewayApplication.java`**

```java
package com.ota.cloud.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@SpringBootApplication
@EnableDiscoveryClient
public class GatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(GatewayApplication.class, args);
    }
}
```

### 2.5.3 bootstrap.yml

**文件：`ota-gateway/src/main/resources/bootstrap.yml`**

```yaml
spring:
  application:
    name: ota-gateway
  cloud:
    nacos:
      config:
        server-addr: 127.0.0.1:8848
        namespace: dev
        group: DEFAULT_GROUP
        file-extension: yml
        shared-configs:
          - data-id: ota-shared.yml
            group: DEFAULT_GROUP
            refresh: true
      discovery:
        server-addr: 127.0.0.1:8848
        namespace: dev
```

### 2.5.4 application.yml（路由配置 + JWT Key）

**文件：`ota-gateway/src/main/resources/application.yml`**

```yaml
server:
  port: 8080

spring:
  cloud:
    gateway:
      # 全局默认过滤器
      default-filters:
        - DedupeResponseHeader=Access-Control-Allow-Origin
      # 路由配置
      routes:
        # 认证服务
        - id: ota-auth
          uri: lb://ota-auth
          predicates:
            - Path=/api/auth/**
          filters:
            - StripPrefix=1

        # 用户服务
        - id: ota-user
          uri: lb://ota-user
          predicates:
            - Path=/api/user/**
          filters:
            - StripPrefix=1

        # 酒店服务（预留）
        - id: ota-hotel
          uri: lb://ota-hotel
          predicates:
            - Path=/api/hotel/**
          filters:
            - StripPrefix=1

        # 订单服务（预留）
        - id: ota-order
          uri: lb://ota-order
          predicates:
            - Path=/api/order/**
          filters:
            - StripPrefix=1

        # 库存服务（预留）
        - id: ota-inventory
          uri: lb://ota-inventory
          predicates:
            - Path=/api/inventory/**
          filters:
            - StripPrefix=1

        # AI服务（预留）
        - id: ota-ai
          uri: lb://ota-ai
          predicates:
            - Path=/api/ai/**
          filters:
            - StripPrefix=1

      # 全局 CORS 配置
      globalcors:
        cors-configurations:
          '[/**]':
            allowedOriginPatterns: "*"
            allowedMethods: "*"
            allowedHeaders: "*"
            allowCredentials: true
            maxAge: 3600

  data:
    redis:
      host: 127.0.0.1
      port: 6379
      password: redis123456
      timeout: 3000ms

# JWT 配置
jwt:
  secret: "ota-cloud-jwt-secret-key-must-be-at-least-256-bits-long-for-hs256"

logging:
  level:
    com.ota.cloud: DEBUG
    org.springframework.cloud.gateway: TRACE
```

### 2.5.5 白名单配置

**文件：`ota-gateway/src/main/java/com/ota/cloud/gateway/config/WhiteListConfig.java`**

```java
package com.ota.cloud.gateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 白名单配置
 *
 * 白名单中的路径无需 JWT 认证，Gateway 直接放行。
 */
@Component
@ConfigurationProperties(prefix = "gateway.white-list")
public class WhiteListConfig {

    private List<String> paths;

    public List<String> getPaths() {
        return paths;
    }

    public void setPaths(List<String> paths) {
        this.paths = paths;
    }
}
```

### 2.5.6 application.yml 补充（白名单配置）

在 `application.yml` 末尾追加：

```yaml
# 白名单路径（无需登录）
gateway:
  white-list:
    paths:
      - /api/auth/login
      - /api/auth/register
      - /api/user/register
      - /api/auth/health
      - /api/user/health
      - /actuator/health
```

### 2.5.7 全局认证过滤器

这是 Gateway 的核心——所有请求经过此过滤器：

**关键架构变化：** 与旧版教程不同，Gateway **不再**每次请求都物理转发到 auth 的 `/auth/validate`。改为：

1. **本地解析 JWT** — 校验签名 + 有效期（使用与 auth 相同的 Secret Key）
2. **本地检查 Redis 黑名单** — catch 异常时降级放行（依赖 15 分钟 TTL 兜底）
3. **必要时调用 auth** — 权限上下文补充等场景才会回退到 `/auth/validate`

**文件：`ota-gateway/src/main/java/com/ota/cloud/gateway/filter/AuthGlobalFilter.java`**

```java
package com.ota.cloud.gateway.filter;

import com.ota.cloud.common.constant.CommonConstant;
import com.ota.cloud.common.util.TraceIdUtil;
import com.ota.cloud.gateway.config.WhiteListConfig;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;

/**
 * 全局认证过滤器
 *
 * 处理顺序：
 * 1. 白名单路径直接放行（login, register）
 * 2. 其他请求检查 Authorization Header
 * 3. 本地解析 JWT（验签 + 查过期），必要时调 auth/validate
 * 4. Redis 黑名单检查（降级策略：Redis 挂了默认放行）
 * 5. 透传用户上下文（userId, username）到下游服务
 * 6. 生成并透传 TraceId
 */
@Slf4j
@Component
public class AuthGlobalFilter implements GlobalFilter, Ordered {

    private final WhiteListConfig whiteListConfig;
    private final StringRedisTemplate redisTemplate;
    private final SecretKey secretKey;

    public AuthGlobalFilter(WhiteListConfig whiteListConfig,
                            StringRedisTemplate redisTemplate) {
        this.whiteListConfig = whiteListConfig;
        this.redisTemplate = redisTemplate;
        this.secretKey = Keys.hmacShaKeyFor(
                CommonConstant.JWT_SECRET.getBytes(StandardCharsets.UTF_8)
        );
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        ServerHttpRequest request = exchange.getRequest();
        String path = request.getURI().getPath();

        // ===== Step 1: 白名单检查 =====
        if (isWhiteListed(path)) {
            log.debug("白名单路径放行: {}", path);
            return chain.filter(exchange);
        }

        // ===== Step 2: 提取 Token =====
        String authHeader = request.getHeaders()
                .getFirst(CommonConstant.JWT_HEADER);

        if (authHeader == null || !authHeader.startsWith(CommonConstant.JWT_PREFIX)) {
            log.warn("缺少 Authorization Header: {}", path);
            return unauthorized(exchange, "Missing or invalid Authorization header");
        }

        String token = authHeader.substring(CommonConstant.JWT_PREFIX.length()).trim();

        // ===== Step 3: 校验 Token =====
        try {
            // 解析 Claims（本步骤本地完成，不依赖远程调用）
            Claims claims = Jwts.parser()
                    .verifyWith(secretKey)
                    .build()
                    .parseSignedClaims(token)
                    .getPayload();

            Long userId = claims.get("userId", Long.class);
            String username = claims.get("username", String.class);
            String jti = claims.getId();

            // ===== Step 4: 黑名单检查（Redis）=====
            try {
                String blacklisted = redisTemplate.opsForValue()
                        .get(CommonConstant.REDIS_KEY_JWT_BLACKLIST + jti);
                if (blacklisted != null) {
                    log.warn("Token 已被列入黑名单: jti={}", jti);
                    return unauthorized(exchange, "Token has been revoked");
                }
            } catch (Exception e) {
                // Redis 挂了 ↓ 降级：默认放行，依赖短 TTL（15 分钟）兜底
                log.warn("Redis 不可用，跳过黑名单检查，依赖 TTL 兜底: {}", e.getMessage());
            }

            String traceId = TraceIdUtil.generate();

            log.debug("Token 校验通过: userId={}, username={}", userId, username);

            // ===== Step 5: 透传用户上下文 =====
            ServerHttpRequest mutatedRequest = request.mutate()
                    .header(CommonConstant.HEADER_USER_ID, String.valueOf(userId))
                    .header(CommonConstant.HEADER_USERNAME, username)
                    .header(CommonConstant.HEADER_TRACE_ID, traceId)
                    .build();

            return chain.filter(exchange.mutate().request(mutatedRequest).build());

        } catch (Exception e) {
            log.warn("Token 校验失败: {}", e.getMessage());
            return unauthorized(exchange, "Invalid or expired token");
        }
    }

    @Override
    public int getOrder() {
        // 优先级最高，在所有过滤器之前执行
        return -100;
    }

    /**
     * 检查是否在白名单中
     */
    private boolean isWhiteListed(String path) {
        return whiteListConfig.getPaths().stream()
                .anyMatch(whitePath -> path.startsWith(whitePath)
                        || path.matches(whitePath));
    }

    /**
     * 返回 401 未授权响应
     */
    private Mono<Void> unauthorized(ServerWebExchange exchange, String message) {
        exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
        exchange.getResponse().getHeaders()
                .setContentType(MediaType.APPLICATION_JSON);

        String body = String.format(
                "{\"code\":401,\"message\":\"%s\",\"timestamp\":%d}",
                message, System.currentTimeMillis()
        );

        DataBuffer buffer = exchange.getResponse().bufferFactory()
                .wrap(body.getBytes(StandardCharsets.UTF_8));
        return exchange.getResponse().writeWith(Mono.just(buffer));
    }
}
```

### 2.5.8 TraceId 过滤器

**文件：`ota-gateway/src/main/java/com/ota/cloud/gateway/filter/TraceIdFilter.java`**

```java
package com.ota.cloud.gateway.filter;

import com.ota.cloud.common.constant.CommonConstant;
import com.ota.cloud.common.util.TraceIdUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * TraceId 过滤器
 *
 * 在请求链路的入口生成 TraceId。
 * 如果前端已传入 TraceId，则复用（全链路追踪场景）。
 * 优先级低于 AuthGlobalFilter，确保认证之后透传。
 */
@Slf4j
@Component
public class TraceIdFilter implements GlobalFilter, Ordered {

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        ServerHttpRequest request = exchange.getRequest();

        // 尝试从请求头获取 TraceId
        String traceId = request.getHeaders()
                .getFirst(CommonConstant.HEADER_TRACE_ID);

        // 如果没有，则生成新的
        if (traceId == null || traceId.isEmpty()) {
            traceId = TraceIdUtil.generate();
        }

        // 将 TraceId 放入请求头透传
        ServerHttpRequest mutatedRequest = request.mutate()
                .header(CommonConstant.HEADER_TRACE_ID, traceId)
                .build();

        log.debug("TraceId: {}", traceId);

        return chain.filter(exchange.mutate().request(mutatedRequest).build());
    }

    @Override
    public int getOrder() {
        // 在 AuthGlobalFilter（-100）之前执行
        return -200;
    }
}
```

---

## 2.6 验证步骤

### Step 1：启动所有服务

```bash
# 1. 确保基础设施已启动
docker compose up -d

# 2. 启动三个服务（在各自模块目录或根目录执行）
# 启动 ota-auth（新开终端）
cd ota-auth
mvn spring-boot:run

# 启动 ota-user（新开终端）
cd ota-user
mvn spring-boot:run

# 启动 ota-gateway（新开终端）
cd ota-gateway
mvn spring-boot:run
```

> **💡 小贴士：** 可以在 IDEA 中直接运行三个 Application 类。注意 JDK 21 虚拟线程默认启用，Spring Boot 3.x 自动配置。

### Step 2：用户注册

```bash
curl -X POST http://localhost:8080/api/user/register \
  -H "Content-Type: application/json" \
  -d '{
    "username": "testuser",
    "password": "123456",
    "phone": "13800138000",
    "nickname": "测试用户"
  }'
```

**预期响应：**
```json
{
  "code": 200,
  "message": "success",
  "data": {
    "id": 2,
    "username": "testuser",
    "phone": "13800138000",
    "nickname": "测试用户",
    "status": 1,
    "createTime": "2025-05-24 12:00:00"
  },
  "timestamp": 1748088000000
}
```

### Step 3：用户登录（获取 Token）

```bash
curl -X POST http://localhost:8080/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "username": "testuser",
    "password": "123456"
  }'
```

**预期响应：**
```json
{
  "code": 200,
  "message": "success",
  "data": {
    "token": "eyJhbGciOiJIUzI1NiJ9.eyJ1c2VySWQiOjIsInVzZXJuYW1lIjoidGVzdHVzZXIiLCJzdWIiOiIyIiwiaWF0IjoxNzQ4MDg4MDAwLCJleHAiOjE3NDgxNzQ0MDB9.xxx",
    "tokenType": "Bearer",
    "expiresIn": 900
  },
  "timestamp": 1748088000000
}
```

保存 Token 到环境变量以便后续使用：
```bash
export TOKEN="eyJ...（上面返回的完整token）"
```

### Step 4：携带 Token 访问受保护接口

```bash
curl -X GET http://localhost:8080/api/user/info \
  -H "Authorization: Bearer $TOKEN"
```

**预期响应：**
```json
{
  "code": 200,
  "message": "success",
  "data": {
    "id": 2,
    "username": "testuser",
    "phone": "13800138000",
    "nickname": "测试用户",
    "status": 1,
    "createTime": "2025-05-24 12:00:00"
  },
  "timestamp": 1748088000000
}
```

### Step 5：不带 Token 访问（验证拦截）

```bash
curl -X GET http://localhost:8080/api/user/info
```

**预期响应（401）：**
```json
{
  "code": 401,
  "message": "Missing or invalid Authorization header",
  "timestamp": 1748088000000
}
```

### Step 6：登出后验证 Token 失效

```bash
# 登出
curl -X POST http://localhost:8080/api/auth/logout \
  -H "Authorization: Bearer $TOKEN"

# 再次访问受保护接口（应返回401）
curl -X GET http://localhost:8080/api/user/info \
  -H "Authorization: Bearer $TOKEN"
```

**预期响应：**
```json
{
  "code": 401,
  "message": "Token has been revoked",
  "timestamp": 1748088000000
}
```

### Step 7：验证 TraceId 透传

在 ota-user 的日志中，能看到请求头包含 `X-OTA-TraceId`、`X-OTA-UserId`、`X-OTA-Username`：

```
2025-05-24 12:00:00.123 DEBUG [,,,] 12345 --- [ctor-http-nio-2] c.o.c.u.UserController
: GET /user/info - userId=2, username=testuser, traceId=abc123def456...
```

### Step 8：验证 Nacos 服务注册

访问 Nacos 控制台 http://localhost:8848/nacos → **服务管理 → 服务列表**

预期看到三个服务已注册：
- `ota-gateway`
- `ota-auth`
- `ota-user`

---

## 2.7 常见坑

### ❌ 坑1：Gateway 启动报错 `ClassNotFoundException` 或 Web 冲突

**现象：**
```
The bean 'characterEncodingFilter', defined in ... could not be registered.
```

**原因：** Gateway 基于 WebFlux，不能同时引入 `spring-boot-starter-web`。

**解决：**
```xml
<!-- ota-common 在 Gateway 中引入时排除 Web -->
<dependency>
    <groupId>com.ota.cloud</groupId>
    <artifactId>ota-common</artifactId>
    <exclusions>
        <exclusion>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```

### ❌ 坑2：JWT 签名失败

**现象：**
```
io.jsonwebtoken.security.SignatureException: JWT signature does not match
```

**原因：** 签名密钥在不同服务中不一致。

**解决：** Gateway 和 ota-auth 使用相同的 `JWT_SECRET` 常量，确保两个服务的密钥一致。生产环境建议使用 RSA 密钥对，auth 服务持有私钥签名，Gateway 持有公钥验签。

### ❌ 坑3：CORS 跨域问题

**现象：** 前端浏览器调用 API 时出现跨域错误。

**解决：** Gateway 已配置全局 CORS：
```yaml
spring:
  cloud:
    gateway:
      globalcors:
        cors-configurations:
          '[/**]':
            allowedOriginPatterns: "*"
            allowedMethods: "*"
            allowedHeaders: "*"
```

如果还需要更精细的控制，可以在 Gateway 中自定义 CorsWebFilter：
```java
@Bean
public CorsWebFilter corsWebFilter() {
    CorsConfiguration config = new CorsConfiguration();
    config.addAllowedOriginPattern("*");
    config.addAllowedMethod("*");
    config.addAllowedHeader("*");
    config.setAllowCredentials(true);
    UrlBasedCorsConfigurationSource source =
            new UrlBasedCorsConfigurationSource(new PathPatternParser());
    source.registerCorsConfiguration("/**", config);
    return new CorsWebFilter(source);
}
```

### ❌ 坑4：BCrypt 密码匹配失败

**现象：** `passwordEncoder.matches()` 始终返回 false。

**原因：**
1. 注册时加密方式与登录时不一致
2. 数据库中密码字段长度不够（BCrypt 输出长达60字符）

**解决：**
- 确保密码字段类型为 `VARCHAR(256)` 或以上
- 确保注册和登录使用同一个 `PasswordEncoder` bean

### ❌ 坑5：Redis 连接失败导致 Gateway 启动失败

**现象：** Gateway 启动报 Redis 连接超时。

**解决：**
- 确认 Docker Redis 已启动：`docker ps | grep redis`
- 检查密码是否正确：`redis-cli -a redis123456 ping`
- 如果 Redis 不是强依赖，可以配置连接池超时重试：
```yaml
spring:
  data:
    redis:
      timeout: 3000ms
      lettuce:
        pool:
          max-active: 8
          max-idle: 4
          min-idle: 0
```

### ❌ 坑6：Maven 编译时 MyBatis-Plus 报错

**现象：** `java.lang.NoClassDefFoundError: com/baomidou/mybatisplus/...`

**解决：** 确认父 POM 中 MyBatis-Plus 版本为 `3.5.16`，且各个子模块都在父 POM 的 `<modules>` 中声明。

### ❌ 坑7：Spring Cloud Gateway 的 `lb://` 路由解析失败

**现象：**
```
org.springframework.cloud.gateway.support.NotFoundException: Unable to find instance for ota-auth
```

**原因：** 服务未成功注册到 Nacos。

**解决：**
1. 检查 Nacos 控制台是否能看到服务列表
2. 确认 `bootstrap.yml` 中 Nacos 地址和命名空间配置正确
3. 确认 `@EnableDiscoveryClient` 注解已添加

### ❌ 坑8：虚拟线程 + Gateway（WebFlux）的注意点

**注意：** Spring Cloud Gateway 基于 Reactor（WebFlux），本身是事件驱动的，**不会**使用虚拟线程处理请求。虚拟线程主要在传统 Servlet 容器（Tomcat + Spring MVC）中生效。Gateway 的过滤器中不要使用 `ThreadLocal`，应使用 Reactor Context 或直接通过请求头传递。

---

## 2.8 面试追问深度解析

### Q1：为什么 access_token 只设 15 分钟？

这是 **安全与性能的平衡**：

| 因素 | 说明 |
|------|------|
| **泄露风险** | Token 在客户端存储（localStorage / Cookie），XS/CSRF 可能导致泄露。短 TTL 缩小攻击窗口 |
| **黑名单兜底** | 登出时将 jti 加入 Redis 黑名单。但若 Redis 挂了（见 2.2.1 降级策略），过期时间是最終安全兜底 |
| **refresh_token** | 15 分钟足够短，用户几乎无感知 refresh 流程。P1 引入 refresh_token rotation 后，access_token 可以更短（如 5 分钟） |
| **性能** | 短 TTL 不代表频繁登录——有 refresh_token 自动续期，对用户透明 |

**面试话术：** 「access_token 15 分钟 = 安全窗口。即使 token 泄露，攻击者最多用 15 分钟。登出后靠黑名单 + TTL 双保险。」

### Q2：access_token 泄露了怎么处理？

**步骤：**
1. **立即将 jti 加入 Redis 黑名单** — 调用 `/auth/logout` 或管理后台的踢人接口
2. **15 分钟内必然失效** — 即使黑名单查不到，短 TTL 确保 Token 自然过期
3. **安全通知用户** — 检测到异常登录行为时触发密码重置流程

**代码层面：** `AuthGlobalFilter` 中通过 `claims.getId()` 拿到 jti，黑名单检查在 Redis 中进行（降级时依赖 TTL）。

> **深度追问：** 黑名单为什么用 jti 而不是整个 token？
> 答：jti 是 JWT 的 ID（UUID），用 jti 做 key 更短、可控。整个 token 字符串长且包含敏感信息，不适合做 Redis key。

### Q3：refresh_token rotation 解决什么安全问题？

**核心问题：** 静态 refresh_token 存在重放风险。

| 场景 | 静态 refresh_token | rotation |
|------|------------------|----------|
| refresh_token 被窃取 | 攻击者可无限续期 | 攻击者拿到旧 token 时已被轮换，无效 |
| 正常用户同时刷新 | 两处刷新都成功，两方都可用 | 后刷新的使前一个失效，串号不可能 |
| 长期未使用 | token 7 天后过期，无风险 | 同左 |

**实现要点（P1 待实现）：**
```
POST /auth/refresh
Request: { refreshToken: "old_refresh_token" }
Response: {
  accessToken: "new_access_token",      // 15 分钟
  refreshToken: "new_refresh_token",    // 7 天
  expiresIn: 900
}
// 服务端操作：验证旧 refresh_token → 删除 → 签发新对
```

### Q4：JWT 在网关层校验了，auth 服务自己还需要校验吗？

**需要，但方式不同。**

| 层 | 校验方式 | 原因 |
|----|---------|------|
| **Gateway** | 本地解析 JWT（验签 + 有效期），必要时查黑名单 | 拒绝非法请求进入内网，降低 auth 负载 |
| **Auth 服务** | `/auth/validate` 接口保留做 Redis 黑名单检查 + 权限上下文 | 统一的黑名单入口；如果未来有 RBAC/ABAC，auth 可以返回完整权限树 |

**设计原则：** Gateway 负责「能不能进」，auth 负责「能做什么」。Gateway 做「准入校验」，auth 做「权限校验」。

### Q5：网关是单点挂了怎么办？

网关是流量入口，确实是架构中的关键路径。

**高可用方案（按部署环境）：**

| 方案 | 说明 | 适用场景 |
|------|------|---------|
| **多实例 + LB** | 部署 2+ 个 Gateway 实例，前端负载均衡器分发 | 大多数生产环境 |
| **K8s + HPA** | K8s Deployment 多副本 + 水平自动伸缩 | 云原生 |
| **DNS 轮询** | 多个 Gateway 公网 IP，DNS 随机解析 | 轻量方案（存在缓存延迟） |
| **Keepalived VIP** | 主备模式，VIP 漂移 | 传统物理机部署 |

**本章单实例**仅用于开发和演示。生产环境最低 2 副本 + Nginx/ALB 负载均衡。

### Q6：黑名单在 Redis 中的内存占用评估

**假设：**
- 每日活跃用户（DAU）：10 万
- 每次登录产生一个 jti（UUID: 36 字节 key + "1" 1 字节 value）
- 黑名单 TTL = JWT 过期时间 = 15 分钟

**计算：**
```
每个黑名单条目 ≈ 50 bytes（key + value + 元数据）
15 分钟内最大在线 ≈ 10 万 * 15/1440 ≈ 1041 人
最大黑名单条目 ≈ 1041 * 1 = 1041（仅登出的 Token 进入黑名单）
内存占用 ≈ 1041 * 50 bytes ≈ 52 KB
```

**结论：** 黑名单内存占用极小（~52 KB / 10 万 DAU），Redis 内存几乎不受影响。如果担心极端场景（如批量踢人），可以对黑名单 key 设置更短的 TTL（如 5 分钟）进一步降低。

---

## 2.9 本章小结

| 产出 | 文件 | 端口 |
|------|------|------|
| API 网关 | `ota-gateway/` | 8080 |
| 认证服务 | `ota-auth/` | 8081 |
| 用户服务 | `ota-user/` | 8082 |
| JWT 认证 | JwtAuthGlobalFilter（本地验签+Redis黑名单） | — |
| 白名单机制 | WhiteListConfig | — |
| TraceId 透传 | TraceIdFilter | — |
| 双 Token（P1） | access_token（15min）+ refresh_token rotation | — |

**完整流程验证：**

```
注册 → 登录 → 获取Token → 访问受保护接口 → 登出 → Token失效
  ✔       ✔         ✔              ✔              ✔         ✔
```

**下一章预告：** 第3章将实现酒店模块（ota-hotel）的 CRUD 与缓存策略（Cache-Aside + 空值缓存 + 短TTL随机抖动），敬请期待。

---

## 2.10 附录：完整的 curl 测试脚本

**文件：`test-auth-flow.sh`**

```bash
#!/bin/bash
# 认证流程完整测试脚本

GATEWAY_URL="http://localhost:8080"

echo "===== 1. 健康检查 ====="
curl -s "$GATEWAY_URL/api/auth/health"

echo -e "\n\n===== 2. 注册用户 ====="
curl -s -X POST "$GATEWAY_URL/api/user/register" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "testuser_'$(date +%s)'",
    "password": "123456",
    "phone": "1380013'$(date +%s | tail -c 5)'",
    "nickname": "测试用户"
  }'

echo -e "\n\n===== 3. 登录 ====="
TOKEN=$(curl -s -X POST "$GATEWAY_URL/api/auth/login" \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin123"}' | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['data']['token'])")

echo "Token: ${TOKEN:0:50}..."

echo -e "\n===== 4. 访问受保护接口（带Token） ====="
curl -s "$GATEWAY_URL/api/user/info" \
  -H "Authorization: Bearer $TOKEN"

echo -e "\n\n===== 5. 不带Token访问（预期401） ====="
curl -s "$GATEWAY_URL/api/user/info"

echo -e "\n\n===== 6. 带无效Token访问（预期401） ====="
curl -s "$GATEWAY_URL/api/user/info" \
  -H "Authorization: Bearer invalid_token_here"

echo -e "\n\n===== 7. 登出 ====="
curl -s -X POST "$GATEWAY_URL/api/auth/logout" \
  -H "Authorization: Bearer $TOKEN"

echo -e "\n\n===== 8. 登出后再次访问（预期401） ====="
curl -s "$GATEWAY_URL/api/user/info" \
  -H "Authorization: Bearer $TOKEN"

echo -e "\n\n===== 测试完成 ====="
```
