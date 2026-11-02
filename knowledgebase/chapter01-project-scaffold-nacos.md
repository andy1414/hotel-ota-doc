# 第1章：项目骨架搭建与Nacos注册配置

> **工期估算：** 1天（P0-1）
>
> **目标读者：** 有Java经验的开发者
>
> **本章产出：** 可运行的父POM、common模块、Nacos+MySQL+Redis基础设施

---

## 1.1 本章目标

- 搭建 Maven 多模块父工程（Spring Boot 3.5.14 + Spring Cloud 2025.0.x + SCA 2025.0.x）
- 创建 `ota-common` 模块：统一响应体、全局异常处理、DTO 基类、常量
- 通过 Docker Compose 启动 Nacos 3.1.1 + MySQL 8.0 + Redis 7.x
- 配置 Nacos 命名空间和 Data ID 分组规则
- 验证：Nacos 控制台能看到服务和配置

---

## 1.2 整体架构

```
ota-cloud/
├── pom.xml                    ← 父POM（本工程）
├── ota-common/                ← 公共模块（本工程）
├── ota-gateway/               ← 网关（第2章）
├── ota-auth/                  ← 认证（第2章）
├── ota-user/                  ← 用户（第2章）
├── ota-hotel/                 ← 酒店（后续）
├── ota-order/                 ← 订单（后续）
├── ota-inventory/             ← 库存（后续）
└── ota-ai/                    ← AI（后续）
```

基础设施层（Docker Compose）：

```
nacos:3.1.1  ──►  mysql:8.0
redis:7.x
P1 可选：kafka + zipkin + seata + sentinel
P2 可选：prometheus/grafana + elasticsearch/logstash/kibana
```

---

## 1.3 父 POM 配置

父 POM 是整个项目的依赖版本管理中枢。所有子模块继承此 POM，无需再声明版本号。

### 关键版本

| 组件 | 版本 | 说明 |
|------|------|------|
| Spring Boot | 3.5.14 | 稳定主线 |
| Spring Cloud | 2025.0.x | 对应 `2025.0.0` |
| Spring Cloud Alibaba | 2025.0.x | 对应 `2025.0.0` |
| JDK | 21+ | 必须启用虚拟线程 |
| MyBatis-Plus | 3.5.16 | ORM 框架 |
| MySQL Connector | 8.0.33 | JDBC 驱动 |
| Druid | 1.2.24 | 连接池 |

> **💡 版本对应关系：** Spring Cloud Alibaba 的版本号需与 Spring Cloud 版本严格对应。2025.0.x 主线是最新的稳定系列。

### 文件：`pom.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.ota.cloud</groupId>
    <artifactId>ota-cloud</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <packaging>pom</packaging>

    <name>OTA Cloud</name>
    <description>OTA Cloud Microservices Platform</description>

    <modules>
        <module>ota-common</module>
        <module>ota-gateway</module>
        <module>ota-auth</module>
        <module>ota-user</module>
        <module>ota-hotel</module>
        <module>ota-order</module>
        <module>ota-inventory</module>
        <module>ota-ai</module>
    </modules>

    <!-- 统一版本管理 -->
    <properties>
        <java.version>21</java.version>
        <maven.compiler.source>21</maven.compiler.source>
        <maven.compiler.target>21</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>

        <!-- 框架版本 -->
        <spring-boot.version>3.5.14</spring-boot.version>
        <spring-cloud.version>2025.0.0</spring-cloud.version>
        <spring-cloud-alibaba.version>2025.0.0</spring-cloud-alibaba.version>

        <!-- 中间件客户端 -->
        <mybatis-plus.version>3.5.16</mybatis-plus.version>
        <mysql-connector.version>8.0.33</mysql-connector.version>
        <druid.version>1.2.24</druid.version>
        <jedis.version>5.2.0</jedis.version>

        <!-- 工具 -->
        <lombok.version>1.18.36</lombok.version>
        <hutool.version>5.8.34</hutool.version>
        <mapstruct.version>1.6.3</mapstruct.version>
        <jackson.version>2.18.3</jackson.version>

        <!-- JWT -->
        <jjwt.version>0.12.6</jjwt.version>

        <!-- 插件 -->
        <maven-compiler-plugin.version>3.13.0</maven-compiler-plugin.version>
    </properties>

    <!-- 依赖版本管理 -->
    <dependencyManagement>
        <dependencies>
            <!-- Spring Boot BOM -->
            <dependency>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-dependencies</artifactId>
                <version>${spring-boot.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>

            <!-- Spring Cloud BOM -->
            <dependency>
                <groupId>org.springframework.cloud</groupId>
                <artifactId>spring-cloud-dependencies</artifactId>
                <version>${spring-cloud.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>

            <!-- Spring Cloud Alibaba BOM -->
            <dependency>
                <groupId>com.alibaba.cloud</groupId>
                <artifactId>spring-cloud-alibaba-dependencies</artifactId>
                <version>${spring-cloud-alibaba.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>

            <!-- MyBatis-Plus -->
            <dependency>
                <groupId>com.baomidou</groupId>
                <artifactId>mybatis-plus-spring-boot3-starter</artifactId>
                <version>${mybatis-plus.version}</version>
            </dependency>

            <!-- MySQL -->
            <dependency>
                <groupId>com.mysql</groupId>
                <artifactId>mysql-connector-j</artifactId>
                <version>${mysql-connector.version}</version>
            </dependency>

            <!-- Druid -->
            <dependency>
                <groupId>com.alibaba</groupId>
                <artifactId>druid-spring-boot-3-starter</artifactId>
                <version>${druid.version}</version>
            </dependency>

            <!-- Redis -->
            <dependency>
                <groupId>redis.clients</groupId>
                <artifactId>jedis</artifactId>
                <version>${jedis.version}</version>
            </dependency>

            <!-- Lombok -->
            <dependency>
                <groupId>org.projectlombok</groupId>
                <artifactId>lombok</artifactId>
                <version>${lombok.version}</version>
                <scope>provided</scope>
            </dependency>

            <!-- Hutool -->
            <dependency>
                <groupId>cn.hutool</groupId>
                <artifactId>hutool-all</artifactId>
                <version>${hutool.version}</version>
            </dependency>

            <!-- MapStruct -->
            <dependency>
                <groupId>org.mapstruct</groupId>
                <artifactId>mapstruct</artifactId>
                <version>${mapstruct.version}</version>
            </dependency>
            <dependency>
                <groupId>org.mapstruct</groupId>
                <artifactId>mapstruct-processor</artifactId>
                <version>${mapstruct.version}</version>
                <scope>provided</scope>
            </dependency>

            <!-- JWT -->
            <dependency>
                <groupId>io.jsonwebtoken</groupId>
                <artifactId>jjwt-api</artifactId>
                <version>${jjwt.version}</version>
            </dependency>
            <dependency>
                <groupId>io.jsonwebtoken</groupId>
                <artifactId>jjwt-impl</artifactId>
                <version>${jjwt.version}</version>
                <scope>runtime</scope>
            </dependency>
            <dependency>
                <groupId>io.jsonwebtoken</groupId>
                <artifactId>jjwt-jackson</artifactId>
                <version>${jjwt.version}</version>
                <scope>runtime</scope>
            </dependency>

            <!-- 内部模块 -->
            <dependency>
                <groupId>com.ota.cloud</groupId>
                <artifactId>ota-common</artifactId>
                <version>${project.version}</version>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <build>
        <pluginManagement>
            <plugins>
                <plugin>
                    <groupId>org.springframework.boot</groupId>
                    <artifactId>spring-boot-maven-plugin</artifactId>
                    <version>${spring-boot.version}</version>
                    <configuration>
                        <excludes>
                            <exclude>
                                <groupId>org.projectlombok</groupId>
                                <artifactId>lombok</artifactId>
                            </exclude>
                        </excludes>
                    </configuration>
                </plugin>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-compiler-plugin</artifactId>
                    <version>${maven-compiler-plugin.version}</version>
                    <configuration>
                        <source>${java.version}</source>
                        <target>${java.version}</target>
                        <parameters>true</parameters>
                        <annotationProcessorPaths>
                            <path>
                                <groupId>org.projectlombok</groupId>
                                <artifactId>lombok</artifactId>
                                <version>${lombok.version}</version>
                            </path>
                            <path>
                                <groupId>org.mapstruct</groupId>
                                <artifactId>mapstruct-processor</artifactId>
                                <version>${mapstruct.version}</version>
                            </path>
                        </annotationProcessorPaths>
                    </configuration>
                </plugin>
            </plugins>
        </pluginManagement>
    </build>
</project>
```

### 版本管理的三种方式

1. **BOM import**：Spring Boot / Cloud / Alibaba 通过 `scope=import type=pom` 导入
2. **显式声明**：MyBatis-Plus、Druid 等非 BOM 管理的依赖显式声明版本
3. **内部模块**：`ota-common` 等内部模块统一管理版本

---

## 1.4 ota-common 公共模块

公共模块是所有微服务共享的基础设施，不包含业务逻辑。

### 1.4.1 POM 配置

**文件：`ota-common/pom.xml`**

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

    <artifactId>ota-common</artifactId>
    <packaging>jar</packaging>
    <description>OTA Cloud Common Module</description>

    <dependencies>
        <!-- Spring Boot Web（仅用于注解，不嵌入容器） -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
            <scope>provided</scope>
        </dependency>

        <!-- Spring Validation -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-validation</artifactId>
            <scope>provided</scope>
        </dependency>

        <!-- Lombok -->
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
        </dependency>

        <!-- Hutool -->
        <dependency>
            <groupId>cn.hutool</groupId>
            <artifactId>hutool-all</artifactId>
        </dependency>

        <!-- Jackson -->
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>
        <dependency>
            <groupId>com.fasterxml.jackson.datatype</groupId>
            <artifactId>jackson-datatype-jsr310</artifactId>
        </dependency>
    </dependencies>
</project>
```

### 1.4.2 统一响应体 `Result<T>`

所有接口统一返回此结构，前端/网关据此判断请求结果。

**文件：`ota-common/src/main/java/com/ota/cloud/common/model/Result.java`**

```java
package com.ota.cloud.common.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.io.Serializable;

/**
 * 统一响应体
 *
 * @param <T> 数据泛型
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Result<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 状态码 */
    private int code;

    /** 消息 */
    private String message;

    /** 数据 */
    private T data;

    /** 请求追踪ID */
    private String traceId;

    /** 时间戳 */
    private long timestamp;

    private Result() {
        this.timestamp = System.currentTimeMillis();
    }

    // ========== 成功 ==========

    public static <T> Result<T> success() {
        return success(null);
    }

    public static <T> Result<T> success(T data) {
        Result<T> result = new Result<>();
        result.code = 200;
        result.message = "success";
        result.data = data;
        return result;
    }

    // ========== 失败 ==========

    public static <T> Result<T> error(int code, String message) {
        Result<T> result = new Result<>();
        result.code = code;
        result.message = message;
        return result;
    }

    public static <T> Result<T> error(String message) {
        return error(500, message);
    }

    // ========== 便捷方法 ==========

    public boolean isSuccess() {
        return this.code == 200;
    }

    // ========== Builder ==========

    public Result<T> code(int code) {
        this.code = code;
        return this;
    }

    public Result<T> message(String message) {
        this.message = message;
        return this;
    }

    public Result<T> data(T data) {
        this.data = data;
        return this;
    }

    public Result<T> traceId(String traceId) {
        this.traceId = traceId;
        return this;
    }
}
```

### 1.4.3 业务异常类

**文件：`ota-common/src/main/java/com/ota/cloud/common/exception/BusinessException.java`**

```java
package com.ota.cloud.common.exception;

import lombok.Getter;

/**
 * 业务异常
 *
 * 所有业务层面的异常统一使用此类，而非直接抛出 RuntimeException。
 * 配合全局异常处理器自动转换为标准 Result 响应。
 */
@Getter
public class BusinessException extends RuntimeException {

    private final int code;

    public BusinessException(int code, String message) {
        super(message);
        this.code = code;
    }

    public BusinessException(String message) {
        this(500, message);
    }

    /**
     * 常用错误码常量
     */
    public static class ErrorCode {
        public static final int BAD_REQUEST = 400;
        public static final int UNAUTHORIZED = 401;
        public static final int FORBIDDEN = 403;
        public static final int NOT_FOUND = 404;
        public static final int CONFLICT = 409;
        public static final int INTERNAL_ERROR = 500;
        public static final int SERVICE_UNAVAILABLE = 503;

        private ErrorCode() {}
    }
}
```

### 1.4.4 全局异常处理器

**文件：`ota-common/src/main/java/com/ota/cloud/common/exception/GlobalExceptionHandler.java`**

```java
package com.ota.cloud.common.exception;

import com.ota.cloud.common.model.Result;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.stream.Collectors;

/**
 * 全局异常处理器
 *
 * 统一将各种异常转换为 Result 响应，避免将堆栈信息暴露给前端。
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    /**
     * 业务异常处理
     */
    @ExceptionHandler(BusinessException.class)
    public Result<Void> handleBusinessException(BusinessException e, HttpServletRequest request) {
        log.warn("业务异常 [{}]: {} | URI: {}", e.getCode(), e.getMessage(), request.getRequestURI());
        return Result.error(e.getCode(), e.getMessage());
    }

    /**
     * 参数校验异常（@Valid + @RequestBody）
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Result<Void> handleValidationException(MethodArgumentNotValidException e) {
        String message = e.getBindingResult().getFieldErrors().stream()
                .map(FieldError::getDefaultMessage)
                .collect(Collectors.joining(", "));
        log.warn("参数校验失败: {}", message);
        return Result.error(400, message);
    }

    /**
     * 参数校验异常（@RequestParam / @PathVariable）
     */
    @ExceptionHandler(ConstraintViolationException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Result<Void> handleConstraintViolation(ConstraintViolationException e) {
        log.warn("参数校验失败: {}", e.getMessage());
        return Result.error(400, e.getMessage());
    }

    /**
     * 缺少请求参数
     */
    @ExceptionHandler(MissingServletRequestParameterException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Result<Void> handleMissingParam(MissingServletRequestParameterException e) {
        log.warn("缺少请求参数: {}", e.getParameterName());
        return Result.error(400, "缺少必要参数: " + e.getParameterName());
    }

    /**
     * 兜底异常处理
     */
    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public Result<Void> handleException(Exception e, HttpServletRequest request) {
        log.error("系统异常 URI: {}", request.getRequestURI(), e);
        return Result.error(500, "系统繁忙，请稍后重试");
    }
}
```

### 1.4.5 BaseDTO —— DTO 基类

所有数据传输对象（DTO）继承此类，统一序列化支持。

**文件：`ota-common/src/main/java/com/ota/cloud/common/model/BaseDTO.java`**

```java
package com.ota.cloud.common.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * DTO 基类
 *
 * 所有前端请求/响应 DTO 继承此类，统一时间格式和序列化配置。
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class BaseDTO implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 创建时间 */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createTime;

    /** 更新时间 */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime updateTime;
}
```

### 1.4.6 常量类

**文件：`ota-common/src/main/java/com/ota/cloud/common/constant/CommonConstant.java`**

```java
package com.ota.cloud.common.constant;

/**
 * 系统通用常量
 */
public final class CommonConstant {

    private CommonConstant() {}

    /* ========== JWT 相关 ========== */
    public static final String JWT_HEADER = "Authorization";
    public static final String JWT_PREFIX = "Bearer ";
    public static final String JWT_SECRET = "ota-cloud-jwt-secret-key-must-be-at-least-256-bits-long-for-hs256";
    public static final long JWT_EXPIRATION = 86400L;           // 24小时（秒）
    public static final long JWT_BLACKLIST_TTL = 86400L;        // 黑名单 TTL（秒）

    /* ========== 请求头 ========== */
    public static final String HEADER_USER_ID = "X-OTA-UserId";
    public static final String HEADER_USERNAME = "X-OTA-Username";
    public static final String HEADER_TRACE_ID = "X-OTA-TraceId";

    /* ========== Redis Key 前缀 ========== */
    public static final String REDIS_KEY_JWT_BLACKLIST = "jwt:blacklist:";
    public static final String REDIS_KEY_HOTEL_DETAIL = "hotel:detail:";
    public static final String REDIS_KEY_PRICE_STOCK = "ps:";

    /* ========== Kafka Topic ========== */
    public static final String TOPIC_ORDER_PAID = "order-paid";
    public static final String TOPIC_ORDER_CANCELLED = "order-cancelled";
    public static final String TOPIC_INVENTORY_CHANGED = "inventory-changed";
    public static final String TOPIC_CACHE_DELETE_RETRY = "cache-delete-retry";

    /* ========== 通用 ========== */
    public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String DATE_FORMAT_DATE = "yyyy-MM-dd";
    public static final int DEFAULT_PAGE_SIZE = 20;
    public static final int MAX_PAGE_SIZE = 100;
}
```

### 1.4.7 TraceId 生成工具

**文件：`ota-common/src/main/java/com/ota/cloud/common/util/TraceIdUtil.java`**

```java
package com.ota.cloud.common.util;

import java.util.UUID;

/**
 * TraceId 工具类
 *
 * 用于请求全链路的追踪，透传至各微服务。
 */
public final class TraceIdUtil {

    private static final ThreadLocal<String> TRACE_ID_HOLDER = new ThreadLocal<>();

    private TraceIdUtil() {}

    /**
     * 生成 TraceId（32位小写UUID，去横线）
     */
    public static String generate() {
        return UUID.randomUUID().toString().replace("-", "").toLowerCase();
    }

    /**
     * 设置当前线程的 TraceId
     */
    public static void set(String traceId) {
        TRACE_ID_HOLDER.set(traceId);
    }

    /**
     * 获取当前线程的 TraceId
     */
    public static String get() {
        return TRACE_ID_HOLDER.get();
    }

    /**
     * 清除当前线程的 TraceId（务必在 finally 中调用）
     */
    public static void remove() {
        TRACE_ID_HOLDER.remove();
    }
}
```

> **⚠️ 注意：** `ThreadLocal` 在虚拟线程（Virtual Threads）环境下需要特别小心。JDK 21 的虚拟线程不支持 `ThreadLocal` 中的 InheritableThreadLocal 语义，但普通的 ThreadLocal 仍然可用。后续在 Gateway 中，我们会使用 Reactor Context 替代 ThreadLocal 实现跨 Filter 的 TraceId 透传。

---

## 1.5 Docker Compose 基础环境

### 1.5.1 Docker 安装方式

本章只依赖 Docker Engine 和 Docker Compose。优先使用 Docker Desktop；如果是 Linux 服务器，也可以只安装 Docker Engine。

#### 方式一：Windows 安装 Docker Desktop（推荐本地开发）

适合 Windows 10/11 开发机。Docker Desktop 会自动带上 Docker Engine 和 Docker Compose。

1. 开启 WSL2：

```powershell
wsl --install
wsl --set-default-version 2
```

2. 安装 Docker Desktop：

```powershell
winget install Docker.DockerDesktop
```

3. 启动 Docker Desktop 后验证：

```powershell
docker --version
docker compose version
docker run --rm hello-world
```

4. 如果通过 SSH 操作 Windows Docker Desktop，公共镜像拉取可能被 Windows 凭据助手拦住。可先在 Windows 桌面登录一次 Docker Desktop，或在本机拉取镜像后导入远端：

```bash
docker pull --platform linux/amd64 mysql:8.0
docker save mysql:8.0 | ssh administrator@100.99.175.108 "docker load"
```

#### 方式二：macOS 安装 Docker Desktop

适合 Mac 本机开发。

```bash
brew install --cask docker
open -a Docker
docker --version
docker compose version
docker run --rm hello-world
```

#### 方式三：Ubuntu 安装 Docker Engine

适合 Linux 服务器或云主机。

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker $USER
```

重新登录 shell 后验证：

```bash
docker --version
docker compose version
docker run --rm hello-world
```

#### 方式四：Linux 一键脚本（实验环境）

适合临时开发机，不建议生产环境直接使用。

```bash
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER
```

重新登录后执行：

```bash
docker run --rm hello-world
```

#### 常用 Docker 命令

```bash
# 查看 Docker 状态
docker version
docker info

# 查看容器
docker ps
docker ps -a

# 查看日志
docker logs -f ota-nacos
docker logs --tail 100 ota-mysql

# 进入容器
docker exec -it ota-mysql bash
docker exec -it ota-redis redis-cli -a redis123456

# Compose 启停
docker compose up -d
docker compose ps
docker compose logs -f nacos
docker compose down

# 如果要删除数据卷并重新初始化数据库，才使用 -v
docker compose down -v
```

### 文件：`docker-compose.yml`

```yaml
networks:
  ota-network:
    driver: bridge

services:
  # ==================== MySQL 8.0 ====================
  mysql:
    image: mysql:8.0
    container_name: ota-mysql
    restart: unless-stopped
    environment:
      MYSQL_ROOT_PASSWORD: root123456
      MYSQL_DATABASE: ota_cloud
      TZ: Asia/Shanghai
    ports:
      - "3306:3306"
    volumes:
      - mysql-data:/var/lib/mysql
      - ./init-sql:/docker-entrypoint-initdb.d    # 初始化SQL
    command:
      --default-authentication-plugin=mysql_native_password
      --character-set-server=utf8mb4
      --collation-server=utf8mb4_unicode_ci
      --max_connections=1000
      --innodb_buffer_pool_size=1G
    networks:
      - ota-network
    healthcheck:
      test: [ "CMD", "mysqladmin", "ping", "-h", "localhost", "-uroot", "-proot123456" ]
      interval: 10s
      timeout: 5s
      retries: 5

  # ==================== Redis 7.x ====================
  redis:
    image: redis:7
    container_name: ota-redis
    restart: unless-stopped
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
    command: redis-server --requirepass redis123456 --appendonly yes
    networks:
      - ota-network
    healthcheck:
      test: [ "CMD", "redis-cli", "-a", "redis123456", "ping" ]
      interval: 10s
      timeout: 5s
      retries: 5

  # ==================== Nacos 3.1.1 ====================
  nacos:
    image: nacos/nacos-server:v3.1.1
    container_name: ota-nacos
    restart: unless-stopped
    environment:
      MODE: standalone
      JVM_XMS: 512m
      JVM_XMX: 512m
      # 使用MySQL存储（默认使用内置Derby，生产建议切换MySQL）
      SPRING_DATASOURCE_PLATFORM: mysql
      MYSQL_SERVICE_HOST: mysql
      MYSQL_SERVICE_PORT: 3306
      MYSQL_SERVICE_DB_NAME: ota_nacos
      MYSQL_SERVICE_USER: root
      MYSQL_SERVICE_PASSWORD: root123456
      NACOS_SERVER_PORT: 8848
      # Nacos 3.x 必填：鉴权 token 必须是 Base64 字符串
      NACOS_AUTH_TOKEN: e4lSMfb6WeShX52uxJqhuIs/fKOIfRqSlaTQeoczR2s=
      NACOS_AUTH_IDENTITY_KEY: serverIdentity
      NACOS_AUTH_IDENTITY_VALUE: ota-cloud
    ports:
      - "18080:8080"       # Nacos Console 端口
      - "8848:8848"
      - "9848:9848"        # gRPC 端口
      - "9849:9849"        # gRPC 端口
    volumes:
      - nacos-data:/home/nacos/data
    networks:
      - ota-network
    depends_on:
      mysql:
        condition: service_healthy
    healthcheck:
      test: [ "CMD", "curl", "-f", "http://localhost:8848/nacos" ]
      interval: 30s
      timeout: 10s
      retries: 5

  # ==================== Kafka (P1) ====================
  kafka:
    image: bitnami/kafka:3.9
    container_name: ota-kafka
    restart: unless-stopped
    environment:
      KAFKA_ENABLE_KRAFT: "yes"
      KAFKA_CFG_NODE_ID: 1
      KAFKA_CFG_PROCESS_ROLES: broker,controller
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CFG_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_CFG_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CFG_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      ALLOW_PLAINTEXT_LISTENER: "yes"
    ports:
      - "9092:9092"
    volumes:
      - kafka-data:/bitnami/kafka
    networks:
      - ota-network

  # ==================== Zipkin (P1) ====================
  zipkin:
    image: openzipkin/zipkin:3
    container_name: ota-zipkin
    restart: unless-stopped
    environment:
      STORAGE_TYPE: mem
      JAVA_OPTS: -Dzipkin.ui.enabled=true
    ports:
      - "9411:9411"
    networks:
      - ota-network

  # ==================== Seata (P1) ====================
  seata:
    image: seataio/seata-server:2.6.0
    container_name: ota-seata
    restart: unless-stopped
    ports:
      - "8091:8091"
      - "7091:7091"
    networks:
      - ota-network

  # ==================== Sentinel Dashboard (P1) ====================
  sentinel:
    image: bladex/sentinel-dashboard:1.8.10
    container_name: ota-sentinel
    restart: unless-stopped
    environment:
      JAVA_OPTS: -Dserver.port=8858
    ports:
      - "8088:8858"
    networks:
      - ota-network

  # ==================== Prometheus (P2) ====================
  prometheus:
    image: prom/prometheus:v3.0
    container_name: ota-prometheus
    restart: unless-stopped
    ports:
      - "9090:9090"
    networks:
      - ota-network

  # ==================== Grafana (P2) ====================
  grafana:
    image: grafana/grafana:11.0
    container_name: ota-grafana
    restart: unless-stopped
    environment:
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: admin123
    ports:
      - "3000:3000"
    volumes:
      - grafana-data:/var/lib/grafana
    networks:
      - ota-network
    depends_on:
      - prometheus

  # ==================== ELK (P2) ====================
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.15.0
    container_name: ota-elasticsearch
    restart: unless-stopped
    environment:
      discovery.type: single-node
      xpack.security.enabled: "false"
      ES_JAVA_OPTS: "-Xms512m -Xmx512m"
    ports:
      - "9200:9200"
    volumes:
      - elasticsearch-data:/usr/share/elasticsearch/data
    networks:
      - ota-network

  logstash:
    image: docker.elastic.co/logstash/logstash:8.15.0
    container_name: ota-logstash
    restart: unless-stopped
    ports:
      - "5044:5044"
    volumes:
      - ./logstash/pipeline:/usr/share/logstash/pipeline
    networks:
      - ota-network
    depends_on:
      - elasticsearch

  kibana:
    image: docker.elastic.co/kibana/kibana:8.15.0
    container_name: ota-kibana
    restart: unless-stopped
    environment:
      ELASTICSEARCH_HOSTS: http://elasticsearch:9200
    ports:
      - "5601:5601"
    networks:
      - ota-network
    depends_on:
      - elasticsearch

volumes:
  mysql-data:
  redis-data:
  nacos-data:
  kafka-data:
  grafana-data:
  elasticsearch-data:
```

> **说明：** Nacos 3.1.1 的控制台服务在容器内 `8080` 端口，本教程映射为宿主机 `18080`，避免和本机其他服务的 `8080` 冲突。Nacos API 仍然使用 `8848`。
> **P2 说明：** Prometheus/Grafana 和 ELK 是可选扩展，不参与 P0 核心链路；需要监控和日志分析演示时再单独启动。

### 初始化 SQL 脚本

**文件：`init-sql/001-init-nacos-db.sql`**

```sql
-- 创建 Nacos 专用数据库
CREATE DATABASE IF NOT EXISTS `ota_nacos`
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_unicode_ci;

-- Nacos 初始化脚本较长，请从官方下载并执行：
-- https://github.com/alibaba/nacos/blob/3.1.1/distribution/bin/mysql-schema.sql
-- 或直接使用 Nacos 内置 Derby 模式启动后，在控制台手动配置
```

> **💡 小贴士：** 首次使用可直接设置 `MODE=standalone` 并用内置 Derby 数据库启动 Nacos。它会自动初始化。使用 MySQL 存储需要先执行 Nacos 官方提供的 `mysql-schema.sql`。

本教程使用 MySQL 存储 Nacos 配置，推荐把官方 schema 放到独立文件中：

```bash
mkdir -p init-sql
curl -fsSL https://raw.githubusercontent.com/alibaba/nacos/3.1.1/distribution/conf/mysql-schema.sql \
  > init-sql/002-nacos-schema.sql
```

然后在文件第一行加上：

```sql
USE `ota_nacos`;
```

最终目录结构如下：

```text
init-sql/
├── 001-init.sql          # 创建 ota_cloud、ota_nacos 和业务基础表
└── 002-nacos-schema.sql  # Nacos 官方 MySQL 表结构
```

---

## 1.6 Nacos 配置中心设计

### 1.6.1 命名空间规划

| 命名空间 | 命名空间ID | 用途 |
|---------|-----------|------|
| `dev` | `9d0e1c2f-...` | 开发环境 |
| `test` | `...` | 测试环境 |
| `prod` | `...` | 生产环境 |

本教程使用 `dev` 命名空间。

### 1.6.2 Data ID 分组规则

每个微服务有两个配置文件：

| Data ID | 分组 | 用途 |
|---------|------|------|
| `ota-{service}.yml` | `DEFAULT_GROUP` | 服务独享配置 |
| `ota-shared.yml` | `DEFAULT_GROUP` | 共享配置（Redis、Kafka等公共组件） |

示例：

| 服务 | Data ID |
|------|---------|
| ota-gateway | `ota-gateway.yml` |
| ota-auth | `ota-auth.yml` |
| ota-user | `ota-user.yml` |
| ota-hotel | `ota-hotel.yml` |

### 1.6.3 bootstrap.yml 配置

每个微服务都需要以下配置来连接 Nacos。这里以 ota-auth 为例，其他服务只需修改 `spring.application.name`。

**文件：`ota-auth/src/main/resources/bootstrap.yml`**

```yaml
spring:
  application:
    name: ota-auth
  cloud:
    nacos:
      config:
        server-addr: 127.0.0.1:8848
        namespace: dev                    # 命名空间ID
        group: DEFAULT_GROUP
        file-extension: yml
        shared-configs:
          - data-id: ota-shared.yml       # 共享配置
            group: DEFAULT_GROUP
            refresh: true
      discovery:
        server-addr: 127.0.0.1:8848
        namespace: dev
```

### 1.6.4 共享配置示例：`ota-shared.yml`

```yaml
# ========== 共享配置 — 所有服务通用 ==========

spring:
  # Redis
  data:
    redis:
      host: 127.0.0.1
      port: 6379
      password: redis123456
      timeout: 3000ms
      lettuce:
        pool:
          max-active: 16
          max-idle: 8
          min-idle: 4

# 日志
logging:
  level:
    com.ota.cloud: INFO
    org.springframework.cloud: WARN
```

### 1.6.5 服务独享配置示例：`ota-auth.yml`

```yaml
# ========== ota-auth 独享配置 ==========

server:
  port: 8081

spring:
  datasource:
    url: jdbc:mysql://127.0.0.1:3306/ota_cloud?useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai&useSSL=false
    username: root
    password: root123456
    driver-class-name: com.mysql.cj.jdbc.Driver
    type: com.alibaba.druid.pool.DruidDataSource
    druid:
      initial-size: 5
      min-idle: 5
      max-active: 20
      max-wait: 60000

# MyBatis-Plus
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
```

### 1.6.6 Nacos 启动后创建配置

Nacos 启动完成后，先登录控制台，再创建命名空间和配置文件。

#### 控制台入口

本教程的 Compose 将 Nacos Console 映射到宿主机 `18080`：

```text
Console: http://localhost:18080/
账号: nacos
密码: nacos
```

如果 Docker 跑在远端 Windows 机器上，例如 `100.99.175.108`：

```text
Console: http://100.99.175.108:18080/
```

如果浏览器或代理拦截 Tailscale IP，可以在本机做 SSH 端口转发：

```bash
ssh -N \
  -L 18080:127.0.0.1:18080 \
  -L 18848:127.0.0.1:8848 \
  administrator@100.99.175.108
```

然后访问：

```text
Console: http://127.0.0.1:18080/
API:     http://127.0.0.1:18848/nacos
```

#### 创建 dev 命名空间

1. 进入 **命名空间** 页面。
2. 点击 **新建命名空间**。
3. 填写：

```text
命名空间名: dev
命名空间ID: dev
描述: 开发环境
```

4. 保存后，在左上角命名空间切换到 `dev`。

> **建议：** 教程里直接使用固定命名空间 ID `dev`，这样 `bootstrap.yml` 更容易阅读。生产环境可以改成 UUID。

#### 创建 `ota-shared.yml`

进入 **配置管理** → **配置列表** → 切换到 `dev` 命名空间 → 点击 **创建配置**。

```text
Data ID: ota-shared.yml
Group: DEFAULT_GROUP
配置格式: YAML
```

配置内容：

```yaml
spring:
  data:
    redis:
      host: 127.0.0.1
      port: 6379
      password: redis123456
      timeout: 3000ms
      lettuce:
        pool:
          max-active: 16
          max-idle: 8
          min-idle: 4

logging:
  level:
    com.ota.cloud: INFO
    org.springframework.cloud: WARN
```

如果服务运行在宿主机本地，Redis 使用 `127.0.0.1`。如果服务也运行在同一个 Docker 网络内，Redis host 可以改为 Compose 服务名 `redis`。

#### 创建 `ota-auth.yml`

同样点击 **创建配置**。

```text
Data ID: ota-auth.yml
Group: DEFAULT_GROUP
配置格式: YAML
```

配置内容：

```yaml
server:
  port: 8081

spring:
  datasource:
    url: jdbc:mysql://127.0.0.1:3306/ota_cloud?useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai&useSSL=false
    username: root
    password: root123456
    driver-class-name: com.mysql.cj.jdbc.Driver
    type: com.alibaba.druid.pool.DruidDataSource
    druid:
      initial-size: 5
      min-idle: 5
      max-active: 20
      max-wait: 60000

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
```

#### 使用命令行写入配置（可选）

控制台写配置最直观；如果要脚本化，也可以使用 Nacos Open API。先登录拿 token：

```bash
TOKEN=$(curl -s -X POST 'http://127.0.0.1:8848/nacos/v1/auth/users/login' \
  -d 'username=nacos&password=nacos' \
  | sed -n 's/.*"accessToken":"\([^"]*\)".*/\1/p')
```

写入 `ota-shared.yml`：

```bash
curl -X POST 'http://127.0.0.1:8848/nacos/v1/cs/configs' \
  -d "accessToken=${TOKEN}" \
  -d 'tenant=dev' \
  -d 'dataId=ota-shared.yml' \
  -d 'group=DEFAULT_GROUP' \
  -d 'type=yaml' \
  --data-urlencode 'content=spring:
  data:
    redis:
      host: 127.0.0.1
      port: 6379
      password: redis123456
      timeout: 3000ms
logging:
  level:
    com.ota.cloud: INFO
    org.springframework.cloud: WARN'
```

查询配置是否写入成功：

```bash
curl 'http://127.0.0.1:8848/nacos/v1/cs/configs?tenant=dev&dataId=ota-shared.yml&group=DEFAULT_GROUP'
```

---

## 1.7 验证步骤

### Step 1：启动基础设施

```bash
# 从 docker-compose.yml 所在目录执行
docker compose up -d

# 检查状态
docker compose ps

# 确认三个服务全部 healthy
NAME                   COMMAND                  SERVICE    STATUS              PORTS
ota-mysql              "docker-entrypoint.s…"   mysql      running (healthy)   0.0.0.0:3306->3306/tcp
ota-redis              "docker-entrypoint.s…"   redis      running (healthy)   0.0.0.0:6379->6379/tcp
ota-nacos              "bin/docker-startup.…"   nacos      running (healthy)   0.0.0.0:18080->8080/tcp, 0.0.0.0:8848->8848/tcp
```

### Step 2：访问 Nacos 控制台

```
URL:  http://localhost:18080/
账号: nacos / nacos
```

![Nacos控制台](https://img.alicdn.com/imgextra/i1/O1CN012wR1Dg1lHqWQq0T1j_!!6000000004793-2-tps-2880-1572.png)

预期看到：
- 服务列表为空（尚未注册任何服务）
- 配置管理中有 `ota-shared.yml`（如果已创建）

### Step 3：在 Nacos 中创建配置

登录 Nacos 控制台 → **配置管理 → 配置列表** → 切换到 `dev` 命名空间 → 点击 **+** 创建配置：

**创建 `ota-shared.yml`：**
- Data ID: `ota-shared.yml`
- 分组: `DEFAULT_GROUP`
- 格式: YAML
- 内容: 见上面共享配置

**创建 `ota-auth.yml`：**
- Data ID: `ota-auth.yml`
- 分组: `DEFAULT_GROUP`
- 格式: YAML
- 内容: 见上面 auth 独享配置

### Step 4：编译验证

```bash
# 在项目根目录执行
mvn clean compile -pl ota-common -am

# 预期输出: BUILD SUCCESS
```

---

## 1.8 常见坑

### ❌ 坑1：Nacos 端口冲突

**现象：** `docker compose up` 报错端口已被占用。

**排查：**
```bash
# 检查端口占用
lsof -i :8848
lsof -i :3306
lsof -i :6379
```

**解决：**
- macOS：禁用内置 MySQL：`brew services stop mysql`
- 或修改 `docker-compose.yml` 中的映射端口

### ❌ 坑2：Nacos 启动后一直 Unhealthy

**原因：** Nacos 需要 MySQL 完全就绪后才开始初始化，健康检查超时。

**解决：** 增大 `healthcheck.retries`，或者手动等待：
```bash
docker logs -f ota-nacos
# 看到 "Nacos started successfully" 即为就绪
```

### ❌ 坑3：Spring Cloud Alibaba 版本不匹配

**现象：** `NoSuchMethodError` 或 `ClassNotFoundException`

**原因：** Spring Cloud 与 SCA 版本号不匹配。

**检查：**
| Spring Cloud | SCA 建议版本 |
|-------------|-------------|
| 2025.0.x | 2025.0.x 对应版本 |
| 2024.0.x | 2024.0.x 对应版本 |

> 一定要从 [官方版本映射](https://github.com/alibaba/spring-cloud-alibaba/wiki/%E7%89%88%E6%9C%AC%E8%AF%B4%E6%98%8E) 确认兼容性。

### ❌ 坑4：Maven 编译时报错 Lombok 版本

**现象：** `java: cannot find symbol` 指向 lombok 生成的 getter/setter。

**解决：** 确保 IDE（IDEA）的 Lombok 插件已安装，并开启 **Annotation Processing**：
- Setting → Build → Compiler → Annotation Processors → Enable annotation processing

### ❌ 坑5：虚拟线程的 ThreadLocal 陷阱

**现象：** TraceId 在线程池复用场景下丢失。

**背景：** JDK 21 虚拟线程不支持 `InheritableThreadLocal`。如果使用 `@Async` 异步任务，ThreadLocal 值不会传递。

**解决策略：** 后续 Gateway 章节使用 Reactor Context；对异步任务使用 MDC 或显式传递参数。

---

## 1.9 本章小结

| 产出 | 文件 |
|------|------|
| 父 POM | `pom.xml` |
| 公共模块 | `ota-common/` |
| Docker Compose | `docker-compose.yml` |
| Nacos 配置 | 控制台手动创建 |

**下一章预告：** 第2章将实现 Spring Cloud Gateway 路由网关 + ota-auth JWT 认证服务 + ota-user 用户服务。届时我们将通过 curl 验证完整的登录 → 鉴权 → 访问保护接口流程。
