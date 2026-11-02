# 第10章：商家中心 (ota-merchant)

> **目标**：搭建独立的商家视角微服务 `ota-merchant:8086`，构建商家账号体系、基于角色的权限访问控制（RBAC）、多租户级数据隔离以及商家专属订单管理与对账中心。

---

## 10.1 服务边界与网络拓扑

在典型的 OTA 平台中，“平台运营”与“入驻商家/酒店集团”拥有完全不同的业务视角与权限边界：
- **平台视角（8087 `ota-admin`）**：全局运营人员，能够跨商家排查订单，支持全局加价系数与供应商规则配置。
- **商家视角（8086 `ota-merchant`）**：入驻商户独立后台，严格数据物理/逻辑隔离，仅能查看本商户名下的房型、价库、订单与结算流水。

```
                    ┌───────────────────────────────┐
                    │      外部商家 Web / 客户端     │
                    └───────────────┬───────────────┘
                                    │ (8080)
                                    ▼
                    ┌───────────────────────────────┐
                    │      ota-gateway 统一网关     │
                    │   StripPrefix=1, 转发至商家   │
                    │   /api/merchant/** ➔ 8086     │
                    └───────────────┬───────────────┘
                                    │
                                    ▼
                    ┌───────────────────────────────┐
                    │      ota-merchant (8086)      │
                    │   MerchantAuthFilter (上下文) │
                    │   @RequiresPermission (鉴权)  │
                    └───────┬───────────────┬───────┘
                            │               │
        ┌───────────────────┘               └───────────────────┐
        ▼ (Feign 内部调用)                                      ▼ (Feign 内部调用)
┌───────────────────────┐                               ┌───────────────────────┐
│   ota-order (8085)    │                               │  ota-supplier (8089)  │
│ /order/internal/      │                               │ /supplier/internal/   │
│ merchant/** (校验隔离)│                               │ reconcile/** (对账)   │
└───────────────────────┘                               └───────────────────────┘
```

---

## 10.2 数据库设计与 RBAC 权限体系

商家中心在底层数据上包含两大部分：商家主体与 RBAC 权限 5 张表（`init-sql/002-merchant.sql`），以及交易订单表的租户字段扩展（`init-sql/003-order-merchant.sql`）。

### 1. 商家核心表 DDL

```sql
-- 1. 商家主体表
CREATE TABLE IF NOT EXISTS `merchant` (
    `id` BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '商家ID',
    `code` VARCHAR(64) NOT NULL UNIQUE COMMENT '商家唯一标识编码',
    `name` VARCHAR(128) NOT NULL COMMENT '商家企业名称',
    `contact_phone` VARCHAR(32) DEFAULT '' COMMENT '联系电话',
    `status` VARCHAR(16) NOT NULL DEFAULT 'ENABLED' COMMENT '状态: ENABLED, DISABLED',
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商家主体信息表';

-- 2. 商家用户账号表
CREATE TABLE IF NOT EXISTS `merchant_user` (
    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `merchant_id` BIGINT NOT NULL COMMENT '所属商家ID',
    `username` VARCHAR(64) NOT NULL UNIQUE COMMENT '登录用户名',
    `password_hash` VARCHAR(128) NOT NULL COMMENT 'BCrypt 加密密码',
    `real_name` VARCHAR(64) DEFAULT '' COMMENT '真实姓名',
    `status` VARCHAR(16) NOT NULL DEFAULT 'ACTIVE' COMMENT '状态: ACTIVE, FROZEN',
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_merchant_id` (`merchant_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商家操作员账号表';
```

### 2. 订单表增加租户隔离字段

```sql
ALTER TABLE `orders`
    ADD COLUMN `merchant_id` BIGINT NOT NULL DEFAULT 1 COMMENT '所属商家ID（多租户数据隔离）' AFTER `user_id`,
    ADD INDEX `idx_merchant_id_status` (`merchant_id`, `status`),
    ADD INDEX `idx_merchant_id_created_at` (`merchant_id`, `created_at`);
```

---

## 10.3 商家多租户数据隔离设计

为了彻底杜绝水平越权漏洞（BOLA），系统建立了严格的强制隔离规则：
1. **禁止前端传参**：Controller / Service 严禁通过 `@RequestParam("merchantId")` 或 Body 接收客户端传入的 `merchantId`。
2. **网关校验后注入 Header**：网关在校验 JWT 后，提取 Claim 中的 `merchantId` 注入到内部请求头 `X-OTA-Merchant-Id`。
3. **上下文线程绑定**：微服务通过 `MerchantAuthFilter` 统一抓取请求头并绑定至 ThreadLocal。
4. **越权防御性回包**：当请求查询非当前商家的订单时，统一返回 `Result.success(null)`，不抛 5xx 也不返回 404，防止攻击者通过暴力遍历枚举订单 ID。

### 商家上下文载体 (`MerchantContext`)

```java
package com.ota.cloud.merchant.security;

import com.ota.cloud.common.constant.CommonConstant;
import com.ota.cloud.merchant.dto.MerchantPrincipal;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Collections;
import java.util.List;

/**
 * 商家请求上下文持有者。
 * <p>
 * 依托底层 {@link ThreadLocal} 机制，在当前处理线程内安全存储已认证商家的安全主体对象，
 * 提供租户 ID、用户身份与细粒度权限码的全局快速静态访问能力。
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class MerchantContext {

    /** 绑定到当前线程的商家安全主体容器。 */
    private static final ThreadLocal<MerchantPrincipal> CURRENT = new ThreadLocal<>();

    /**
     * 设置当前线程的安全主体。
     *
     * @param principal 已通过网关鉴权解析的商家主体信息
     */
    public static void set(MerchantPrincipal principal) {
        CURRENT.set(principal);
    }

    /**
     * 获取当前线程绑定的商家安全主体。
     *
     * @return 商家主体实体；未绑定时返回 null
     */
    public static MerchantPrincipal get() {
        return CURRENT.get();
    }

    /**
     * 快捷获取当前请求所属商家的租户主键 ID。
     *
     * @return 商家 ID；若未登录返回 null
     */
    public static Long currentMerchantId() {
        MerchantPrincipal principal = CURRENT.get();
        return principal != null ? principal.getMerchantId() : null;
    }

    /**
     * 判断当前访问用户是否属于商家账号体系。
     *
     * @return true 表示为商家操作员；false 表示为平台运营或普通用户
     */
    public static boolean isMerchant() {
        MerchantPrincipal p = CURRENT.get();
        return p != null && CommonConstant.USER_TYPE_MERCHANT.equals(p.getUserType());
    }

    /**
     * 获取当前商家用户拥有的扁平化权限码列表。
     *
     * @return 权限码集合（如 order:view, reconcile:export）；为空时返回不可变空集合
     */
    public static List<String> currentPermissions() {
        MerchantPrincipal p = CURRENT.get();
        return p == null ? Collections.emptyList() : p.getPermissions();
    }

    /**
     * 强制清除当前线程存储的上下文信息。
     * <p>
     * 必须在 Filter 的 finally 块中显式调用，杜绝线程池工作线程复用导致的租户数据串号泄露风险。
     */
    public static void clear() {
        CURRENT.remove();
    }
}
```

### 过滤器生命周期安全控制 (`MerchantAuthFilter`)

必须在 `finally` 块中显式执行 `clear()`，避免线程池工作线程复用导致跨请求串号污染：

```java
package com.ota.cloud.merchant.security;

import com.ota.cloud.common.constant.CommonConstant;
import com.ota.cloud.merchant.dto.MerchantPrincipal;
import com.ota.cloud.merchant.entity.Merchant;
import com.ota.cloud.merchant.mapper.MerchantMapper;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 商家请求上下文统一拦截装配过滤器。
 * <p>
 * 职责：
 * 拦截所有进入商家微服务的 HTTP 请求，解析 API 网关注入的标准请求头，
 * 组装 {@link MerchantPrincipal} 并绑定至 {@link MerchantContext}，
 * 在请求结束时负责在 finally 块中彻底释放 ThreadLocal 内存。
 */
@Slf4j
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 1)
@RequiredArgsConstructor
public class MerchantAuthFilter extends OncePerRequestFilter {

    /** 商家权限运算服务。 */
    private final MerchantPermissionService permissionService;

    /** 商家数据 Mapper。 */
    private final MerchantMapper merchantMapper;

    /**
     * 核心过滤处理逻辑。
     *
     * @param request  当前入站 HTTP 请求
     * @param response 当前出站 HTTP 响应
     * @param chain    Servlet 过滤器调用链
     * @throws ServletException 容器 Servlet 处理异常
     * @throws IOException      I/O 传输异常
     */
    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain chain) throws ServletException, IOException {
        try {
            // 1. 读取网关层验签后安全透传的请求头
            String userType = request.getHeader(CommonConstant.HEADER_USER_TYPE);
            String userIdRaw = request.getHeader(CommonConstant.HEADER_USER_ID);
            String username = request.getHeader(CommonConstant.HEADER_USERNAME);
            String merchantIdRaw = request.getHeader(CommonConstant.HEADER_MERCHANT_ID);
            String rolesHeader = request.getHeader(CommonConstant.HEADER_ROLES);

            // 2. 若未携带用户 ID（如未登录的公开探测请求），直接流转下游
            if (!StringUtils.hasText(userIdRaw)) {
                chain.doFilter(request, response);
                return;
            }

            // 3. 转换数字类型标识
            Long userId = parseLong(userIdRaw);
            Long merchantId = parseLong(merchantIdRaw);
            List<String> roles = StringUtils.hasText(rolesHeader)
                    ? Arrays.asList(rolesHeader.split(","))
                    : Collections.emptyList();

            // 4. 解析商家租户代码
            String merchantCode = null;
            if (merchantId != null) {
                Merchant m = merchantMapper.selectById(merchantId);
                if (m != null) {
                    merchantCode = m.getCode();
                }
            }

            // 5. 组装商家安全主体并挂载至当前处理线程
            MerchantPrincipal principal = permissionService.buildPrincipal(
                    userId, username, merchantId, merchantCode, userType, roles);
            MerchantContext.set(principal);

            // 6. 执行业务调用链
            chain.doFilter(request, response);
        } finally {
            // 7. 严防内存泄露与线程污染：在请求退栈的唯一终点执行彻底清理
            MerchantContext.clear();
        }
    }

    /**
     * 安全解析 Long 数字字符串，防止因格式错误抛错中断链路。
     *
     * @param s 原始数字字符串
     * @return 解析后的 Long 对象；若为空或格式不合法返回 null
     */
    private static Long parseLong(String s) {
        if (!StringUtils.hasText(s)) {
            return null;
        }
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            log.warn("无法解析的 Long 头部: {}", s);
            return null;
        }
    }
}
```

---

## 10.4 权限拦截器与动态菜单契约

### 1. 基于注解的方法鉴权

```java
package com.ota.cloud.merchant.security;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 商家细粒度 RBAC 权限管控自定义注解。
 * <p>
 * 标注在 Controller 类或方法上，由 {@link PermissionInterceptor} 拦截器在进入方法前
 * 校验 {@code MerchantContext.currentPermissions()} 是否包含目标权限码。
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface RequiresPermission {
    /** 要求的权限标识码（例如 order:view、reconcile:export） */
    String value();
}
```

### 2. 动态菜单契约接口 (`GET /api/merchant/menus`)

前端在登录完成后拉取菜单，后端根据当前商家的权限编码数组过滤可见菜单列表：

```json
{
  "code": 200,
  "data": [
    {
      "id": "order-management",
      "name": "订单中心",
      "path": "/merchant/orders",
      "permission": "order:view"
    },
    {
      "id": "reconcile-center",
      "name": "财务对账",
      "path": "/merchant/reconcile",
      "permission": "reconcile:view"
    }
  ]
}
```

---

## 10.5 快速验证

```bash
# 执行第 10 章商家中心全流程冒烟（覆盖健康检查、登录、越权拦截、订单隔离与动态菜单）
bash scripts/chapter10-merchant-smoke.sh
```

