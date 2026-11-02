# Chapter 10: 商家中心 (Merchant Center)

**Date**: 2026-06-10
**Status**: Planning
**Owners**: ota-cloud

## 背景

第 9 章把 `ota-supplier` 与商家对账页放进了 `ota-admin`，但 `ota-admin` 是"平台管理员"视角（`auth_user` + 全局可见的 channel/hotel/range 配置）。真实场景里"商家"和"平台"是两套账号体系：

- 平台管理员 (`ota-auth` 当前实现) 配置全平台数据，看到所有酒店 / 渠道 / 订单。
- 商家账号只看自己商户的数据，对账、退款、订单查看都按 `merchantId` 隔离。

第 10 章引入 `ota-merchant` 服务 + 商家 RBAC + 平台/商家双 token 路由，并把第 9 章的 `SupplierReconcileController` 迁过去（保留 `ota-admin` 一份"平台视角"入口，按 merchantId 过滤）。

## Phase 0：现状与可复用资产 (调研 + 资料归口)

**目标**：把后续 Phase 要复用的现有代码、DDL、Filter 模式定位清楚。

**输出**：
- 本计划文档（本文）。
- `legacy-otaapi-reuse.md` 增补商家中心小节（指向老系统 `/Users/fuxinwei/work/otaapi/hoa-main/hoa-normal` 的 merchant 角色配置）。

**主要参考资料**：

| 复用对象 | 路径 | 用途 |
| --- | --- | --- |
| JWT 签发与解析 | [JwtUtil.java](file:///Users/fuxinwei/work/ota-cloud/ota-auth/src/main/java/com/ota/cloud/auth/util/JwtUtil.java) | 商家 JWT 同样用 `ota-auth` 签发，扩展 claims |
| 登录 / 校验服务 | [AuthService.java](file:///Users/fuxinwei/work/ota-cloud/ota-auth/src/main/java/com/ota/cloud/auth/service/AuthService.java) | 复制 login/validate 模式到 `MerchantAuthService` |
| Gateway 鉴权 | [AuthGlobalFilter.java](file:///Users/fuxinwei/work/ota-cloud/ota-gateway/src/main/java/com/ota/cloud/gateway/filter/AuthGlobalFilter.java) | 扩展白名单 + 增加 `X-OTA-Merchant-Id` / `X-OTA-User-Type` 头 |
| 用户表 DDL | `/Users/fuxinwei/work/db/api_hotel.txt` | 参考 `auth_user`，商家表自建 `merchant_user` |
| 中台配置 DDL | [001-init.sql](file:///Users/fuxinwei/work/ota-cloud/init-sql/001-init.sql) `downstream_channel` / `channel_data_range` | 复制表结构，添加 `merchant` 主表 |
| 对账页现状 | [SupplierReconcileController.java](file:///Users/fuxinwei/work/ota-cloud/ota-admin/src/main/java/com/ota/cloud/admin/controller/SupplierReconcileController.java) | 迁到 `ota-merchant` 时把内嵌的 admin 视角剥成 "merchant view" |
| 老系统商家账号 | `/Users/fuxinwei/work/otaapi/hoa-main/hoa-normal` | 调研老系统的 `merchant_user / merchant_role / merchant_role_permission` |

**反模式（不要做）**：
- 不把平台管理员和商家用户混在同一张表里（不要给 `auth_user` 加 `merchant_id` 列）。
- 不复用 `ota-admin` 的 controller 兼做商家后台。哪怕是同样的对账逻辑也分两个服务。
- 不改 `JwtUtil` 的现有签名 key / claims 结构。商家侧走新增的 `merchantToken` 字段（与 `token` 并存）。

**完成判定**：
- `legacy-otaapi-reuse.md` 已增补商家中心一节。
- 本计划被用户在 `cloud-tutorial-progress.md` 引用。

## Phase 1：商家账号 DDL + 实体

**目标**：在 `init-sql/002-merchant.sql` 新增 5 张表（沿用 ota-cloud 风格 DDL 注释 + 来源 DDL 标注），并加 entity / mapper。

**表**：
1. `merchant`：`id, name, contact_name, contact_phone, status, created_at, updated_at`，字段参考 `partner.sql` / `hoa_channel`。
2. `merchant_user`：`id, merchant_id, username, password_hash, status, last_login_at, created_at, updated_at`（BCrypt 与 `auth_user` 同策略）。
3. `merchant_role`：`id, merchant_id, code, name, status`，每个商家独立角色。约定 `ADMIN` / `OPERATOR` / `VIEWER` 三个内置 code。
4. `merchant_permission`：`id, code, name, module`（全局字典：`reconcile:view, reconcile:retry, order:view, order:retry, hotel:view, account:view, account:edit`）。
5. `merchant_user_role` / `merchant_role_permission`：标准 RBAC 关联表。

**Source DDL 标注**：
- `merchant` 字段注释：来源 `/Users/fuxinwei/work/db/partner.sql` 的 `partner_basic`。
- `merchant_user` 字段注释：来源 `/Users/fuxinwei/work/db/auth.txt` 的 `auth_user` 字段。
- `merchant_role/permission` 来源：`/Users/fuxinwei/work/otaapi/hoa-main/hoa-normal` 的 `merchant_role.xml`。

**Mapper**：
- `MerchantMapper.java` / `MerchantMapper.xml`
- `MerchantUserMapper.java` / `MerchantUserMapper.xml`
- `MerchantRoleMapper.java` / `MerchantRoleMapper.xml`
- `MerchantPermissionMapper.java` / `MerchantPermissionMapper.xml`
- `MerchantUserRoleMapper.java` / `MerchantUserRoleMapper.xml`
- `MerchantRolePermissionMapper.java` / `MerchantRolePermissionMapper.xml`

**完成判定**：
- `init-sql/002-merchant.sql` 在 Windows Docker 上 `mysql -uroot -proot123456 ota_cloud < 002-merchant.sql` 成功。
- 6 个 Mapper 接口编译通过。
- `MerchantUserMapperTest` 至少覆盖：按 username 查、按 merchant_id 查、唯一索引冲突。

## Phase 2：`ota-merchant` 服务骨架 + 登录链路

**目标**：新建 `ota-merchant` 服务，跑通"商家登录 → JWT → 校验"全链路。

**新建**：
- `ota-merchant/pom.xml`：参考 `ota-admin/pom.xml` 依赖（spring-boot 3 + spring-cloud + nacos + mybatis-plus + spring-security crypto + redisson）。
- `MerchantApplication.java`、bootstrap.yml、application.yml（Nacos dataId=`ota-merchant.yml`）。
- `MerchantAuthService.login(username, password, merchantId?)`：若 username 命中多个 merchant（同名不同商家），用 merchantId 二次确认；否则按"username 全局唯一"约定走。
- `MerchantAuthController`：`POST /api/merchant/auth/login`、`POST /api/merchant/auth/logout`、`GET /api/merchant/auth/validate`。
- **JWT 扩展**：复用 `ota-auth` 的 `JwtUtil`，不签发到现有 `auth` 体系。新增 `MerchantJwtUtil` 单独签发 `merchantToken`（同样的 HS256 密钥，但 claims 多了 `merchantId` + `userType=MERCHANT` + `roles` 数组）。
- **黑名单**：复用 `ota-auth` 用的 Redis key `ota:jwt:blacklist:`，jti 同样放进去。

**Gateway 改造**：
- [application.yml](file:///Users/fuxinwei/work/ota-cloud/ota-gateway/src/main/resources/application.yml) 加 `/api/merchant/**` 路由指向 `lb://ota-merchant`。
- [WhiteListConfig.java](file:///Users/fuxinwei/work/ota-cloud/ota-gateway/src/main/java/com/ota/cloud/gateway/config/WhiteListConfig.java) 加 `/api/merchant/auth/login`。
- [AuthGlobalFilter.java](file:///Users/fuxinwei/work/ota-cloud/ota-gateway/src/main/java/com/ota/cloud/gateway/filter/AuthGlobalFilter.java) 在解析 claims 后增加：
  - `X-OTA-Merchant-Id`：仅 userType=MERCHANT 时注入。
  - `X-OTA-User-Type`：值 `PLATFORM` 或 `MERCHANT`。
  - `X-OTA-Roles`：逗号分隔的角色 code 列表（用于下游做粗粒度权限检查）。
- 商家路由在 `ota-merchant` 内的拦截器（`MerchantAuthInterceptor`）校验 `X-OTA-Merchant-Id` 是否与请求路径里的 `{merchantId}` 一致，不一致返回 403。

**完成判定**：
- `ota-merchant` 通过 `mvn -pl ota-merchant -am package` 编译通过。
- 真实环境 smoke：`scripts/chapter10-merchant-smoke.sh` 中前 3 步（健康检查 / 登录 / 校验）。
- 商家账号不能访问 `/api/auth/*` 与 `/api/admin/*`（已实测被现有 Gateway 路由剥前缀后，merchant 路径会落到 `ota-merchant`，无对应端点 → 404 / 401）。

## Phase 3：商家 RBAC（权限查询 + 接口拦截）

**目标**：商家侧的 controller 强制校验"用户在该 merchant 下的角色拥有该接口对应的 permission code"。

**实现**：
- `MerchantPermissionService.check(userId, merchantId, permissionCode)`：查 `merchant_user_role` + `merchant_role_permission`，缺失抛 `BusinessException(FORBIDDEN, "无权限: xxx")`。
- `MerchantAuthInterceptor` 注入到所有 `ota-merchant` 的 controller 上，**非白名单接口**（`/auth/**`、`/health`）按 `@MerchantPermission("xxx")` 注解取值校验。
- `@MerchantPermission` 注解定义在 `ota-merchant` 内部，annotation holder 与 value(code) 两个字段。

**Service**：
- `MerchantRoleService` 增删改查角色（平台管理员视角，但 1 期通过 SQL 注入，REST 接口放到 4 期）。
- `MerchantUserRoleService.assignRoles(merchantId, userId, roleIds)`：分配角色。

**Controller**：
- `MerchantUserController`：`POST /api/merchant/users`（平台侧创建商家账号）、`PUT /api/merchant/users/{id}/status`（启用/禁用）、`GET /api/merchant/users?merchantId=xxx`。
- `MerchantRoleController`：`GET /api/merchant/roles?merchantId=xxx`、`PUT /api/merchant/roles/{id}/permissions`。
- `MerchantHealthController`：`GET /api/merchant/health`，白名单放行。

**完成判定**：
- `MerchantPermissionServiceTest` 覆盖：管理员可见 / 运营者只读 / 查看者无 retry 权限。
- `MerchantAuthInterceptor` 单元测试：未带 token → 401；token 来自其它 merchant → 403；权限缺失 → 403。

## Phase 4：数据隔离 (merchantId 注入 + 过滤)

**目标**：商家侧业务查询强制按 `merchantId` 过滤，避免跨商家数据泄漏。

**实现**：
- `MerchantContextHolder`：基于 `ThreadLocal<MerchantContext>`（`userId, merchantId, roles, permissions`）在 `MerchantAuthInterceptor` 里设置。
- 任何带 `merchantId` 的查询，Service 层统一从 `MerchantContextHolder.current().getMerchantId()` 拿，禁止用 `@RequestParam` 传入（仅 1 个 admin 端点允许 override）。
- 订单 / 酒店 / 库存 三个核心查询，对应 Feign client 或 Service 加 `merchantId` 过滤。

**Feign 透传**：
- `ota-merchant` 调 `ota-order` 时，新增 `X-Merchant-Id` Header（参考现有 `X-OTA-UserId`）。
- `ota-order` 的 OrderMapper 增加 `selectByMerchant(merchantId, status, page)` 方法，SQL 写在 `OrderMapper.xml`。
- `OrderService.listForMerchant` 走 `MerchantContextHolder`，无 merchantId 时拒绝。

**完成判定**：
- `MerchantContextHolderTest`：跨线程 / 异常分支不污染。
- `OrderMapper.merchantFilter` 单元测试：相同 status 订单不同 merchantId 不混。

## Phase 5：对账页迁到商家侧 + 平台 admin 侧保留

**目标**：把 [SupplierReconcileController](file:///Users/fuxinwei/work/ota-cloud/ota-admin/src/main/java/com/ota/cloud/admin/controller/SupplierReconcileController.java) 的核心对账能力迁到 `ota-merchant`，平台 admin 保留"按 merchant 维度筛选"的能力。

**实现**：
- `ota-merchant` 新增 `MerchantSupplierReconcileController`，路径 `/api/merchant/reconcile/**`。
  - `GET /reconcile/orders`：分页拉本商家订单，**从 `MerchantContextHolder` 拿 merchantId**。
  - `GET /reconcile/orders/{orderId}/diff`：单订单对账，结构与 [SupplierReconcileVO.java](file:///Users/fuxinwei/work/ota-cloud/ota-admin/src/main/java/com/ota/cloud/admin/vo/SupplierReconcileVO.java) 对齐。
  - `POST /reconcile/orders/{orderId}/retry`：手动触发 `SupplierCompensationJob` 逻辑。注入 `SupplierCompensationJob` 的"单订单模式"（新增 `runOnce(orderId)` 方法，不动现有 `run()`）。
- `ota-admin` 现有 `SupplierReconcileController` 改造：
  - `listOrders` 增加 `?merchantId=` 过滤（admin 默认不过滤，可下拉选商家）。
  - `reconcileOne` 路径不变。
- `SupplierReconcileService` 拆出"merchantId 过滤逻辑"到公共方法，被两边复用。

**完成判定**：
- `chapter10-merchant-smoke.sh` 加 2 个对账页断言：商家用户只看到自己 merchantId 的订单；admin 用户可看到全部。
- 商家侧 retry 按钮触发后，订单 supplier_order_no 由空变为非空（mock 环境可测）。

## Phase 6：菜单 / 导航（前后端契约）

**目标**：商家后台有清晰的"我能看哪些菜单"。

**实现**：
- 后端：`GET /api/merchant/menus`，根据 `MerchantContextHolder.permissions` 返回菜单树 JSON（静态菜单元数据写在 `MerchantMenus` 常量里，前端不写死权限）。
- 前端：本仓库暂未引入商家后台 Vue 端；只输出契约（`MerchantMenus` 的 JSON 结构 + `permission -> menu` 映射表），用 `ota-portfolio-demo` 在 mock 模式渲染一版演示。

**完成判定**：
- `MerchantMenusTest` 验证：3 个内置角色的菜单可见性。
- `ota-portfolio-demo` 新增 `/merchant-demo` 路由（仅 mock 数据），并附 README 截图说明。

## Phase 7：测试 + Smoke + 文档

**目标**：所有新代码配套测试 + 真实环境 smoke + 文档收口。

**新增测试**：
- `MerchantUserMapperTest`、`MerchantRoleMapperTest`：基础 CRUD + 唯一索引冲突。
- `MerchantAuthServiceTest`：密码错误 / 账号禁用 / 跨 merchant 登录 / Token 黑名单。
- `MerchantPermissionServiceTest`：RBAC 命中 / 缺失。
- `MerchantAuthInterceptorTest`：未带 token / 跨 merchant / 权限缺失。
- `MerchantContextHolderTest`：ThreadLocal 隔离。
- `OrderMapper.merchantFilterTest`：merchantId 过滤 SQL。
- `SupplierReconcileService.merchantIdTest`：拆出的过滤方法单元测试。

**Smoke**：
- `scripts/chapter10-merchant-smoke.sh`：
  1. `ota-merchant` 健康检查（直连 + Gateway 白名单）。
  2. 商家账号登录 → 拿到 merchantToken。
  3. 商家 token 访问 `/api/merchant/auth/validate` 通过。
  4. 商家 token 调 `/api/merchant/reconcile/orders` 返回本商家订单。
  5. 商家 token 调 admin 路由 `/api/admin/xxx` 返回 403 / 404。
  6. admin 账号 token 调 `/api/admin/reconcile/orders?merchantId=xxx` 返回该商家订单。
  7. 商家 retry 按钮触发后，订单 supplier_order_no 由空变非空（mock）。
  8. 平台账号可调 `/api/merchant/menus` 看任意商家菜单（admin 视角）。

**文档**：
- `cloud-tutorial-progress.md` 在 Chapter 10 行填入 Done 与本计划摘要。
- `legacy-otaapi-reuse.md` 增加"商家中心"小节，引用本计划文件。
- `ota-merchant/README.md`：商家后台 API 列表 + 角色权限约定 + smoke 步骤。
- `ota-admin/README.md`：补充"按 merchantId 过滤"的 admin 端使用方式。
- `AGENTS.md`：增加"商家侧服务跑在 8086 端口"约定（如无冲突）。

**反模式**：
- 不写 E2E Selenium 测试（本项目 P0 仍是后端 + smoke 脚本）。
- 不为商家引入 Vue 端工程；菜单演示用 `ota-portfolio-demo` 复用。

**完成判定**：
- `mvn -pl ota-merchant,ota-admin,ota-order,ota-gateway -am test` 全绿。
- `chapter10-merchant-smoke.sh` 在 Windows Docker 真实环境跑通。
- 文档 3 处全部就绪。

## 风险与回退

| 风险 | 应对 |
| --- | --- |
| 现有 `X-OTA-UserId` 在商家侧语义不清 | 商家 token 也带 `X-OTA-UserId`（仍是 userId），新增 `X-OTA-Merchant-Id` 区分 |
| 多商家同名 username | 登录要求客户端传 `merchantId` 二次确认；不传时若全局唯一则放行 |
| 老系统 merchant 字段不在 `partner.sql` | 用 `partner_basic` 字段命名兜底，注释里写明 |
| Gateway 全局 filter 改坏 | 仅扩展 header 注入，不动现有鉴权逻辑；白名单 + filter 单元测试覆盖 |
