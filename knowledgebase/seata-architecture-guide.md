# ota-cloud Seata 分布式事务全景架构与生产实践指南

> **定位**：系统性梳理 `ota-cloud` 中基于 Apache Seata 2.6.x 的**分布式全局事务（Global Transaction）架构设计**。深度解析 TC/TM/RM 角色协同、无侵入 AT 模式与两阶段提交（2PC）、`undo_log` 逆向回滚补偿、与本地事务及 Outbox 最终一致性模式的选型权衡。

---

## 一、架构定位与设计模式：Two-Phase Commit & AT Pattern

在传统的跨服务调用中（如下单服务调用远程库存中心预占库存），若后续订单落库异常，远程微服务的库存已被冻结且无法感知：
- **无侵入 AT 模式**：业务代码仅需添加 `@GlobalTransactional` 注解，Seata 代理数据源（`DataSourceProxy`）自动解析 SQL 并生成前置镜像（Before-Image）与后置镜像（After-Image），写入本地 `undo_log` 表；
- **两阶段提交（2PC）**：
  - **Phase 1（执行与上报）**：本地业务 SQL 与 `undo_log` 在同一个本地事务中提交，立即释放数据库本地行锁；向 TC（事务协调者）注册分支事务；
  - **Phase 2 提交**：若全部微服务均成功，TC 异步通知各 RM 批量删除 `undo_log`，无需任何二次回滚开销；
  - **Phase 2 回滚**：若任一微服务失败，TC 通知各 RM，RM 根据 `undo_log` 中的前置镜像生成反向补偿 SQL 并恢复数据。

```
                    ┌──────────────────────────────┐
                    │  Seata TC (Transaction Coord)│ (端口: 8091)
                    │  事务协调者，管理全局 XID     │
                    └───────▲──────────────▲───────┘
                            │ (1. 开启全局事务)│ (4. 通知二阶段提交/回滚)
            ┌───────────────┴──────────────┴───────────────┐
            │                                              │
    【TM / RM: ota-order】                         【RM: ota-inventory】
    @GlobalTransactional                           远程 Feign 调用
    1. 代理数据源记录 undo_log                     1. 代理数据源记录 undo_log
    2. 本地事务提交并释放锁                        2. 本地事务提交并释放锁
    3. 向 TC 上报分支结果                          3. 向 TC 上报分支结果
```

---

## 二、微服务拓扑与角色分工

1. **TC (Transaction Coordinator - 事务协调者)**：
   - 独立部署的 `ota-seata` 容器（端口 8091），维护全局和分支事务状态，驱动全局提交或回滚。
2. **TM (Transaction Manager - 事务管理器)**：
   - 发起全局事务的边界微服务（`ota-order` 订单中心），通过 `@GlobalTransactional` 声明全局事务边界。
3. **RM (Resource Manager - 资源管理器)**：
   - 管理分支事务涉及的资源（`ota-order` 订单库与 `ota-inventory` 库存库），与 TC 交互注册分支与报告执行状态。

---

## 三、分布式事务模式选型对比：Seata AT vs. Outbox 最终一致性

| 维度 | Seata AT 模式 | Transactional Outbox (最终一致性) |
| :--- | :--- | :--- |
| **一致性强度** | 强一致性（强近实时） | 最终一致性（异步延迟数秒） |
| **性能损耗** | 较重（需解析 SQL 生成前置/后置镜像及写 undo_log） | 极轻（仅本地多写单条事件表，异步投递） |
| **下游系统要求** | 下游必须同属于微服务体系且受 Seata 代理 | 任意异构系统均可（Kafka 广播解耦） |
| **生产推荐场景** | 强金融对账、跨行转账、同组织强原子扣减 | **OTA 主流场景（下单、支付、推单、出票）首选** |

> **生产权衡约定**：`ota-cloud` 中交易主流程默认以 `Transactional Outbox + Kafka` 作为主线模式；同时在配置中心保留 `seata.enabled` 与标准配置骨架，作为跨服务强一致性演示与技术储备。

---

## 四、关键落地技术细节

### 1. 客户端配置（`application.yml`）

```yaml
seata:
  enabled: ${SEATA_ENABLED:false}
  application-id: ${spring.application.name}
  tx-service-group: ota-tx-group
  service:
    vgroup-mapping:
      ota-tx-group: default
    grouplist:
      default: ${SEATA_SERVER:100.99.175.108:8091}
  config:
    type: nacos
    nacos:
      server-addr: ${NACOS_SERVER:100.99.175.108:8848}
      namespace: ${NACOS_NAMESPACE:public}
      group: SEATA_GROUP
  registry:
    type: nacos
    nacos:
      server-addr: ${NACOS_SERVER:100.99.175.108:8848}
      namespace: ${NACOS_NAMESPACE:public}
      group: SEATA_GROUP
      application: seata-server
```

### 2. 本地回滚依赖表：`undo_log` DDL

各业务数据库（订单库、库存库）必须预先创建如下标准 `undo_log` 表以支撑反向补偿：

```sql
CREATE TABLE IF NOT EXISTS `undo_log` (
  `branch_id` BIGINT NOT NULL COMMENT 'branch transaction id',
  `xid` VARCHAR(128) NOT NULL COMMENT 'global transaction id',
  `context` VARCHAR(128) NOT NULL COMMENT 'undo_log context,such as serialization',
  `rollback_info` LONGBLOB NOT NULL COMMENT 'rollback info',
  `log_status` INT NOT NULL COMMENT '0:normal status,1:defense status',
  `log_created` DATETIME(6) NOT NULL COMMENT 'create datetime',
  `log_modified` DATETIME(6) NOT NULL COMMENT 'modify datetime',
  UNIQUE KEY `ux_undo_log` (`xid`, `branch_id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'AT transaction mode undo table';
```

---

## 五、监控指标与运维排查

### 1. 常见问题诊断
- **报错 `Could not found service provider for : default`**：
  - **根因**：客户端声明的事务分组（`ota-tx-group`）在 Nacos 或本地配置中未能映射到正确的集群名 `default`。
- **锁冲突超时（LockConflictException）**：
  - 当多个全局事务争抢同一行数据时，Seata 会申请全局锁（Global Lock）。若业务事务占用时间过长，会导致后续事务等待超时，需排查大事务并尽快缩减 `@GlobalTransactional` 范围。

---

## 六、基础设施与运维部署

- **镜像规范**：`seataio/seata-server:2.6.0`
- **Docker 运行参数**：
  ```yaml
  seata:
    image: seataio/seata-server:2.6.0
    container_name: ota-seata
    restart: unless-stopped
    ports:
      - "7091:7091"
      - "8091:8091"
    environment:
      SEATA_PORT: 8091
      STORE_MODE: file
      TZ: Asia/Shanghai
    networks:
      - ota-network
    depends_on:
      nacos:
        condition: service_healthy
      mysql:
        condition: service_healthy
  ```
