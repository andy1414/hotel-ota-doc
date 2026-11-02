# 第8.5章：作品展示 Demo 前端 (ota-portfolio-demo)

> **目标**：打造独立的现代静态前端演示系统，将后端的微服务调用链路、状态流转、分布式事务与可观测性证据抽象为可视化交互。支持纯前端内存状态机离线回放与直连真实微服务集群（Gateway 8080）双模式运行。

---

## 8.5.1 业务诉求与设计定位

分布式微服务架构在求职面试或项目汇报中面临一个典型痛点：后端依赖组件繁多（Nacos、MySQL、Redis、Kafka、Sentinel、Prometheus 等），无法随时随地在轻量级环境或移动端快速拉起并直观展示。

`ota-portfolio-demo` 的核心定位是：
1. **轻量便携**：基于纯前端静态单页应用（SPA），可部署于 GitHub Pages、Vercel 或 Nginx，零服务端计算成本。
2. **状态仿真与真实证据结合**：内置纯函数状态机模拟全链路，同时展示 Chapter 8 真实自动化 Smoke 测试中产出的订单编号、MySQL 快照和 Kafka 消息。
3. **支持直连网关模式**：在后端集群启动时，一键切换为真实 API 模式，直接向 `ota-gateway (8080)` 发起跨域 HTTP 请求。

```
┌────────────────────────────────────────────────────────┐
│             浏览器展示层 (React 19 + Vite)              │
│  [微服务分层拓扑图]   [全链路交互看板]   [实时 JSON 证据]  │
└───────────────────────────┬────────────────────────────┘
                            │
            ┌───────────────┴───────────────┐
            ▼ (离线演示模式)                 ▼ (真实联调模式)
┌───────────────────────┐       ┌───────────────────────┐
│  flowReplay.ts 状态机  │       │  realApiClient.ts     │
│ 纯函数推导 / 内存状态  │       │ 直连 Gateway 8080     │
└───────────────────────┘       └───────────────────────┘
```

---

## 8.5.2 技术选型与工程组织

- **基础框架**：React 19 + TypeScript + Vite 7。
- **UI 风格与图标**：Vanilla CSS 现代暗色微光质感（深空蓝背景、玻璃拟态卡片），搭配 Lucide React 图标库。
- **目录结构**：

```
ota-portfolio-demo/
├── src/
│   ├── App.tsx                 # 主交互界面（拓扑图、调用回放、证据面板）
│   ├── styles.css              # 现代化暗色主题样式
│   ├── data/
│   │   └── realSmokeEvidence.ts# 真实环境 Smoke 测试生成的持久化证据数据
│   └── lib/
│       ├── flowReplay.ts       # 纯函数业务调用状态机（无副作用）
│       ├── flowReplay.test.ts  # 状态机单元测试
│       └── realApiClient.ts    # 真实网关 API 客户端
├── package.json
└── vite.config.ts
```

---

## 8.5.3 核心状态机架构 (`flowReplay.ts`)

为了在没有后端进程时依然能准确演示高并发库存扣减与 Kafka 事件状态，状态机采用纯函数设计，接收当前状态与用户操作，返回下一个不可变状态快照：

```typescript
/**
 * 前端交互触发的仿真动作类型联合体。
 */
export type ReplayAction =
  | 'login'          // 模拟网关登录鉴权并获取 Token
  | 'searchHotel'    // 模拟搜索并加载酒店基础元数据
  | 'queryRoomTypes' // 模拟查询酒店房型
  | 'queryInventory' // 模拟按日历范围拉取价库
  | 'query'          // 综合查询
  | 'create'         // 创建订单并触发库存冻结
  | 'pay'            // 支付订单并触发库存扣减及 Outbox 事件写入
  | 'cancel'         // 取消订单并释放冻结库存
  | 'reset';         // 重置回初始演示状态

/** 订单全生命周期状态枚举类型。 */
export type OrderStatus = 'READY' | 'WAIT_PAY' | 'PAID' | 'CANCELLED';

/** 本地事务消息关联的 Kafka 领域事件 Topic。 */
export type OutboxTopic = 'none' | 'order-paid' | 'order-cancelled';

/** 本地事务消息表发送状态。 */
export type OutboxStatus = 'NONE' | 'PENDING' | 'SENT';

/**
 * 库存核心三要素镜像快照接口。
 */
export interface InventorySnapshot {
  available: number; // 可售配额
  frozen: number;    // 已冻结预占配额
  booked: number;    // 最终已售出核销配额
}

/**
 * 前端回放演示聚合状态对象。
 */
export interface ReplayState {
  currentStep: number;         // 当前流转步骤索引 (-1 代表未开始)
  orderId: string;             // 模拟全局订单唯一 ID
  orderStatus: OrderStatus;    // 当前订单状态
  inventory: InventorySnapshot;// 当前库存状态快照
  outboxTopic: OutboxTopic;    // 关联发件箱主题
  outboxStatus: OutboxStatus;  // 本地消息表投递状态
  responseSummary: string;     // 页面底部技术原理与架构证据摘要
  traceId: string;             // 全链路分布式追踪 ID
}

/**
 * 生成 32 位标准十六进制 TraceId 字符串，模拟网关层链路染色。
 *
 * @returns 32 位全链路追踪 ID
 */
function generateTraceId(): string {
  const chars = '0123456789abcdef';
  let result = '';
  for (let i = 0; i < 32; i++) {
    result += chars[Math.floor(Math.random() * 16)];
  }
  return result;
}

/**
 * 构造初始演示状态快照。
 *
 * @returns 未激活链路时的基准初始状态对象
 */
export function createInitialReplayState(): ReplayState {
  return {
    currentStep: -1,
    orderId: 'DEMO-ORDER-20260530',
    orderStatus: 'READY',
    inventory: {
      available: 18,
      frozen: 0,
      booked: 2,
    },
    outboxTopic: 'none',
    outboxStatus: 'NONE',
    responseSummary: '等待演示开始，请先点击“模拟登录”激活链路。',
    traceId: '',
  };
}

/**
 * 纯函数业务状态机核心转换函数。
 * <p>
 * 纯函数特性：
 * 1. 无副作用：不修改入参 state，返回全新的不可变状态对象；
 * 2. 确定性推导：相同输入必产生相同输出；
 * 3. 严格契约守恒：available + frozen + booked 保持总和恒定不变。
 *
 * @param state  当前瞬时状态
 * @param action 用户触发的交互动作
 * @returns 跃迁推导后的下一个不可变状态
 */
export function runReplayAction(state: ReplayState, action: ReplayAction): ReplayState {
  // 重置操作：还原回基准状态
  if (action === 'reset') {
    return createInitialReplayState();
  }

  // 1. 模拟登录鉴权：生成 TraceId 并通过网关分发 JWT
  if (action === 'login') {
    return {
      ...state,
      currentStep: 0,
      traceId: generateTraceId(),
      responseSummary:
        '[Gateway] 拦截 HTTP 请求，Auth 服务校验 JWT Token 成功，提取 UserId=1001 并透传至下游各微服务。',
    };
  }

  // 2. 模拟酒店与价库查询：Cache-Aside 模式
  if (action === 'searchHotel' || action === 'query') {
    if (state.currentStep < 0) {
      return {
        ...state,
        responseSummary: '【操作拦截】请先执行“模拟登录”以获取有效 JWT Token。',
      };
    }
    return {
      ...state,
      currentStep: 1,
      responseSummary:
        '[Hotel] Cache-Aside 命中 Redis 缓存，返回酒店详情与房型基础信息（深圳湾万丽酒店 · 大床房）。',
    };
  }

  // 3. 创建订单与预占库存：Available -1, Frozen +1
  if (action === 'create') {
    if (state.currentStep < 1) {
      return {
        ...state,
        responseSummary: '【操作拦截】请先完成房型与价库查询。',
      };
    }
    if (state.inventory.available <= 0) {
      return {
        ...state,
        responseSummary: '【业务异常】当前房型已售罄，无法创建订单。',
      };
    }
    return {
      ...state,
      currentStep: 2,
      orderStatus: 'WAIT_PAY',
      inventory: {
        available: state.inventory.available - 1,
        frozen: state.inventory.frozen + 1,
        booked: state.inventory.booked,
      },
      responseSummary:
        '[Order] 订单创建成功 (WAIT_PAY)。Feign 同步调用 Inventory 执行乐观锁冻结 (Available -1, Frozen +1)。',
    };
  }

  // 4. 支付订单并触发 Outbox：Frozen -1, Booked +1, 投递 Kafka
  if (action === 'pay') {
    if (state.orderStatus !== 'WAIT_PAY') {
      return {
        ...state,
        responseSummary: '【操作拦截】当前状态不是待支付，无法执行支付。',
      };
    }
    return {
      ...state,
      currentStep: 3,
      orderStatus: 'PAID',
      inventory: {
        available: state.inventory.available,
        frozen: state.inventory.frozen - 1,
        booked: state.inventory.booked + 1,
      },
      outboxTopic: 'order-paid',
      outboxStatus: 'SENT',
      responseSummary:
        '[Order] 支付完成。扣减库存 (Frozen -1, Booked +1)，并在本地事务写入 outbox_event，可靠投递至 Kafka。',
    };
  }

  // 5. 取消订单并释放库存：Frozen -1, Available +1
  if (action === 'cancel') {
    if (state.orderStatus !== 'WAIT_PAY') {
      return {
        ...state,
        responseSummary: '【操作拦截】仅允许取消待支付状态的订单。',
      };
    }
    return {
      ...state,
      currentStep: 4,
      orderStatus: 'CANCELLED',
      inventory: {
        available: state.inventory.available + 1,
        frozen: state.inventory.frozen - 1,
        booked: state.inventory.booked,
      },
      outboxTopic: 'order-cancelled',
      outboxStatus: 'SENT',
      responseSummary:
        '[Order] 订单已取消。释放预占库存 (Frozen -1, Available +1)，投递 order-cancelled 事件。',
    };
  }

  return state;
}
```

---

## 8.5.4 真实微服务客户端与网关对接

当接入真实后端时，前端通过 `realApiClient.ts` 调用网关统一入口：

```typescript
/** 真实网关 API 连接配置选项。 */
export interface RealApiConfig {
  baseUrl: string; // 网关 Base URL（默认为 http://localhost:8080）
  token?: string;  // Bearer JWT Token 凭证
}

/**
 * 真实微服务网关 RESTful API 交互客户端封装。
 */
export class RealApiClient {
  private baseUrl: string;
  private token: string | null = null;

  /**
   * 构造函数：指定目标 API 网关地址。
   *
   * @param baseUrl 网关根路径，默认指向本地 8080 端口
   */
  constructor(baseUrl = 'http://localhost:8080') {
    this.baseUrl = baseUrl;
  }

  /**
   * 设置全局通用 JWT 认证令牌。
   *
   * @param token Bearer 令牌文本
   */
  public setToken(token: string) {
    this.token = token;
  }

  /**
   * 登录真实微服务网关获取 Token。
   *
   * @param username 用户名（默认 admin）
   * @param password 明文密码（默认 admin123）
   * @returns 签发出的 JWT Token 字符串
   * @throws 当接口返回非 200 或缺少 token 时抛出 Error
   */
  public async login(username = 'admin', password = 'admin123'): Promise<string> {
    const res = await fetch(`${this.baseUrl}/api/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password }),
    });
    const data = await res.json();
    if (data.code === 200 && data.data?.token) {
      this.token = data.data.token;
      return this.token;
    }
    throw new Error(data.message || '登录失败');
  }

  /**
   * 跨微服务查询指定酒店详情（携带当前登录用户 Token）。
   *
   * @param hotelId 酒店主键 ID
   * @returns 接口返回的统一 JSON 数据载荷
   */
  public async queryHotel(hotelId: number): Promise<any> {
    const headers: Record<string, string> = {};
    if (this.token) {
      headers['Authorization'] = `Bearer ${this.token}`;
    }
    const res = await fetch(`${this.baseUrl}/api/hotel/${hotelId}`, { headers });
    return res.json();
  }
}
```

---

## 8.5.5 微服务分层拓扑与可视化联动

在界面中，拓扑图分为四层：
1. **接入网关层**：展示 `ota-gateway`。
2. **核心微服务层**：展示 `ota-auth`、`ota-user`、`ota-hotel`、`ota-inventory`、`ota-order`。
3. **事件驱动与异步分发层**：展示 `ota-supplier`、`ota-channel`、Kafka Cluster。
4. **数据存储层**：展示 MySQL 8.0（订单库、价库）与 Redis 集群。

用户在左侧操作栏点击“创建订单”时，界面会动态点亮：
`客户端 ➔ 网关 (8080) ➔ 订单服务 (8085) ➔ Feign RPC ➔ 库存服务 (8084) ➔ MySQL` 的调用路径，并实时同步更新库存三要素（Available、Frozen、Booked）的数值跳变。

---

## 8.5.6 本地运行与打包部署

```bash
# 1. 进入前端工程目录
cd ota-portfolio-demo

# 2. 安装项目依赖
npm install

# 3. 运行本地开发服务
npm run dev
# 浏览器访问：http://localhost:5173

# 4. 执行状态机单元测试
npm run test

# 5. 生产环境构建打包（产物位于 dist/ 目录，可直接丢入 Nginx）
npm run build
```

