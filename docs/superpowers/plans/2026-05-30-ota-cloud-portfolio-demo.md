# OTA Cloud Portfolio Demo Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a static, screenshot-friendly OTA Cloud single-project portfolio page for freelance platform project showcases.

**Architecture:** Create an independent Vite + React + TypeScript app under `ota-portfolio-demo/`. The app uses static, desensitized project data plus the recorded Chapter 8 smoke evidence, and all interactive order/payment/cancel replay state stays in the browser without writing to the live backend.

**Tech Stack:** Vite, React, TypeScript, Vitest, CSS, static data modules.

---

## File Structure

- Create `ota-portfolio-demo/package.json`: app metadata, scripts, dependencies.
- Create `ota-portfolio-demo/index.html`: Vite HTML shell.
- Create `ota-portfolio-demo/tsconfig.json`: TypeScript compiler options.
- Create `ota-portfolio-demo/vite.config.ts`: Vite and Vitest config.
- Create `ota-portfolio-demo/src/main.tsx`: React mount entry.
- Create `ota-portfolio-demo/src/App.tsx`: page composition and flow replay state wiring.
- Create `ota-portfolio-demo/src/data/demoData.ts`: static metrics, service status, smoke evidence, architecture nodes, and flow steps.
- Create `ota-portfolio-demo/src/lib/flowReplay.ts`: pure state machine for replaying create/pay/cancel flows.
- Create `ota-portfolio-demo/src/lib/flowReplay.test.ts`: unit tests for replay logic.
- Create `ota-portfolio-demo/src/styles.css`: all visual styling.
- Create `ota-portfolio-demo/README.md`: local preview and static hosting notes.
- Modify `cloud-tutorial-progress.md`: add Chapter 8.5 status after implementation is verified.

The app stays separate from all Spring Boot modules, so no Mapper SQL, Java Mapper annotation, or Service query-wrapper rule is touched.

## Task 1: Scaffold The Static App

**Files:**
- Create: `ota-portfolio-demo/package.json`
- Create: `ota-portfolio-demo/index.html`
- Create: `ota-portfolio-demo/tsconfig.json`
- Create: `ota-portfolio-demo/vite.config.ts`
- Create: `ota-portfolio-demo/src/main.tsx`
- Create: `ota-portfolio-demo/src/App.tsx`
- Create: `ota-portfolio-demo/src/styles.css`

- [ ] **Step 1: Create package metadata**

Write `ota-portfolio-demo/package.json`:

```json
{
  "name": "ota-cloud-portfolio-demo",
  "version": "1.0.0",
  "private": true,
  "type": "module",
  "scripts": {
    "dev": "vite --host 0.0.0.0",
    "build": "tsc -b && vite build",
    "test": "vitest run",
    "preview": "vite preview --host 0.0.0.0"
  },
  "dependencies": {
    "@vitejs/plugin-react": "^5.0.0",
    "vite": "^7.0.0",
    "typescript": "^5.8.0",
    "react": "^19.0.0",
    "react-dom": "^19.0.0",
    "lucide-react": "^0.468.0"
  },
  "devDependencies": {
    "vitest": "^3.0.0",
    "jsdom": "^26.0.0",
    "@testing-library/react": "^16.0.0",
    "@testing-library/jest-dom": "^6.6.0",
    "@types/react": "^19.0.0",
    "@types/react-dom": "^19.0.0"
  }
}
```

- [ ] **Step 2: Create Vite HTML shell**

Write `ota-portfolio-demo/index.html`:

```html
<!doctype html>
<html lang="zh-CN">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <meta
      name="description"
      content="OTA Cloud 酒店预订微服务平台作品展示，覆盖订单、库存、Kafka Outbox、缓存和真实环境回归验证。"
    />
    <title>OTA Cloud 酒店预订微服务平台作品展示</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
```

- [ ] **Step 3: Create TypeScript and Vite config**

Write `ota-portfolio-demo/tsconfig.json`:

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "useDefineForClassFields": true,
    "lib": ["DOM", "DOM.Iterable", "ES2022"],
    "allowJs": false,
    "skipLibCheck": true,
    "esModuleInterop": true,
    "allowSyntheticDefaultImports": true,
    "strict": true,
    "forceConsistentCasingInFileNames": true,
    "module": "ESNext",
    "moduleResolution": "Node",
    "resolveJsonModule": true,
    "isolatedModules": true,
    "noEmit": true,
    "jsx": "react-jsx"
  },
  "include": ["src"],
  "references": []
}
```

Write `ota-portfolio-demo/vite.config.ts`:

```ts
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  test: {
    environment: 'jsdom',
    globals: true,
  },
});
```

- [ ] **Step 4: Create minimal React entry**

Write `ota-portfolio-demo/src/main.tsx`:

```tsx
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import App from './App';
import './styles.css';

createRoot(document.getElementById('root') as HTMLElement).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
```

Write `ota-portfolio-demo/src/App.tsx`:

```tsx
export default function App() {
  return (
    <main className="page-shell">
      <section className="hero-section">
        <p className="eyebrow">OTA Cloud Portfolio Demo</p>
        <h1>OTA Cloud 酒店预订微服务平台</h1>
        <p className="hero-copy">
          一个覆盖登录鉴权、酒店价库、订单库存、Kafka Outbox 和真实环境回归验证的微服务项目。
        </p>
      </section>
    </main>
  );
}
```

Write `ota-portfolio-demo/src/styles.css`:

```css
:root {
  font-family: Inter, "PingFang SC", "Microsoft YaHei", system-ui, sans-serif;
  color: #172033;
  background: #f4f7fb;
  line-height: 1.5;
}

* {
  box-sizing: border-box;
}

body {
  margin: 0;
}

button {
  font: inherit;
}

.page-shell {
  min-height: 100vh;
}

.hero-section {
  min-height: 72vh;
  display: grid;
  align-content: center;
  gap: 18px;
  padding: 72px clamp(24px, 6vw, 96px);
  background: #eef3f8;
}

.eyebrow {
  margin: 0;
  color: #2f6b8f;
  font-size: 14px;
  font-weight: 700;
  letter-spacing: 0;
  text-transform: uppercase;
}

h1 {
  max-width: 920px;
  margin: 0;
  font-size: clamp(40px, 6vw, 76px);
  line-height: 1.02;
  letter-spacing: 0;
}

.hero-copy {
  max-width: 760px;
  margin: 0;
  color: #4a5b72;
  font-size: 20px;
}
```

- [ ] **Step 5: Install dependencies**

Run:

```bash
cd /Users/fuxinwei/work/ota-cloud/ota-portfolio-demo
npm install
```

Expected: `node_modules/` and `package-lock.json` are created.

- [ ] **Step 6: Verify scaffold builds**

Run:

```bash
cd /Users/fuxinwei/work/ota-cloud/ota-portfolio-demo
npm run build
```

Expected: TypeScript and Vite finish with exit code 0 and create `dist/`.

- [ ] **Step 7: Checkpoint**

Record changed files with:

```bash
find /Users/fuxinwei/work/ota-cloud/ota-portfolio-demo -maxdepth 2 -type f | sort
```

Expected: the scaffold files, `package-lock.json`, and Vite output are visible. If this directory becomes a Git repository later, commit this checkpoint as `feat: scaffold ota portfolio demo`.

## Task 2: Add Static Demo Data And Flow Replay Tests

**Files:**
- Create: `ota-portfolio-demo/src/data/demoData.ts`
- Create: `ota-portfolio-demo/src/lib/flowReplay.ts`
- Create: `ota-portfolio-demo/src/lib/flowReplay.test.ts`

- [ ] **Step 1: Write flow replay tests first**

Write `ota-portfolio-demo/src/lib/flowReplay.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { createInitialReplayState, runReplayAction } from './flowReplay';

describe('flowReplay', () => {
  it('marks a paid replay as PAID and sends order-paid', () => {
    const state = createInitialReplayState();
    const created = runReplayAction(state, 'create');
    const paid = runReplayAction(created, 'pay');

    expect(paid.orderStatus).toBe('PAID');
    expect(paid.inventory.available).toBe(17);
    expect(paid.inventory.frozen).toBe(0);
    expect(paid.inventory.booked).toBe(3);
    expect(paid.outboxTopic).toBe('order-paid');
    expect(paid.outboxStatus).toBe('SENT');
  });

  it('marks a cancelled replay as CANCELLED and sends order-cancelled', () => {
    const state = createInitialReplayState();
    const created = runReplayAction(state, 'create');
    const cancelled = runReplayAction(created, 'cancel');

    expect(cancelled.orderStatus).toBe('CANCELLED');
    expect(cancelled.inventory.available).toBe(18);
    expect(cancelled.inventory.frozen).toBe(0);
    expect(cancelled.inventory.booked).toBe(2);
    expect(cancelled.outboxTopic).toBe('order-cancelled');
    expect(cancelled.outboxStatus).toBe('SENT');
  });

  it('resets replay state back to smoke baseline', () => {
    const paid = runReplayAction(runReplayAction(createInitialReplayState(), 'create'), 'pay');
    const reset = runReplayAction(paid, 'reset');

    expect(reset.orderStatus).toBe('READY');
    expect(reset.inventory.available).toBe(18);
    expect(reset.outboxTopic).toBe('none');
    expect(reset.currentStep).toBe(0);
  });
});
```

- [ ] **Step 2: Run tests to verify red**

Run:

```bash
cd /Users/fuxinwei/work/ota-cloud/ota-portfolio-demo
npm test -- src/lib/flowReplay.test.ts
```

Expected: FAIL because `./flowReplay` does not exist.

- [ ] **Step 3: Implement flow replay logic**

Write `ota-portfolio-demo/src/lib/flowReplay.ts`:

```ts
export type ReplayAction = 'create' | 'pay' | 'cancel' | 'reset';
export type OrderStatus = 'READY' | 'WAIT_PAY' | 'PAID' | 'CANCELLED';
export type OutboxTopic = 'none' | 'order-paid' | 'order-cancelled';
export type OutboxStatus = 'NONE' | 'PENDING' | 'SENT';

export interface InventorySnapshot {
  available: number;
  frozen: number;
  booked: number;
}

export interface ReplayState {
  currentStep: number;
  orderId: string;
  orderStatus: OrderStatus;
  inventory: InventorySnapshot;
  outboxTopic: OutboxTopic;
  outboxStatus: OutboxStatus;
  responseSummary: string;
}

export function createInitialReplayState(): ReplayState {
  return {
    currentStep: 0,
    orderId: 'DEMO-ORDER-20260530',
    orderStatus: 'READY',
    inventory: {
      available: 18,
      frozen: 0,
      booked: 2,
    },
    outboxTopic: 'none',
    outboxStatus: 'NONE',
    responseSummary: '等待开始演示。基线来自第 8 章真实 smoke：available=18, frozen=0, booked=2。',
  };
}

export function runReplayAction(state: ReplayState, action: ReplayAction): ReplayState {
  if (action === 'reset') {
    return createInitialReplayState();
  }

  if (action === 'create') {
    return {
      ...state,
      currentStep: 2,
      orderStatus: 'WAIT_PAY',
      inventory: {
        available: Math.max(0, state.inventory.available - 1),
        frozen: state.inventory.frozen + 1,
        booked: state.inventory.booked,
      },
      outboxTopic: 'none',
      outboxStatus: 'NONE',
      responseSummary: '创建订单成功：订单进入 WAIT_PAY，库存从 available 转入 frozen。',
    };
  }

  if (action === 'pay') {
    return {
      ...state,
      currentStep: 4,
      orderStatus: 'PAID',
      inventory: {
        available: state.inventory.available,
        frozen: Math.max(0, state.inventory.frozen - 1),
        booked: state.inventory.booked + 1,
      },
      outboxTopic: 'order-paid',
      outboxStatus: 'SENT',
      responseSummary: '支付成功：冻结库存扣减为 booked，Outbox 投递 order-paid 并标记 SENT。',
    };
  }

  return {
    ...state,
    currentStep: 4,
    orderStatus: 'CANCELLED',
    inventory: {
      available: state.inventory.available + state.inventory.frozen,
      frozen: 0,
      booked: state.inventory.booked,
    },
    outboxTopic: 'order-cancelled',
    outboxStatus: 'SENT',
    responseSummary: '取消成功：冻结库存释放回 available，Outbox 投递 order-cancelled 并标记 SENT。',
  };
}
```

- [ ] **Step 4: Run tests to verify green**

Run:

```bash
cd /Users/fuxinwei/work/ota-cloud/ota-portfolio-demo
npm test -- src/lib/flowReplay.test.ts
```

Expected: PASS, 3 tests.

- [ ] **Step 5: Add static data**

Write `ota-portfolio-demo/src/data/demoData.ts`:

```ts
export const stackTags = [
  'Spring Boot 3',
  'Spring Cloud',
  'Nacos',
  'Kafka Outbox',
  'Redis Cache',
  'MySQL',
  'Docker',
  'Focused Tests',
];

export const businessMetrics = [
  { label: '核心服务', value: '6', hint: 'Gateway/Auth/User/Hotel/Inventory/Order' },
  { label: '测试用例', value: '43', hint: '第 8 章 focused regression' },
  { label: '订单金额', value: '888.00', hint: '最近真实支付 smoke' },
  { label: 'Outbox', value: 'SENT', hint: 'order-paid / order-cancelled' },
];

export const serviceStatuses = [
  { name: 'Gateway', detail: '外部统一入口', status: 'healthy' },
  { name: 'Auth/User', detail: '登录鉴权和用户信息', status: 'healthy' },
  { name: 'Hotel', detail: '酒店内容与 Redis 缓存', status: 'healthy' },
  { name: 'Inventory', detail: '价库与库存状态机', status: 'healthy' },
  { name: 'Order', detail: '订单状态机与 Outbox', status: 'healthy' },
  { name: 'Kafka', detail: 'order-paid / order-cancelled', status: 'healthy' },
];

export const smokeEvidence = [
  {
    title: '支付链路',
    orderId: '5ee51f19135444f890823514693f8513',
    status: 'PAID',
    event: 'order-paid SENT',
  },
  {
    title: '取消链路',
    orderId: '9aeae2449e534d98ae542ddd3f2d197b',
    status: 'CANCELLED',
    event: 'order-cancelled SENT',
  },
];

export const architectureNodes = [
  { name: 'Gateway', role: '统一入口' },
  { name: 'Auth/User', role: '认证和用户' },
  { name: 'Hotel', role: '酒店内容' },
  { name: 'Inventory', role: '库存价库' },
  { name: 'Order', role: '订单状态机' },
  { name: 'Kafka Outbox', role: '可靠事件' },
  { name: 'MySQL/Redis/Nacos', role: '数据和注册配置' },
];
```

- [ ] **Step 6: Run full app test**

Run:

```bash
cd /Users/fuxinwei/work/ota-cloud/ota-portfolio-demo
npm test
```

Expected: PASS, 3 tests.

## Task 3: Build The Portfolio Page UI

**Files:**
- Modify: `ota-portfolio-demo/src/App.tsx`
- Modify: `ota-portfolio-demo/src/styles.css`

- [ ] **Step 1: Replace App with full page composition**

Write `ota-portfolio-demo/src/App.tsx`:

```tsx
import { useMemo, useState } from 'react';
import {
  Activity,
  Boxes,
  CheckCircle2,
  Database,
  GitBranch,
  Hotel,
  Network,
  PackageCheck,
  Play,
  RefreshCcw,
  ShieldCheck,
} from 'lucide-react';
import {
  architectureNodes,
  businessMetrics,
  serviceStatuses,
  smokeEvidence,
  stackTags,
} from './data/demoData';
import { createInitialReplayState, runReplayAction } from './lib/flowReplay';

const capabilityItems = [
  '微服务后端设计',
  '订单与库存领域建模',
  'Kafka Outbox 可靠事件',
  'Redis 缓存与黑名单',
  'Nacos 服务发现',
  'Docker 真实环境部署',
];

export default function App() {
  const [replayState, setReplayState] = useState(createInitialReplayState);
  const paidEvidence = useMemo(() => smokeEvidence.find((item) => item.status === 'PAID'), []);

  return (
    <main className="page-shell">
      <section className="hero-section" id="top">
        <div className="hero-copy-block">
          <p className="eyebrow">Freelance Project Showcase</p>
          <h1>OTA Cloud 酒店预订微服务平台</h1>
          <p className="hero-copy">
            一个覆盖登录鉴权、酒店价库、订单库存、Kafka Outbox 和真实环境回归验证的微服务项目，适合作为酒店预订、订单履约和中后台系统的交付样板。
          </p>
          <div className="stack-row">
            {stackTags.map((tag) => (
              <span key={tag}>{tag}</span>
            ))}
          </div>
          <div className="hero-actions">
            <a href="#flow" className="primary-action">
              <Play size={18} />
              查看链路回放
            </a>
            <a href="#architecture" className="secondary-action">
              <Network size={18} />
              查看架构
            </a>
          </div>
        </div>
        <aside className="product-preview" aria-label="项目状态预览">
          <div className="preview-header">
            <div>
              <p className="panel-kicker">Live Evidence</p>
              <h2>真实回归基线</h2>
            </div>
            <CheckCircle2 className="healthy-icon" size={30} />
          </div>
          <div className="preview-grid">
            <div>
              <span>订单状态</span>
              <strong>{paidEvidence?.status}</strong>
            </div>
            <div>
              <span>最近金额</span>
              <strong>888.00</strong>
            </div>
            <div>
              <span>库存</span>
              <strong>18 / 0 / 2</strong>
            </div>
            <div>
              <span>Outbox</span>
              <strong>SENT</strong>
            </div>
          </div>
          <p className="preview-note">
            最近支付 smoke 订单：{paidEvidence?.orderId}
          </p>
        </aside>
      </section>

      <section className="content-section dashboard-section">
        <div className="section-heading">
          <p className="eyebrow">Business Dashboard</p>
          <h2>客户能一眼看懂的业务结果</h2>
          <p>用订单、库存、服务状态和可靠消息证明系统已经完成核心业务闭环。</p>
        </div>
        <div className="metric-grid">
          {businessMetrics.map((metric) => (
            <article className="metric-card" key={metric.label}>
              <span>{metric.label}</span>
              <strong>{metric.value}</strong>
              <p>{metric.hint}</p>
            </article>
          ))}
        </div>
        <div className="status-grid">
          {serviceStatuses.map((service) => (
            <article className="status-card" key={service.name}>
              <Activity size={20} />
              <div>
                <strong>{service.name}</strong>
                <p>{service.detail}</p>
              </div>
              <span className="status-pill">{service.status}</span>
            </article>
          ))}
        </div>
      </section>

      <section className="content-section flow-section" id="flow">
        <div className="section-heading">
          <p className="eyebrow">Interactive Replay</p>
          <h2>下单、支付、取消链路回放</h2>
          <p>公开页面只做前端状态回放，不写真实订单和库存，适合客户安全体验。</p>
        </div>
        <div className="flow-layout">
          <div className="flow-controls">
            <button onClick={() => setReplayState((state) => runReplayAction(state, 'create'))}>
              创建订单
            </button>
            <button onClick={() => setReplayState((state) => runReplayAction(state, 'pay'))}>
              支付订单
            </button>
            <button onClick={() => setReplayState((state) => runReplayAction(state, 'cancel'))}>
              取消订单
            </button>
            <button onClick={() => setReplayState((state) => runReplayAction(state, 'reset'))}>
              <RefreshCcw size={16} />
              重置
            </button>
          </div>
          <div className="flow-console">
            <div className="console-topline">
              <span>Order ID</span>
              <strong>{replayState.orderId}</strong>
            </div>
            <div className="state-grid">
              <div>
                <span>订单状态</span>
                <strong className={`state-badge state-${replayState.orderStatus.toLowerCase()}`}>
                  {replayState.orderStatus}
                </strong>
              </div>
              <div>
                <span>可用</span>
                <strong>{replayState.inventory.available}</strong>
              </div>
              <div>
                <span>冻结</span>
                <strong>{replayState.inventory.frozen}</strong>
              </div>
              <div>
                <span>已订</span>
                <strong>{replayState.inventory.booked}</strong>
              </div>
              <div>
                <span>Topic</span>
                <strong>{replayState.outboxTopic}</strong>
              </div>
              <div>
                <span>Outbox</span>
                <strong>{replayState.outboxStatus}</strong>
              </div>
            </div>
            <p className="response-summary">{replayState.responseSummary}</p>
          </div>
        </div>
      </section>

      <section className="content-section architecture-section" id="architecture">
        <div className="section-heading">
          <p className="eyebrow">Architecture</p>
          <h2>微服务架构和可靠事件链路</h2>
          <p>从 Gateway 到 Kafka Outbox，把酒店预订的核心链路拆成清晰服务边界。</p>
        </div>
        <div className="architecture-grid">
          {architectureNodes.map((node, index) => (
            <article className="architecture-node" key={node.name}>
              <span>{String(index + 1).padStart(2, '0')}</span>
              <strong>{node.name}</strong>
              <p>{node.role}</p>
            </article>
          ))}
        </div>
      </section>

      <section className="content-section proof-section">
        <div className="section-heading">
          <p className="eyebrow">Delivery Proof</p>
          <h2>交付能力和真实验证证据</h2>
        </div>
        <div className="proof-layout">
          <article className="capability-card">
            <h3>
              <PackageCheck size={22} />
              可交付能力
            </h3>
            <ul>
              {capabilityItems.map((item) => (
                <li key={item}>
                  <ShieldCheck size={16} />
                  {item}
                </li>
              ))}
            </ul>
          </article>
          <article className="evidence-card">
            <h3>
              <Database size={22} />
              第 8 章验证证据
            </h3>
            {smokeEvidence.map((item) => (
              <div className="evidence-row" key={item.orderId}>
                <span>{item.title}</span>
                <strong>{item.status}</strong>
                <code>{item.event}</code>
                <small>{item.orderId}</small>
              </div>
            ))}
            <p className="test-summary">Focused tests：Gateway/Auth/User 13，Hotel/Inventory/AI 18，Order 12。</p>
          </article>
        </div>
      </section>
    </main>
  );
}
```

- [ ] **Step 2: Replace CSS with full visual styling**

Write `ota-portfolio-demo/src/styles.css` with the complete styling from Task 3 implementation. Include:

```css
:root {
  font-family: Inter, "PingFang SC", "Microsoft YaHei", system-ui, sans-serif;
  color: #182033;
  background: #f3f6fa;
  line-height: 1.5;
}
```

Then add styles for these selectors exactly as used in `App.tsx`:

```text
body, button, a, .page-shell, .hero-section, .hero-copy-block, .eyebrow, h1, h2, .hero-copy,
.stack-row, .hero-actions, .primary-action, .secondary-action, .product-preview, .preview-header,
.healthy-icon, .panel-kicker, .preview-grid, .preview-note, .content-section, .section-heading,
.metric-grid, .metric-card, .status-grid, .status-card, .status-pill, .flow-layout, .flow-controls,
.flow-controls button, .flow-console, .console-topline, .state-grid, .state-badge, .state-ready,
.state-wait_pay, .state-paid, .state-cancelled, .response-summary, .architecture-grid,
.architecture-node, .proof-layout, .capability-card, .evidence-card, .capability-card ul,
.capability-card li, .evidence-row, .test-summary
```

Use stable card dimensions, responsive grids, and a restrained palette with distinct status colors. Do not use gradients, decorative orbs, or image-less marketing filler.

- [ ] **Step 3: Build to catch type errors**

Run:

```bash
cd /Users/fuxinwei/work/ota-cloud/ota-portfolio-demo
npm run build
```

Expected: PASS.

## Task 4: Polish CSS And Verify Screenshots

**Files:**
- Modify: `ota-portfolio-demo/src/styles.css`
- Create: `ota-portfolio-demo/README.md`

- [ ] **Step 1: Run local server**

Run:

```bash
cd /Users/fuxinwei/work/ota-cloud/ota-portfolio-demo
npm run dev -- --port 5174
```

Expected: Vite serves at `http://localhost:5174/`.

- [ ] **Step 2: Inspect in browser**

Open `http://localhost:5174/` and verify:

- First viewport shows project name, value, stack, and product proof.
- Dashboard section fits in a screenshot.
- Flow replay buttons update order status, inventory counters, and Outbox topic.
- Architecture section is readable.
- No text overlaps at desktop width.

- [ ] **Step 3: Add README**

Write `ota-portfolio-demo/README.md`:

```md
# OTA Cloud Portfolio Demo

这是 OTA Cloud 第 8.5 章作品展示 Demo：面向程序员客栈这类兼职平台的单项目案例页。

## 本地运行

```bash
npm install
npm run dev
```

## 构建

```bash
npm run build
```

## 数据说明

页面默认使用脱敏静态数据和第 8 章真实 smoke 证据，不直接写真实订单或库存。

- 支付链路：`5ee51f19135444f890823514693f8513`，`PAID`，`order-paid SENT`
- 取消链路：`9aeae2449e534d98ae542ddd3f2d197b`，`CANCELLED`，`order-cancelled SENT`
```

- [ ] **Step 4: Final verification**

Run:

```bash
cd /Users/fuxinwei/work/ota-cloud/ota-portfolio-demo
npm test
npm run build
```

Expected: tests pass and production build succeeds.

## Task 5: Update Project Progress

**Files:**
- Modify: `cloud-tutorial-progress.md`
- Modify: `README.md`

- [ ] **Step 1: Add Chapter 8.5 to progress table**

Modify `cloud-tutorial-progress.md` to insert after Chapter 8:

```md
| Chapter 8.5 | Done | 已新增 `ota-portfolio-demo` 作品展示静态站，面向程序员客栈等兼职平台展示单个 OTA Cloud 项目案例；页面采用混合型项目案例结构，包含首屏产品预览、业务大屏、前端交互链路回放、架构拓扑和第 8 章 smoke 证据。公开页面默认使用脱敏静态数据和真实 smoke 记录，不暴露后台账号、Token、数据库密码，也不允许公网访客直接写真实订单或库存。 |
```

- [ ] **Step 2: Add README link**

Modify the root `README.md` near the Chapter 8 smoke section:

```md
## 作品展示 Demo

第 8.5 章新增独立静态前端 `ota-portfolio-demo/`，用于程序员客栈这类兼职平台的单项目案例展示。

本地预览：

```bash
cd ota-portfolio-demo
npm install
npm run dev
```

生产构建：

```bash
cd ota-portfolio-demo
npm run build
```

页面使用脱敏静态数据和第 8 章真实 smoke 证据，不直接写真实订单或库存。
```

- [ ] **Step 3: Verify root constraints**

Run:

```bash
cd /Users/fuxinwei/work/ota-cloud
rg -n "@(Select|Update|Insert|Delete)\\b" ota-*/src/main/java || true
rg -n "(LambdaQueryWrapper|QueryWrapper)" ota-*/src/main/java || true
```

Expected: no output from either command.

- [ ] **Step 4: Final checkpoint**

Run:

```bash
cd /Users/fuxinwei/work/ota-cloud
find ota-portfolio-demo -maxdepth 3 -type f | sort
```

Expected: all static app files are present. If this directory becomes a Git repository later, commit this checkpoint as `feat: add ota cloud portfolio demo`.

## Self-Review

- Spec coverage: the plan covers static deployment, screenshot-friendly sections, interactive replay, architecture topology, smoke evidence, and privacy boundaries.
- Placeholder scan: no `TBD`, `TODO`, `FIXME`, or open-ended implementation steps are intentionally left.
- Type consistency: `ReplayState`, `ReplayAction`, `OrderStatus`, `OutboxTopic`, and `OutboxStatus` are defined in Task 2 and consumed by Task 3.
- Scope check: Chapter 9 supplier integration, real public backend writes, and personal multi-project homepage are excluded.
