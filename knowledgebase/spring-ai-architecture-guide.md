# ota-cloud Spring AI 大模型微服务赋能全景架构指南

> **定位**：系统性梳理 `ota-cloud` 中基于 Spring AI 1.x 规范构建的**智能化出行微服务（ota-ai）架构方案**。解析大语言模型（LLM）API 统一适配、Prompt 结构化工程、无 Key 稳定降级机制、Java 21 虚拟线程支持以及传统微服务与 AI 的职责边界。

---

## 一、架构定位与设计模式：LLM Assistant Pattern

在传统 OTA 平台上，用户搜索酒店通常基于生硬的关键字匹配（如“南山”、“双床”），无法理解自然语言中复杂的情境诉求（如“带5岁小孩去深圳湾万象城，预算1000左右求推荐”）：
- **智能推荐（Intelligent Recommendation）**：通过 Prompt 工程将非结构化的用户旅行意图转换为结构化的选房建议；
- **强事实与弱事实分离（Boundary Isolation）**：
  - **强事实（绝对严谨）**：实时房价、剩余可用库存、订单支付扣款，**绝对由底层关系型数据库和 Java 领域服务计算**，禁止由大模型自由发挥（避免大模型幻觉导致超卖或资损）；
  - **弱事实（创意生成）**：特色卖点提炼、智能客服常见问题解答、周边游玩攻略，交由 LLM 生成；
- **开箱即用与优雅降级（Graceful Degradation）**：系统在未配置 `AI_API_KEY` 或大模型接口偶发超时抖动时，绝不抛出 500 异常，而是无缝回退至兜底文案，保障交易主流程不受任何影响。

```
【客户端用户提问: 深圳湾带娃高性价比酒店】
                     │
                     ▼
┌────────────────────────────────────────────────────────┐
│               ota-ai 智能辅助微服务 (8088)              │
│                                                        │
│  1. AiAvailability 检测 (检查环境变量是否开启 AI)       │
│  2. Prompt 模版组装 (设定专家角色、输出边界与反作弊约束)│
│  3. RestClient / Spring AI Client 发送调用              │
│  4. 异常捕获与超时防御 (兜底 fallback)                 │
└────────────────────────────┬───────────────────────────┘
                             │
            ┌────────────────┴────────────────┐
            ▼ (未配置 Key 或网络故障)          ▼ (配置有效 Key)
  【返回安全兜底提示文案】               【DeepSeek / OpenAI 大模型推理】
  “AI推荐暂时不可用，请联系管理员...”     返回生动的个性化选房建议
```

---

## 二、微服务拓扑与角色分工

1. **`ota-ai`（智能辅助中心，端口 8088）**：
   - 暴露 `/api/ai/recommend`：多条件意图识别与酒店智能推荐；
   - 暴露 `/api/ai/ask`：针对指定酒店 ID 的 FAQ 智能问答；
   - 接入 Gateway 路由：由外部统一入口 `/api/ai/**` 转发，享有全局 TraceId 与 Sentinel 流量防护。

---

## 三、模型适配与参数矩阵

| 配置项 | 环境变量默认值 | 作用说明 |
| :--- | :--- | :--- |
| **`ai.enabled`** | `false` | 全局功能开关，默认为关闭状态，保障开箱启动无门槛 |
| **`ai.base-url`** | `https://api.deepseek.com/v1` | 大模型 OpenAI 兼容接口地址（支持 DeepSeek / 通义千问等） |
| **`ai.model`** | `deepseek-chat` | 调用的基模名称 |
| **`ai.timeout-millis`** | `30000` (30s) | 大模型长文本生成超时熔断时间 |
| **`temperature`** | `0.7` | 平衡推荐创造力与事实稳定性 |

---

## 四、关键落地技术细节

### 1. 结构化 Prompt 与优雅降级实现（`AiRecommendService.java`）

```java
package com.ota.cloud.ai.service;

import com.ota.cloud.ai.config.AiAvailability;
import com.ota.cloud.ai.config.AiProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class AiRecommendService {

    private static final String UNAVAILABLE_RECOMMEND = "AI 推荐功能暂不可用，请联系管理员配置 API Key";
    private static final String TEMPORARY_RECOMMEND_ERROR = "AI 推荐暂时无法使用，请稍后再试";

    private final AiAvailability availability;
    private final AiProperties properties;
    private final RestClient.Builder restClientBuilder;

    public String recommend(String city, String budget, List<String> preferences) {
        // 1. 无凭据防打崩检查
        if (!availability.isAvailable()) {
            return UNAVAILABLE_RECOMMEND;
        }

        // 2. 强边界约束的 Prompt 组装
        String prompt = """
                你是一个专业的酒店推荐助手。请根据用户需求推荐 3-5 家酒店。
                城市：%s
                预算：%s
                偏好：%s
                请包含酒店名称、50字以内推荐理由、建议房型、预估价格。
                """.formatted(city, budget, String.join("、", preferences));

        return callModel(prompt, TEMPORARY_RECOMMEND_ERROR);
    }

    private String callModel(String prompt, String errorMessage) {
        try {
            Map<String, Object> request = Map.of(
                    "model", properties.getModel(),
                    "messages", List.of(Map.of("role", "user", "content", prompt)),
                    "temperature", 0.7
            );

            ChatCompletionResponse response = restClientBuilder
                    .baseUrl(properties.getBaseUrl())
                    .build()
                    .post()
                    .uri("/chat/completions")
                    .contentType(MediaType.APPLICATION_JSON)
                    .headers(headers -> headers.setBearerAuth(availability.getApiKey()))
                    .body(request)
                    .retrieve()
                    .body(ChatCompletionResponse.class);

            if (response == null || response.choices() == null || response.choices().isEmpty()) {
                return errorMessage;
            }
            return response.choices().get(0).message().content();
        } catch (Exception e) {
            log.error("调用大语言模型发生异常", e);
            return errorMessage;
        }
    }
}
```

### 2. 拥抱 Java 21 虚拟线程（Virtual Threads）

大模型接口调用具有“长耗时（通常在 2~8 秒）、高等待”的典型 I/O 密集型特征。在 `application.yml` 中开启：
```yaml
spring:
  threads:
    virtual:
      enabled: true
```
虚拟线程以极低内存开销承接长并发挂起，杜绝因外部大模型响应慢耗尽系统物理线程池。
