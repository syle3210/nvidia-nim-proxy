// server.js - Clean NVIDIA NIM Proxy for Janitor AI (GLM-5.2 focused)
const express = require('express');
const cors = require('cors');
const axios = require('axios');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json({ limit: '100mb' }));
app.use(express.urlencoded({ limit: '100mb', extended: true }));

const NIM_API_BASE = process.env.NIM_API_BASE || 'https://integrate.api.nvidia.com/v1';
const NIM_API_KEY = process.env.NIM_API_KEY;

const SHOW_REASONING = process.env.SHOW_REASONING === 'false' || false;
const ENABLE_THINKING_MODE = process.env.ENABLE_THINKING_MODE === 'false' || false;

const MODEL_MAPPING = {
  'glm-pro': 'z-ai/glm-5.2',
  'gpt-4': 'deepseek-ai/deepseek-v4-pro',
  'gpt-4o': 'deepseek-ai/deepseek-v4-flash',
  'gpt-3.5-turbo': 'nvidia/nemotron-3-ultra-550b-a55b',
  'minimax': 'minimaxai/minimax-m3',
  'kimi': 'moonshotai/kimi-k2.6',
  'step-flash': 'stepfun-ai/step-3.7-flash',
  'gemma': 'google/gemma-4-31b-it',
};

const FALLBACK_CHAIN = [
  'z-ai/glm-5.2',
  'deepseek-ai/deepseek-v4-flash',
  'mistralai/mistral-medium-3.5-128b',
  'minimaxai/minimax-m3',
];

async function makeNimRequest(nimRequest, stream) {
  const modelsToTry = [nimRequest.model, ...FALLBACK_CHAIN.filter(m => m !== nimRequest.model)];

  for (let i = 0; i < modelsToTry.length; i++) {
    const modelAttempt = modelsToTry[i];
    try {
      const response = await axios.post(`${NIM_API_BASE}/chat/completions`, {
        ...nimRequest,
        model: modelAttempt
      }, {
        headers: {
          'Authorization': `Bearer ${NIM_API_KEY}`,
          'Content-Type': 'application/json'
        },
        responseType: stream ? 'stream' : 'json',
        timeout: 180000
      });

      if (modelAttempt !== nimRequest.model) {
        console.log(`Fell back to: ${modelAttempt}`);
      }
      return response;
    } catch (err) {
      if (err.response?.status === 429 && i < modelsToTry.length - 1) {
        continue;
      }
      throw err;
    }
  }
}

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    service: 'Clean NIM Proxy',
    reasoning: SHOW_REASONING,
    thinking: ENABLE_THINKING_MODE
  });
});

app.get('/', (req, res) => {
  res.json({ status: 'Clean NVIDIA NIM Proxy running' });
});

app.post('/v1/chat/completions', async (req, res) => {
  try {
    if (!NIM_API_KEY) {
      return res.status(500).json({ error: { message: 'NIM_API_KEY not set' } });
    }

    const { model, messages, temperature, max_tokens, stream } = req.body;

    let nimModel = MODEL_MAPPING[model] || model;

    // If the model name is not in mapping, try using it directly
    if (!MODEL_MAPPING[model]) {
      nimModel = model;
    }

    const nimRequest = {
      model: nimModel,
      messages: messages,               // Your system prompt is passed exactly as Janitor sends it
      temperature: temperature ?? 0.9,
      max_tokens: max_tokens || 4096,
      stream: stream || false
    };

    // Light force thinking only for GLM-5.2
    if (ENABLE_THINKING_MODE && (nimModel.includes('glm-5.2') || nimModel.includes('z-ai/glm'))) {
      nimRequest.chat_template_kwargs = { enable_thinking: true };
      // Using "high" instead of "max" to reduce over-formal/short reasoning on free endpoint
      nimRequest.reasoning_effort = "high";
    }

    const response = await makeNimRequest(nimRequest, stream || false);

    if (stream) {
      res.setHeader('Content-Type', 'text/event-stream');
      res.setHeader('Cache-Control', 'no-cache');
      res.setHeader('Connection', 'keep-alive');

      response.data.on('data', (chunk) => {
        res.write(chunk);
      });

      response.data.on('end', () => res.end());
      response.data.on('error', () => res.end());
    } else {
      const data = response.data;

      // Only show reasoning if the model actually returned it
      if (SHOW_REASONING && data.choices?.[0]?.message?.reasoning_content) {
        data.choices[0].message.content =
          '<think>\n' + data.choices[0].message.reasoning_content + '\n</think>\n\n' +
          (data.choices[0].message.content || '');
      }

      res.json(data);
    }

  } catch (error) {
    const status = error.response?.status || 500;
    const message = error.response?.data?.error?.message || error.message || 'Proxy error';
    res.status(status).json({
      error: {
        message: message,
        type: 'invalid_request_error',
        code: status
      }
    });
  }
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Clean NIM Proxy running on port ${PORT}`);
});
