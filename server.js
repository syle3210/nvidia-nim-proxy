import express from 'express';
import cors from 'cors';
import axios from 'axios';

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json({ limit: '50mb' }));

const NIM_API_KEY = process.env.NIM_API_KEY || process.env.NVIDIA_API_KEY;
const NIM_BASE = 'https://integrate.api.nvidia.com/v1';

app.get('/', (req, res) => {
  res.json({ status: 'ok', service: 'Clean NIM Proxy' });
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok', keyConfigured: !!NIM_API_KEY });
});

app.post('/v1/chat/completions', async (req, res) => {
  console.log('>>> Request received - Model:', req.body?.model);

  if (!NIM_API_KEY) {
    return res.status(500).json({ error: { message: 'NIM_API_KEY not set' } });
  }

  try {
    const body = { ...req.body };

    // Clean problematic fields
    delete body.extra_body;
    delete body.logit_bias;
    delete body.presence_penalty;
    delete body.frequency_penalty;
    delete body.n;
    delete body.seed;

    const modelName = (body.model || '').toLowerCase();

    // Thinking / Reasoning
    if (modelName.includes('gemma') || modelName.includes('minimax')) {
      body.chat_template_kwargs = { enable_thinking: true };
    }

    if (modelName.includes('kimi-k3') || modelName.includes('kimi_k3')) {
      body.reasoning_effort = 'high';
    }

    if (modelName.includes('deepseek')) {
      body.reasoning_effort = 'high';
    }

    const isStreaming = body.stream === true;

    const response = await axios({
      method: 'post',
      url: `${NIM_BASE}/chat/completions`,
      headers: {
        'Authorization': `Bearer ${NIM_API_KEY}`,
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0',
        ...(isStreaming ? { 'Accept': 'text/event-stream' } : {})
      },
      data: body,
      responseType: isStreaming ? 'stream' : 'json',
      timeout: 180000, // 3 minutes
      validateStatus: () => true
    });

    if (response.status !== 200) {
      let errorMsg = 'Unknown error';
      try {
        if (typeof response.data === 'string') {
          errorMsg = response.data;
        } else if (response.data?.error?.message) {
          errorMsg = response.data.error.message;
        } else {
          errorMsg = JSON.stringify(response.data).slice(0, 400);
        }
      } catch (e) {
        errorMsg = `NVIDIA returned status ${response.status}`;
      }

      console.error('NVIDIA Error:', response.status, errorMsg);

      return res.status(response.status).json({
        error: {
          message: errorMsg,
          type: 'upstream_error',
          code: response.status
        }
      });
    }

    if (isStreaming) {
      res.setHeader('Content-Type', 'text/event-stream');
      res.setHeader('Cache-Control', 'no-cache');
      res.setHeader('Connection', 'keep-alive');
      res.setHeader('Access-Control-Allow-Origin', '*');
      response.data.pipe(res);
    } else {
      res.json(response.data);
    }

  } catch (err) {
    console.error('Proxy error:', err.message);
    res.status(500).json({
      error: {
        message: err.message || 'Internal proxy error',
        type: 'proxy_error',
        code: 500
      }
    });
  }
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Clean NIM Proxy running on port ${PORT}`);
});
