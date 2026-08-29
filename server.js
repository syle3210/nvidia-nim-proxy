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
  if (!NIM_API_KEY) {
    return res.status(500).json({ error: { message: 'NIM_API_KEY not set' } });
  }

  try {
    const body = { ...req.body };

    // Remove fields that commonly cause 400s
    delete body.extra_body;
    delete body.logit_bias;
    delete body.presence_penalty;
    delete body.frequency_penalty;
    delete body.n;

    const modelName = (body.model || '').toLowerCase();

    // Gemma & MiniMax
    if (modelName.includes('gemma') || modelName.includes('minimax')) {
      body.chat_template_kwargs = { enable_thinking: true };
    }

    // Kimi K3 - keep it very clean
    if (modelName.includes('kimi-k3') || modelName.includes('kimi_k3')) {
      body.reasoning_effort = 'high';   // or "max"
      // Do NOT send chat_template_kwargs for Kimi K3
    }

    // DeepSeek
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
      timeout: 180000,
    });

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

    const status = err.response?.status || 500;
    let message = err.message;

    if (err.response?.data) {
      message = err.response.data.error?.message || JSON.stringify(err.response.data);
    }

    res.status(status).json({
      error: {
        message: message,
        type: 'upstream_error',
        code: status
      }
    });
  }
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Clean NIM Proxy running on port ${PORT}`);
});
