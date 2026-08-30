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
  res.json({ status: 'ok', service: 'NIM Proxy' });
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

    // Minimal cleaning
    delete body.extra_body;
    delete body.logit_bias;

    const modelName = (body.model || '').toLowerCase();

    // Only force thinking on models that need the old method
    if (modelName.includes('gemma') || modelName.includes('minimax')) {
      body.chat_template_kwargs = { enable_thinking: true };
    }

    // DeepSeek only
    if (modelName.includes('deepseek')) {
      body.reasoning_effort = 'high';
    }

    // Kimi K3 → send nothing extra (completely clean)
    // (We removed reasoning_effort temporarily for testing)

    const isStreaming = body.stream === true;

    const response = await axios({
      method: 'post',
      url: `${NIM_BASE}/chat/completions`,
      headers: {
        'Authorization': `Bearer ${NIM_API_KEY}`,
        'Content-Type': 'application/json',
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
      try {
        message = err.response.data.error?.message || JSON.stringify(err.response.data);
      } catch (e) {
        message = err.message;
      }
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
  console.log(`NIM Proxy running on port ${PORT}`);
});
