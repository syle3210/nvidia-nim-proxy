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

    // Clean problematic fields
    delete body.extra_body;
    delete body.logit_bias;
    delete body.presence_penalty;
    delete body.frequency_penalty;
    delete body.n;
    delete body.seed;

    const modelName = (body.model || '').toLowerCase();

    if (modelName.includes('gemma') || modelName.includes('minimax')) {
      body.chat_template_kwargs = { enable_thinking: true };
    }

    if (modelName.includes('kimi-k3') || modelName.includes('kimi_k3')) {
      body.reasoning_effort = 'high';
    }

    if (modelName.includes('deepseek')) {
      body.reasoning_effort = 'high';
    }

    // Always request non-stream first so we can properly read errors
    const isStreaming = body.stream === true;
    body.stream = false; // Force non-stream to debug the 400

    const response = await axios({
      method: 'post',
      url: `${NIM_BASE}/chat/completions`,
      headers: {
        'Authorization': `Bearer ${NIM_API_KEY}`,
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0'
      },
      data: body,
      timeout: 60000,
      validateStatus: () => true,
      responseType: 'text'   // Force text so we can always read the body
    });

    console.error('===== NVIDIA RESPONSE =====');
    console.error('Status:', response.status);
    console.error('Body:', response.data);
    console.error('===========================');

    if (response.status !== 200) {
      return res.status(response.status).json({
        error: {
          message: response.data || `NVIDIA returned status ${response.status}`,
          type: 'upstream_error',
          code: response.status
        }
      });
    }

    // If we reach here, it worked (even though we forced non-stream for debugging)
    try {
      const jsonData = JSON.parse(response.data);
      res.json(jsonData);
    } catch (e) {
      res.json({ error: { message: 'Failed to parse NVIDIA response' } });
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
