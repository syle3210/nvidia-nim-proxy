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
  res.json({
    status: 'ok',
    service: 'NIM Proxy'
  });
});

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    keyConfigured: !!NIM_API_KEY
  });
});

app.post('/v1/chat/completions', async (req, res) => {
  const requestId = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

  console.log('');
  console.log('=======================================================');
  console.log(`>>> REQUEST RECEIVED: ${requestId}`);

  if (!NIM_API_KEY) {
    return res.status(500).json({
      error: {
        message: 'NIM_API_KEY not set'
      }
    });
  }

  try {
    const body = { ...req.body };

    const modelName = (body.model || '').toLowerCase();
    const isKimi =
      modelName.includes('kimi-k3') ||
      modelName.includes('kimi_k3');

    console.log('Model:', body.model);

    /*
     * Keep the existing compatibility cleanup.
     */
    delete body.extra_body;
    delete body.logit_bias;
    delete body.presence_penalty;
    delete body.frequency_penalty;
    delete body.n;
    delete body.seed;

    /*
     * Kimi K3
     *
     * Force maximum reasoning effort and give the model
     * an explicit generation budget.
     */
    if (isKimi) {
      console.log('>>> KIMI K3 DETECTED');

      body.reasoning_effort = 'max';

      /*
       * IMPORTANT:
       * JanitorAI does not currently send a token limit.
       * Give K3 a large explicit budget so reasoning tokens
       * have room to be generated.
       */
      body.max_tokens = 16384;

      /*
       * K3 is recommended at temperature 1.0.
       * Only force this for Kimi.
       */
      body.temperature = 1.0;

      console.log('>>> KIMI SETTINGS FORCED');
      console.log('reasoning_effort:', body.reasoning_effort);
      console.log('temperature:', body.temperature);
      console.log('max_tokens:', body.max_tokens);
      console.log(
        'max_completion_tokens:',
        body.max_completion_tokens
      );
    }

    /*
     * Preserve the existing DeepSeek behavior.
     */
    if (modelName.includes('deepseek')) {
      body.reasoning_effort = 'high';
    }

    console.log('stream:', body.stream);
    console.log('messages:', Array.isArray(body.messages)
      ? body.messages.length
      : 0
    );

    if (Array.isArray(body.messages)) {
      const assistantMessages = body.messages.filter(
        m => m?.role === 'assistant'
      );

      const reasoningHistory = assistantMessages.filter(
        m =>
          typeof m?.reasoning_content === 'string' &&
          m.reasoning_content.trim().length > 0
      );

      console.log(
        'assistant messages:',
        assistantMessages.length
      );

      console.log(
        'assistant messages containing reasoning_content:',
        reasoningHistory.length
      );
    }

    const isStreaming = body.stream === true;

    console.log('>>> Sending request to NVIDIA');
    console.log('>>> Streaming:', isStreaming);

    const response = await axios({
      method: 'post',
      url: `${NIM_BASE}/chat/completions`,

      headers: {
        'Authorization': `Bearer ${NIM_API_KEY}`,
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0',
        ...(isStreaming
          ? { 'Accept': 'text/event-stream' }
          : {})
      },

      data: body,

      responseType: isStreaming
        ? 'stream'
        : 'json',

      timeout: 180000,

      validateStatus: () => true
    });

    console.log(
      '>>> NVIDIA STATUS:',
      response.status
    );

    /*
     * Handle NVIDIA errors.
     */
    if (response.status !== 200) {
      let errorMsg = 'Unknown error';

      try {
        if (typeof response.data === 'string') {
          errorMsg = response.data;
        } else if (response.data?.error?.message) {
          errorMsg = response.data.error.message;
        } else {
          errorMsg = JSON.stringify(response.data).slice(0, 1000);
        }
      } catch (e) {
        errorMsg =
          `NVIDIA returned status ${response.status}`;
      }

      console.error('>>> NVIDIA ERROR');
      console.error(errorMsg);

      return res.status(response.status).json({
        error: {
          message: errorMsg,
          type: 'upstream_error',
          code: response.status
        }
      });
    }

    /*
     * STREAMING
     *
     * Completely raw pass-through.
     *
     * We are deliberately NOT modifying reasoning_content.
     * This lets us determine exactly what NVIDIA sends.
     */
    if (isStreaming) {
      console.log('>>> NVIDIA STREAM CONNECTED');
      console.log('>>> RAW KIMI PASS-THROUGH ENABLED');

      res.setHeader(
        'Content-Type',
        'text/event-stream'
      );

      res.setHeader(
        'Cache-Control',
        'no-cache'
      );

      res.setHeader(
        'Connection',
        'keep-alive'
      );

      res.setHeader(
        'Access-Control-Allow-Origin',
        '*'
      );

      /*
       * Diagnostic counters.
       */
      let buffer = '';
      let chunkCount = 0;
      let reasoningChunks = 0;
      let contentChunks = 0;
      let firstChunkLogged = false;
      let firstReasoningLogged = false;

      response.data.on('data', chunk => {
        const text = chunk.toString();

        buffer += text;

        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
          if (!line.startsWith('data:')) {
            continue;
          }

          const payload = line
            .slice(5)
            .trim();

          if (!payload || payload === '[DONE]') {
            continue;
          }

          try {
            const parsed = JSON.parse(payload);

            chunkCount++;

            const delta =
              parsed?.choices?.[0]?.delta || {};

            const hasReasoning =
              typeof delta.reasoning_content === 'string' &&
              delta.reasoning_content.length > 0;

            const hasContent =
              typeof delta.content === 'string' &&
              delta.content.length > 0;

            if (hasReasoning) {
              reasoningChunks++;

              if (!firstReasoningLogged) {
                firstReasoningLogged = true;

                console.log('');
                console.log(
                  '>>> FIRST REASONING_CONTENT RECEIVED'
                );
                console.log(
                  'Request ID:',
                  requestId
                );
                console.log(
                  'Reasoning preview:',
                  delta.reasoning_content.slice(0, 300)
                );
              }
            }

            if (hasContent) {
              contentChunks++;
            }

            if (!firstChunkLogged) {
              firstChunkLogged = true;

              console.log('');
              console.log(
                '>>> FIRST RAW KIMI STREAM CHUNK'
              );
              console.log(
                'Request ID:',
                requestId
              );
              console.log(
                'Delta keys:',
                Object.keys(delta)
              );
              console.log(
                'Has reasoning_content:',
                hasReasoning
              );
              console.log(
                'Has content:',
                hasContent
              );
              console.log(
                'Full first chunk:',
                JSON.stringify(
                  parsed,
                  null,
                  2
                )
              );
            }

            const finishReason =
              parsed?.choices?.[0]?.finish_reason;

            if (finishReason) {
              console.log(
                '>>> FINISH REASON:',
                finishReason
              );
            }

            if (parsed?.usage) {
              console.log(
                '>>> USAGE:',
                JSON.stringify(parsed.usage)
              );
            }
          } catch (e) {
            /*
             * Ignore malformed/non-JSON SSE lines.
             */
          }
        }
      });

      response.data.on('end', () => {
        console.log('');
        console.log(
          '======================================================='
        );
        console.log(
          '>>> KIMI RAW STREAM SUMMARY'
        );
        console.log(
          'Request ID:',
          requestId
        );
        console.log(
          'Total parsed chunks:',
          chunkCount
        );
        console.log(
          'Reasoning chunks:',
          reasoningChunks
        );
        console.log(
          'Content chunks:',
          contentChunks
        );
        console.log(
          'Reasoning received:',
          reasoningChunks > 0
            ? 'YES'
            : 'NO'
        );
        console.log(
          '======================================================='
        );
      });

      response.data.on('error', err => {
        console.error(
          '>>> NVIDIA STREAM ERROR:',
          err.message
        );
      });

      /*
       * Raw NVIDIA stream goes directly to JanitorAI.
       */
      response.data.pipe(res);

      return;
    }

    /*
     * NON-STREAMING
     */
    console.log(
      '>>> NON-STREAM RESPONSE RECEIVED'
    );

    console.log(
      'Response has choices:',
      Array.isArray(response.data?.choices)
    );

    if (response.data?.choices?.[0]?.message) {
      const msg =
        response.data.choices[0].message;

      console.log(
        'Response message keys:',
        Object.keys(msg)
      );

      console.log(
        'Has reasoning_content:',
        typeof msg.reasoning_content === 'string' &&
        msg.reasoning_content.length > 0
      );
    }

    /*
     * Do NOT alter reasoning_content.
     */
    return res.json(response.data);

  } catch (err) {
    console.error('');
    console.error(
      '>>> PROXY ERROR:',
      err.message
    );

    return res.status(500).json({
      error: {
        message:
          err.message ||
          'Internal proxy error',
        type: 'proxy_error',
        code: 500
      }
    });
  }
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(
    `NIM Proxy running on port ${PORT}`
  );
});
