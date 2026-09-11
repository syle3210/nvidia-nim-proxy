import express from 'express';
import cors from 'cors';
import axios from 'axios';

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json({ limit: '50mb' }));

const NIM_API_KEY =
  process.env.NIM_API_KEY ||
  process.env.NVIDIA_API_KEY;

const NIM_BASE =
  process.env.NIM_API_BASE ||
  'https://integrate.api.nvidia.com/v1';


// ============================================================
// BASIC ROUTES
// ============================================================

app.get('/', (req, res) => {
  res.json({
    status: 'ok',
    service: 'NVIDIA NIM Proxy'
  });
});

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    keyConfigured: !!NIM_API_KEY
  });
});


// ============================================================
// KIMI STREAM TRANSFORMER
//
// NVIDIA sends:
//   delta.reasoning_content
//
// We convert it into:
//   delta.content = "<think>...</think>"
//
// This makes the reasoning visible to clients that understand
// <think> tags but do not reliably render reasoning_content.
// ============================================================

function createKimiStreamTransformer(inputStream, res, requestId) {
  let buffer = '';

  let reasoningStarted = false;
  let reasoningEnded = false;

  let reasoningChunks = 0;
  let contentChunks = 0;
  let totalChunks = 0;

  let reasoningChars = 0;
  let contentChars = 0;

  let firstChunkLogged = false;

  const sendSSE = (obj) => {
    res.write(`data: ${JSON.stringify(obj)}\n\n`);
  };

  const processLine = (line) => {
    const trimmed = line.trim();

    if (!trimmed) {
      return;
    }

    if (!trimmed.startsWith('data:')) {
      return;
    }

    const raw = trimmed.slice(5).trim();

    if (!raw) {
      return;
    }

    if (raw === '[DONE]') {
      console.log('');
      console.log('================ KIMI STREAM SUMMARY ================');
      console.log('Request ID:', requestId);
      console.log('Total chunks:', totalChunks);
      console.log('Reasoning chunks:', reasoningChunks);
      console.log('Content chunks:', contentChunks);
      console.log('Reasoning characters:', reasoningChars);
      console.log('Content characters:', contentChars);
      console.log(
        'Reasoning received:',
        reasoningChars > 0 ? 'YES' : 'NO'
      );
      console.log('=======================================================');
      console.log('');

      res.write('data: [DONE]\n\n');
      return;
    }

    let parsed;

    try {
      parsed = JSON.parse(raw);
    } catch {
      return;
    }

    totalChunks++;

    const choice = parsed?.choices?.[0];
    const delta = choice?.delta;

    if (!delta) {
      sendSSE(parsed);
      return;
    }

    if (!firstChunkLogged) {
      firstChunkLogged = true;

      console.log('');
      console.log('================ KIMI FIRST CHUNK ================');
      console.log('Request ID:', requestId);
      console.log('Delta keys:', Object.keys(delta));
      console.log(
        'Has reasoning_content:',
        typeof delta.reasoning_content === 'string' &&
          delta.reasoning_content.length > 0
      );
      console.log(
        'Has content:',
        typeof delta.content === 'string' &&
          delta.content.length > 0
      );
      console.log('====================================================');
      console.log('');
    }


    // ========================================================
    // REASONING CONTENT
    // ========================================================

    if (
      typeof delta.reasoning_content === 'string' &&
      delta.reasoning_content.length > 0
    ) {
      const reasoningText = delta.reasoning_content;

      reasoningChunks++;
      reasoningChars += reasoningText.length;

      // First reasoning chunk opens <think>
      if (!reasoningStarted) {
        reasoningStarted = true;

        sendSSE({
          ...parsed,
          choices: [
            {
              ...choice,
              delta: {
                ...delta,
                reasoning_content: undefined,
                content: '<think>\n' + reasoningText
              }
            }
          ]
        });
      } else {
        sendSSE({
          ...parsed,
          choices: [
            {
              ...choice,
              delta: {
                ...delta,
                reasoning_content: undefined,
                content: reasoningText
              }
            }
          ]
        });
      }

      return;
    }


    // ========================================================
    // NORMAL CONTENT
    // ========================================================

    if (
      typeof delta.content === 'string' &&
      delta.content.length > 0
    ) {
      const contentText = delta.content;

      contentChunks++;
      contentChars += contentText.length;

      // If reasoning was active, close <think> first.
      if (reasoningStarted && !reasoningEnded) {
        reasoningEnded = true;

        sendSSE({
          ...parsed,
          choices: [
            {
              ...choice,
              delta: {
                ...delta,
                content: '</think>\n\n' + contentText
              }
            }
          ]
        });
      } else {
        sendSSE(parsed);
      }

      return;
    }


    // ========================================================
    // OTHER CHUNKS
    // role, finish_reason, usage, etc.
    // ========================================================

    sendSSE(parsed);
  };


  inputStream.on('data', (chunk) => {
    buffer += chunk.toString();

    const lines = buffer.split('\n');

    buffer = lines.pop() || '';

    for (const line of lines) {
      processLine(line);
    }
  });


  inputStream.on('end', () => {
    if (buffer.trim()) {
      processLine(buffer);
    }

    // Safety close if NVIDIA ended the stream without
    // sending normal content after reasoning.
    if (reasoningStarted && !reasoningEnded) {
      res.write(
        `data: ${JSON.stringify({
          choices: [
            {
              delta: {
                content: '</think>\n\n'
              }
            }
          ]
        })}\n\n`
      );

      reasoningEnded = true;
    }

    console.log('');
    console.log('>>> KIMI INPUT STREAM ENDED');
    console.log('Request ID:', requestId);
    console.log('');
  });


  inputStream.on('error', (err) => {
    console.error('');
    console.error('>>> KIMI INPUT STREAM ERROR');
    console.error('Request ID:', requestId);
    console.error('Error:', err.message);
    console.error('');
  });
}


// ============================================================
// CHAT COMPLETIONS
// ============================================================

app.post('/v1/chat/completions', async (req, res) => {
  const requestId =
    `${Date.now()}-${Math.random()
      .toString(36)
      .slice(2, 8)}`;

  console.log('');
  console.log('=======================================================');
  console.log('>>> REQUEST RECEIVED');
  console.log('Request ID:', requestId);
  console.log('Model:', req.body?.model);
  console.log('=======================================================');

  if (!NIM_API_KEY) {
    return res.status(500).json({
      error: {
        message: 'NIM_API_KEY not set'
      }
    });
  }

  try {
    const body = { ...req.body };

    // Remove fields NVIDIA may reject.
    delete body.extra_body;
    delete body.logit_bias;
    delete body.presence_penalty;
    delete body.frequency_penalty;
    delete body.n;
    delete body.seed;

    const modelName =
      (body.model || '').toLowerCase();

    const isKimi =
      modelName.includes('kimi-k3') ||
      modelName.includes('kimi_k3');

    const isDeepSeek =
      modelName.includes('deepseek');


    // --------------------------------------------------------
    // Other thinking models
    // --------------------------------------------------------

    if (
      modelName.includes('gemma') ||
      modelName.includes('minimax')
    ) {
      body.chat_template_kwargs = {
        enable_thinking: true
      };
    }


    // --------------------------------------------------------
    // KIMI K3
    // --------------------------------------------------------

    if (isKimi) {
      body.reasoning_effort = 'max';

      console.log('');
      console.log('>>> KIMI K3 DETECTED');
      console.log('>>> reasoning_effort:', body.reasoning_effort);
      console.log('>>> stream:', body.stream);
      console.log('>>> temperature:', body.temperature);
      console.log('>>> max_tokens:', body.max_tokens);
      console.log(
        '>>> max_completion_tokens:',
        body.max_completion_tokens
      );

      if (Array.isArray(body.messages)) {
        const assistantMessages =
          body.messages.filter(
            m => m?.role === 'assistant'
          );

        const withReasoning =
          assistantMessages.filter(
            m =>
              typeof m?.reasoning_content === 'string' &&
              m.reasoning_content.length > 0
          );

        console.log(
          '>>> messages:',
          body.messages.length
        );

        console.log(
          '>>> assistant messages:',
          assistantMessages.length
        );

        console.log(
          '>>> assistant messages containing reasoning_content:',
          withReasoning.length
        );
      }
    }


    // --------------------------------------------------------
    // DeepSeek
    // --------------------------------------------------------

    if (isDeepSeek) {
      body.reasoning_effort = 'high';

      console.log('');
      console.log('>>> DEEPSEEK DETECTED');
      console.log(
        '>>> reasoning_effort:',
        body.reasoning_effort
      );
    }


    const isStreaming =
      body.stream === true;


    // --------------------------------------------------------
    // SEND TO NVIDIA
    // --------------------------------------------------------

    const response = await axios({
      method: 'post',

      url:
        `${NIM_BASE}/chat/completions`,

      headers: {
        'Authorization':
          `Bearer ${NIM_API_KEY}`,

        'Content-Type':
          'application/json',

        'User-Agent':
          'Mozilla/5.0',

        'Accept':
          isStreaming
            ? 'text/event-stream'
            : 'application/json'
      },

      data: body,

      responseType:
        isStreaming
          ? 'stream'
          : 'json',

      timeout: 180000,

      validateStatus:
        () => true
    });


    // --------------------------------------------------------
    // NVIDIA ERROR
    // --------------------------------------------------------

    if (response.status !== 200) {
      let errorMsg =
        'Unknown NVIDIA error';

      try {
        if (
          typeof response.data === 'string'
        ) {
          errorMsg =
            response.data;
        } else if (
          response.data?.error?.message
        ) {
          errorMsg =
            response.data.error.message;
        } else {
          errorMsg =
            JSON.stringify(
              response.data
            ).slice(0, 1000);
        }
      } catch {
        errorMsg =
          `NVIDIA returned status ${response.status}`;
      }

      console.error('');
      console.error('>>> NVIDIA ERROR');
      console.error('Status:', response.status);
      console.error('Error:', errorMsg);
      console.error('');

      return res.status(response.status).json({
        error: {
          message: errorMsg,
          type: 'upstream_error',
          code: response.status
        }
      });
    }


    // --------------------------------------------------------
    // STREAMING
    // --------------------------------------------------------

    if (isStreaming) {
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

      console.log('');
      console.log('>>> NVIDIA STREAM CONNECTED');
      console.log(
        '>>> Request ID:',
        requestId
      );

      if (isKimi) {
        console.log(
          '>>> KIMI STREAM TRANSFORMATION ENABLED'
        );

        createKimiStreamTransformer(
          response.data,
          res,
          requestId
        );
      } else {
        response.data.pipe(res);
      }

      return;
    }


    // --------------------------------------------------------
    // NON-STREAMING
    // --------------------------------------------------------

    const data = response.data;

    if (
      isKimi &&
      data?.choices?.[0]?.message
    ) {
      const msg =
        data.choices[0].message;

      const reasoning =
        msg.reasoning_content ||
        msg.reasoning ||
        '';

      console.log('');
      console.log(
        '>>> KIMI NON-STREAM REASONING:',
        reasoning
          ? 'RECEIVED'
          : 'NOT RECEIVED'
      );

      if (
        reasoning &&
        reasoning.trim().length > 0
      ) {
        msg.content =
          `<think>\n${reasoning.trim()}\n</think>\n\n${msg.content || ''}`;
      }
    }

    res.json(data);

  } catch (err) {
    console.error('');
    console.error('=======================================================');
    console.error('>>> PROXY ERROR');
    console.error('Request ID:', requestId);
    console.error('Error:', err.message);
    console.error('=======================================================');
    console.error('');

    res.status(500).json({
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


// ============================================================
// START SERVER
// ============================================================

app.listen(
  PORT,
  '0.0.0.0',
  () => {
    console.log('');
    console.log('=======================================================');
    console.log('NVIDIA NIM Proxy running');
    console.log('Port:', PORT);
    console.log('NIM Base:', NIM_BASE);
    console.log(
      'NIM API Key configured:',
      !!NIM_API_KEY
    );
    console.log('=======================================================');
    console.log('');
  }
);
