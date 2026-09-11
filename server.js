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
// KIMI RAW STREAM DIAGNOSTIC
//
// IMPORTANT:
// This function DOES NOT modify the stream.
// It only observes what NVIDIA sends.
//
// The original NVIDIA SSE stream is still piped directly
// to JanitorAI.
// ============================================================

function monitorKimiStream(stream, requestId) {
  let buffer = '';

  let totalChunks = 0;
  let reasoningChunks = 0;
  let contentChunks = 0;

  let reasoningChars = 0;
  let contentChars = 0;

  let firstChunk = true;
  let lastChunk = null;

  let reasoningPreview = '';
  let contentPreview = '';

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

    // --------------------------------------------------------
    // NVIDIA STREAM END
    // --------------------------------------------------------

    if (raw === '[DONE]') {
      console.log('');
      console.log('=======================================================');
      console.log('>>> KIMI RAW STREAM SUMMARY');
      console.log('Request ID:', requestId);
      console.log('Total chunks:', totalChunks);
      console.log('Reasoning chunks:', reasoningChunks);
      console.log('Content chunks:', contentChunks);
      console.log(
        'Reasoning characters:',
        reasoningChars
      );
      console.log(
        'Content characters:',
        contentChars
      );

      console.log(
        'Reasoning received:',
        reasoningChars > 0 ? 'YES' : 'NO'
      );

      console.log(
        'Reasoning preview:',
        reasoningPreview || '[none]'
      );

      console.log(
        'Content preview:',
        contentPreview || '[none]'
      );

      if (lastChunk) {
        console.log('');
        console.log('>>> LAST KIMI CHUNK');
        console.log(
          JSON.stringify(lastChunk, null, 2)
        );
      }

      console.log('=======================================================');
      console.log('');

      return;
    }


    let parsed;

    try {
      parsed = JSON.parse(raw);
    } catch {
      console.log(
        '>>> Could not parse SSE JSON line'
      );
      return;
    }

    totalChunks++;

    lastChunk = parsed;

    const choice =
      parsed?.choices?.[0];

    const delta =
      choice?.delta || {};


    // --------------------------------------------------------
    // FIRST CHUNK
    // --------------------------------------------------------

    if (firstChunk) {
      firstChunk = false;

      console.log('');
      console.log('=======================================================');
      console.log('>>> KIMI FIRST RAW STREAM CHUNK');
      console.log('Request ID:', requestId);

      console.log(
        'Delta keys:',
        Object.keys(delta)
      );

      console.log(
        'Has reasoning_content:',
        typeof delta.reasoning_content === 'string' &&
        delta.reasoning_content.length > 0
      );

      console.log(
        'Has reasoning:',
        typeof delta.reasoning === 'string' &&
        delta.reasoning.length > 0
      );

      console.log(
        'Has content:',
        typeof delta.content === 'string' &&
        delta.content.length > 0
      );

      console.log(
        'Role:',
        delta.role
      );

      console.log(
        'Finish reason:',
        choice?.finish_reason
      );

      console.log(
        'Full first chunk:',
        JSON.stringify(parsed, null, 2)
      );

      console.log('=======================================================');
      console.log('');
    }


    // --------------------------------------------------------
    // REASONING
    // --------------------------------------------------------

    if (
      typeof delta.reasoning_content === 'string' &&
      delta.reasoning_content.length > 0
    ) {
      reasoningChunks++;

      reasoningChars +=
        delta.reasoning_content.length;

      if (reasoningPreview.length < 1000) {
        reasoningPreview +=
          delta.reasoning_content;

        reasoningPreview =
          reasoningPreview.slice(0, 1000);
      }

      // Only log the FIRST reasoning chunk.
      if (reasoningChunks === 1) {
        console.log('');
        console.log(
          '>>> FIRST REASONING_CONTENT CHUNK RECEIVED'
        );

        console.log(
          'Request ID:',
          requestId
        );

        console.log(
          'Reasoning text:',
          delta.reasoning_content
        );

        console.log('');
      }
    }


    // --------------------------------------------------------
    // ALTERNATIVE REASONING FIELD
    // --------------------------------------------------------

    if (
      typeof delta.reasoning === 'string' &&
      delta.reasoning.length > 0
    ) {
      console.log('');
      console.log(
        '>>> ALTERNATIVE "reasoning" FIELD FOUND'
      );

      console.log(
        'Request ID:',
        requestId
      );

      console.log(
        'Reasoning:',
        delta.reasoning.slice(0, 1000)
      );

      console.log('');
    }


    // --------------------------------------------------------
    // NORMAL CONTENT
    // --------------------------------------------------------

    if (
      typeof delta.content === 'string' &&
      delta.content.length > 0
    ) {
      contentChunks++;

      contentChars +=
        delta.content.length;

      if (contentPreview.length < 500) {
        contentPreview +=
          delta.content;

        contentPreview =
          contentPreview.slice(0, 500);
      }
    }


    // --------------------------------------------------------
    // FINISH REASON
    // --------------------------------------------------------

    if (choice?.finish_reason) {
      console.log('');
      console.log(
        '>>> KIMI FINISH REASON'
      );

      console.log(
        'Request ID:',
        requestId
      );

      console.log(
        'Finish reason:',
        choice.finish_reason
      );

      console.log('');
    }


    // --------------------------------------------------------
    // USAGE
    // --------------------------------------------------------

    if (parsed?.usage) {
      console.log('');
      console.log(
        '>>> KIMI USAGE OBJECT'
      );

      console.log(
        JSON.stringify(
          parsed.usage,
          null,
          2
        )
      );

      console.log('');
    }
  };


  // ==========================================================
  // READ NVIDIA SSE
  // ==========================================================

  stream.on('data', (chunk) => {
    buffer += chunk.toString();

    const lines =
      buffer.split('\n');

    buffer =
      lines.pop() || '';

    for (const line of lines) {
      processLine(line);
    }
  });


  // ==========================================================
  // END
  // ==========================================================

  stream.on('end', () => {
    if (buffer.trim()) {
      processLine(buffer);
    }

    console.log('');
    console.log(
      '>>> KIMI RAW INPUT STREAM ENDED'
    );

    console.log(
      'Request ID:',
      requestId
    );

    console.log('');
  });


  // ==========================================================
  // ERROR
  // ==========================================================

  stream.on('error', (err) => {
    console.error('');
    console.error(
      '>>> KIMI RAW STREAM ERROR'
    );

    console.error(
      'Request ID:',
      requestId
    );

    console.error(
      'Error:',
      err.message
    );

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
    console.error(
      '>>> NIM_API_KEY IS NOT CONFIGURED'
    );

    return res.status(500).json({
      error: {
        message: 'NIM_API_KEY not set'
      }
    });
  }


  try {
    const body = {
      ...req.body
    };


    // --------------------------------------------------------
    // Remove unsupported fields.
    // --------------------------------------------------------

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
    //
    // ONLY force reasoning_effort.
    //
    // stream, temperature and token limits remain under
    // JanitorAI's control.
    // --------------------------------------------------------

    if (isKimi) {
      body.reasoning_effort = 'max';

      console.log('');
      console.log('>>> KIMI K3 DETECTED');

      console.log(
        '>>> reasoning_effort:',
        body.reasoning_effort
      );

      console.log(
        '>>> stream:',
        body.stream
      );

      console.log(
        '>>> temperature:',
        body.temperature
      );

      console.log(
        '>>> max_tokens:',
        body.max_tokens
      );

      console.log(
        '>>> max_completion_tokens:',
        body.max_completion_tokens
      );

      console.log(
        '>>> stream_options:',
        body.stream_options
      );

      console.log(
        '>>> stop:',
        body.stop
      );


      // ------------------------------------------------------
      // MESSAGE DIAGNOSTICS
      // ------------------------------------------------------

      if (Array.isArray(body.messages)) {
        const assistantMessages =
          body.messages.filter(
            message =>
              message?.role === 'assistant'
          );

        const reasoningMessages =
          assistantMessages.filter(
            message =>
              typeof message?.reasoning_content === 'string' &&
              message.reasoning_content.length > 0
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
          reasoningMessages.length
        );

        if (assistantMessages.length > 0) {
          const lastAssistant =
            assistantMessages[
              assistantMessages.length - 1
            ];

          console.log(
            '>>> last assistant has reasoning_content:',
            typeof lastAssistant?.reasoning_content === 'string' &&
            lastAssistant.reasoning_content.length > 0
          );
        }
      }
    }


    // --------------------------------------------------------
    // DEEPSEEK
    // --------------------------------------------------------

    if (isDeepSeek) {
      body.reasoning_effort = 'high';

      console.log('');
      console.log(
        '>>> DEEPSEEK DETECTED'
      );

      console.log(
        '>>> reasoning_effort:',
        body.reasoning_effort
      );
    }


    const isStreaming =
      body.stream === true;


    // --------------------------------------------------------
    // FINAL REQUEST LOG
    // --------------------------------------------------------

    console.log('');
    console.log(
      '>>> FINAL REQUEST'
    );

    console.log(
      '>>> Model:',
      body.model
    );

    console.log(
      '>>> Stream:',
      isStreaming
    );

    console.log(
      '>>> Temperature:',
      body.temperature
    );

    console.log(
      '>>> max_tokens:',
      body.max_tokens
    );

    console.log(
      '>>> max_completion_tokens:',
      body.max_completion_tokens
    );

    console.log(
      '>>> reasoning_effort:',
      body.reasoning_effort
    );

    console.log(
      '>>> Request ID:',
      requestId
    );

    console.log(
      '======================================================='
    );

    console.log('');


    // ========================================================
    // NVIDIA REQUEST
    // ========================================================

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


    // ========================================================
    // NVIDIA ERROR
    // ========================================================

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
            ).slice(0, 1500);
        }
      } catch {
        errorMsg =
          `NVIDIA returned status ${response.status}`;
      }

      console.error('');
      console.error(
        '>>> NVIDIA ERROR'
      );

      console.error(
        'Status:',
        response.status
      );

      console.error(
        'Request ID:',
        requestId
      );

      console.error(
        'Error:',
        errorMsg
      );

      console.error('');

      return res.status(
        response.status
      ).json({
        error: {
          message: errorMsg,
          type: 'upstream_error',
          code: response.status
        }
      });
    }


    // ========================================================
    // RAW STREAM PASS-THROUGH
    //
    // NO TRANSFORMATION.
    // ========================================================

    if (isStreaming) {
      console.log('');
      console.log(
        '>>> NVIDIA STREAM CONNECTED'
      );

      console.log(
        '>>> Request ID:',
        requestId
      );

      console.log(
        '>>> KIMI RAW PASS-THROUGH:',
        isKimi
      );

      console.log('');

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


      // Monitor only.
      //
      // IMPORTANT:
      // This does NOT alter response.data.
      if (isKimi) {
        monitorKimiStream(
          response.data,
          requestId
        );
      }


      // Original NVIDIA stream goes directly
      // to JanitorAI.
      response.data.pipe(res);

      return;
    }


    // ========================================================
    // NON-STREAM RESPONSE
    // ========================================================

    const data =
      response.data;


    if (
      isKimi &&
      data?.choices?.[0]?.message
    ) {
      const message =
        data.choices[0].message;

      const reasoning =
        message.reasoning_content ||
        message.reasoning ||
        '';

      console.log('');
      console.log(
        '>>> KIMI NON-STREAM RESPONSE'
      );

      console.log(
        '>>> reasoning_content:',
        reasoning
          ? 'RECEIVED'
          : 'NOT RECEIVED'
      );

      console.log(
        '>>> content:',
        message.content
          ? 'RECEIVED'
          : 'EMPTY'
      );

      console.log(
        '>>> finish_reason:',
        data?.choices?.[0]?.finish_reason
      );

      console.log('');


      // Preserve previous behavior for
      // non-streaming Kimi responses.
      if (
        reasoning &&
        reasoning.trim().length > 0
      ) {
        message.content =
          `<think>\n${reasoning.trim()}\n</think>\n\n${message.content || ''}`;
      }
    }


    res.json(data);

  } catch (err) {
    console.error('');
    console.error(
      '======================================================='
    );

    console.error(
      '>>> PROXY ERROR'
    );

    console.error(
      'Request ID:',
      requestId
    );

    console.error(
      'Error:',
      err.message
    );

    console.error(
      '======================================================='
    );

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
    console.log(
      '======================================================='
    );

    console.log(
      'NVIDIA NIM Proxy running'
    );

    console.log(
      'Port:',
      PORT
    );

    console.log(
      'NIM Base:',
      NIM_BASE
    );

    console.log(
      'NIM API Key configured:',
      !!NIM_API_KEY
    );

    console.log(
      '======================================================='
    );

    console.log('');
  }
);
