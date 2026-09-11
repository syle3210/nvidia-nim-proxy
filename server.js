import express from 'express';
import cors from 'cors';
import axios from 'axios';

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json({ limit: '50mb' }));

const NIM_API_KEY = process.env.NIM_API_KEY || process.env.NVIDIA_API_KEY;
const NIM_BASE = process.env.NIM_API_BASE || 'https://integrate.api.nvidia.com/v1';


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
// SSE DEBUG MONITOR
// Does NOT modify the Kimi stream.
// It only observes what NVIDIA actually sends back.
// ============================================================

function monitorKimiStream(stream, requestId) {
  let buffer = '';
  let chunkCount = 0;
  let reasoningChunks = 0;
  let contentChunks = 0;
  let firstChunkLogged = false;
  let reasoningPreview = '';
  let contentPreview = '';

  const processLine = (line) => {
    line = line.trim();

    if (!line.startsWith('data:')) {
      return;
    }

    const data = line.slice(5).trim();

    if (!data || data === '[DONE]') {
      return;
    }

    try {
      const parsed = JSON.parse(data);
      chunkCount++;

      const choice = parsed?.choices?.[0];
      const delta = choice?.delta || {};

      const hasReasoning =
        typeof delta.reasoning_content === 'string' &&
        delta.reasoning_content.length > 0;

      const hasReasoningAlt =
        typeof delta.reasoning === 'string' &&
        delta.reasoning.length > 0;

      const hasContent =
        typeof delta.content === 'string' &&
        delta.content.length > 0;

      if (!firstChunkLogged) {
        firstChunkLogged = true;

        console.log('');
        console.log('================ KIMI FIRST STREAM CHUNK ================');
        console.log('Request ID:', requestId);
        console.log('Delta keys:', Object.keys(delta));
        console.log('Has reasoning_content:', hasReasoning);
        console.log('Has reasoning:', hasReasoningAlt);
        console.log('Has content:', hasContent);
        console.log('Finish reason:', choice?.finish_reason);
        console.log('==========================================================');
        console.log('');
      }

      if (hasReasoning || hasReasoningAlt) {
        reasoningChunks++;

        const reasoningText =
          delta.reasoning_content || delta.reasoning || '';

        if (reasoningPreview.length < 500) {
          reasoningPreview += reasoningText;
          reasoningPreview = reasoningPreview.slice(0, 500);
        }
      }

      if (hasContent) {
        contentChunks++;

        if (contentPreview.length < 300) {
          contentPreview += delta.content;
          contentPreview = contentPreview.slice(0, 300);
        }
      }

      if (
        hasContent &&
        delta.content &&
        (
          delta.content.includes('<think>') ||
          delta.content.includes('</think>')
        )
      ) {
        console.log('>>> KIMI <think> TAG FOUND INSIDE CONTENT');
      }

      if (choice?.finish_reason) {
        console.log('');
        console.log('================ KIMI STREAM FINISHED ================');
        console.log('Request ID:', requestId);
        console.log('Finish reason:', choice.finish_reason);
        console.log('Total chunks:', chunkCount);
        console.log('Reasoning chunks:', reasoningChunks);
        console.log('Content chunks:', contentChunks);
        console.log(
          'Reasoning received:',
          reasoningChunks > 0 ? 'YES' : 'NO'
        );
        console.log(
          'Reasoning preview:',
          reasoningPreview || '[none]'
        );
        console.log(
          'Content preview:',
          contentPreview || '[none]'
        );
        console.log('=======================================================');
        console.log('');
      }

    } catch (err) {
      // Ignore incomplete/non-JSON SSE lines.
    }
  };

  stream.on('data', (chunk) => {
    buffer += chunk.toString();

    const lines = buffer.split('\n');

    // Keep the last potentially incomplete line.
    buffer = lines.pop() || '';

    for (const line of lines) {
      processLine(line);
    }
  });

  stream.on('end', () => {
    // Process anything left in the buffer.
    if (buffer.trim()) {
      processLine(buffer);
    }

    console.log('');
    console.log('>>> KIMI RAW STREAM ENDED');
    console.log('Request ID:', requestId);
    console.log('=======================================================');
    console.log('');
  });

  stream.on('error', (err) => {
    console.error('');
    console.error('>>> KIMI STREAM ERROR');
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
    `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

  console.log('');
  console.log('=======================================================');
  console.log('>>> REQUEST RECEIVED');
  console.log('Request ID:', requestId);
  console.log('Model:', req.body?.model);
  console.log('=======================================================');

  if (!NIM_API_KEY) {
    console.error('>>> NIM_API_KEY IS NOT CONFIGURED');

    return res.status(500).json({
      error: {
        message: 'NIM_API_KEY not set'
      }
    });
  }

  try {
    const body = { ...req.body };

    // --------------------------------------------------------
    // Remove fields that NVIDIA NIM may reject.
    // --------------------------------------------------------

    delete body.extra_body;
    delete body.logit_bias;
    delete body.presence_penalty;
    delete body.frequency_penalty;
    delete body.n;
    delete body.seed;


    // --------------------------------------------------------
    // Detect model.
    // --------------------------------------------------------

    const modelName = (body.model || '').toLowerCase();

    const isKimi =
      modelName.includes('kimi-k3') ||
      modelName.includes('kimi_k3');

    const isDeepSeek =
      modelName.includes('deepseek');


    // --------------------------------------------------------
    // Existing thinking-mode models.
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
    // K3 always has thinking enabled.
    // We force MAX reasoning effort.
    //
    // IMPORTANT:
    // We intentionally DO NOT modify:
    //   - stream
    //   - temperature
    //   - max_tokens
    //   - max_completion_tokens
    //
    // JanitorAI controls those.
    // --------------------------------------------------------

    if (isKimi) {
      body.reasoning_effort = 'max';

      console.log('');
      console.log('>>> KIMI K3 DETECTED');
      console.log('>>> reasoning_effort: max');

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
      // Check whether JanitorAI is actually sending previous
      // Kimi reasoning back in the conversation.
      // ------------------------------------------------------

      if (Array.isArray(body.messages)) {
        const assistantMessages = body.messages.filter(
          (message) => message?.role === 'assistant'
        );

        const assistantWithReasoning =
          assistantMessages.filter(
            (message) =>
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
          assistantWithReasoning.length
        );

        if (assistantMessages.length > 0) {
          const lastAssistant =
            assistantMessages[assistantMessages.length - 1];

          console.log(
            '>>> last assistant has reasoning_content:',
            typeof lastAssistant?.reasoning_content === 'string' &&
            lastAssistant.reasoning_content.length > 0
          );
        }
      }
    }


    // --------------------------------------------------------
    // DeepSeek
    // --------------------------------------------------------

    if (isDeepSeek) {
      body.reasoning_effort = 'high';

      console.log('');
      console.log('>>> DEEPSEEK DETECTED');
      console.log('>>> reasoning_effort: high');
    }


    // --------------------------------------------------------
    // Determine streaming mode.
    // --------------------------------------------------------

    const isStreaming = body.stream === true;

    console.log('');
    console.log('>>> FINAL REQUEST SETTINGS');
    console.log('>>> Model:', body.model);
    console.log('>>> Stream:', isStreaming);
    console.log('>>> Temperature:', body.temperature);
    console.log('>>> max_tokens:', body.max_tokens);
    console.log(
      '>>> max_completion_tokens:',
      body.max_completion_tokens
    );
    console.log(
      '>>> reasoning_effort:',
      body.reasoning_effort
    );
    console.log('=======================================================');
    console.log('');


    // --------------------------------------------------------
    // Send request to NVIDIA.
    // --------------------------------------------------------

    const response = await axios({
      method: 'post',
      url: `${NIM_BASE}/chat/completions`,

      headers: {
        'Authorization': `Bearer ${NIM_API_KEY}`,
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0',

        ...(isStreaming
          ? {
              'Accept': 'text/event-stream'
            }
          : {
              'Accept': 'application/json'
            })
      },

      data: body,

      responseType: isStreaming
        ? 'stream'
        : 'json',

      timeout: 180000,

      validateStatus: () => true
    });


    // --------------------------------------------------------
    // NVIDIA returned an error.
    // --------------------------------------------------------

    if (response.status !== 200) {
      let errorMsg = 'Unknown NVIDIA error';

      try {
        if (typeof response.data === 'string') {
          errorMsg = response.data;
        } else if (
          response.data?.error?.message
        ) {
          errorMsg =
            response.data.error.message;
        } else {
          errorMsg =
            JSON.stringify(response.data)
              .slice(0, 1000);
        }
      } catch (err) {
        errorMsg =
          `NVIDIA returned status ${response.status}`;
      }

      console.error('');
      console.error('>>> NVIDIA ERROR');
      console.error('Status:', response.status);
      console.error('Request ID:', requestId);
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
    // STREAMING RESPONSE
    // --------------------------------------------------------

    if (isStreaming) {
      console.log('');
      console.log('>>> NVIDIA STREAM CONNECTED');
      console.log('>>> Request ID:', requestId);
      console.log('>>> Monitoring Kimi stream without modifying it');
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

      // Monitor what NVIDIA sends.
      // This does NOT alter the stream.
      if (isKimi) {
        monitorKimiStream(
          response.data,
          requestId
        );
      }

      // Pass the original NVIDIA stream directly
      // to JanitorAI.
      response.data.pipe(res);

      return;
    }


    // --------------------------------------------------------
    // NON-STREAMING RESPONSE
    // --------------------------------------------------------

    const data = response.data;

    console.log('');
    console.log('>>> NVIDIA NON-STREAM RESPONSE');
    console.log('>>> Request ID:', requestId);

    if (isKimi) {
      const msg =
        data?.choices?.[0]?.message;

      if (msg) {
        const reasoning =
          msg.reasoning_content ||
          msg.reasoning ||
          '';

        console.log(
          '>>> Kimi reasoning_content:',
          reasoning
            ? 'RECEIVED'
            : 'NOT RECEIVED'
        );

        console.log(
          '>>> Kimi content:',
          msg.content
            ? 'RECEIVED'
            : 'EMPTY'
        );

        console.log(
          '>>> finish_reason:',
          data?.choices?.[0]?.finish_reason
        );

        // Keep the existing behavior for non-streaming
        // responses so JanitorAI can see reasoning if
        // NVIDIA returns it.
        if (
          reasoning &&
          reasoning.trim().length > 0
        ) {
          msg.content =
            `<think>\n${reasoning.trim()}\n</think>\n\n${msg.content || ''}`;
        }
      }
    }

    console.log('');

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
