import express from 'express';
import cors from 'cors';
import axios from 'axios';

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json({ limit: '50mb' }));

const NIM_API_KEY =
  process.env.NIM_API_KEY || process.env.NVIDIA_API_KEY;

const NIM_BASE =
  'https://integrate.api.nvidia.com/v1';


// ============================================================
// BASIC ROUTES
// ============================================================

app.get('/', (req, res) => {
  res.json({
    status: 'ok',
    service: 'NVIDIA NIM Proxy - Kimi/DeepSeek Reasoning'
  });
});

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    keyConfigured: !!NIM_API_KEY
  });
});


// ============================================================
// SSE REASONING STREAM HANDLER
// ============================================================

function handleStreamingResponse(response, res) {
  let buffer = '';

  let thinkingStarted = false;
  let thinkingEnded = false;


  function sendJSON(data) {
    res.write(`data: ${JSON.stringify(data)}\n\n`);
  }


  function sendThinkStart(original) {
    const output = {
      ...original,
      choices: original.choices.map(choice => ({
        ...choice,
        delta: {
          ...choice.delta,
          reasoning_content: undefined,
          reasoning: undefined,
          content: '<think>\n'
        }
      }))
    };

    sendJSON(output);
  }


  function sendReasoning(original, reasoning) {
    const output = {
      ...original,
      choices: original.choices.map(choice => ({
        ...choice,
        delta: {
          ...choice.delta,
          reasoning_content: undefined,
          reasoning: undefined,
          content: reasoning
        }
      }))
    };

    sendJSON(output);
  }


  function sendThinkEnd(original) {
    const output = {
      ...original,
      choices: original.choices.map(choice => ({
        ...choice,
        delta: {
          ...choice.delta,
          reasoning_content: undefined,
          reasoning: undefined,
          content: '\n</think>\n\n'
        }
      }))
    };

    sendJSON(output);
  }


  function sendNormalContent(original, content) {
    const output = {
      ...original,
      choices: original.choices.map(choice => ({
        ...choice,
        delta: {
          ...choice.delta,
          reasoning_content: undefined,
          reasoning: undefined,
          content: content
        }
      }))
    };

    sendJSON(output);
  }


  function processLine(line) {
    const trimmed = line.trim();

    if (!trimmed) {
      return;
    }


    // NVIDIA's final SSE marker
    if (trimmed === 'data: [DONE]') {
      res.write('data: [DONE]\n\n');
      return;
    }


    // Ignore non-data SSE lines
    if (!trimmed.startsWith('data:')) {
      return;
    }


    const raw = trimmed.slice(5).trim();

    if (!raw) {
      return;
    }


    let data;

    try {
      data = JSON.parse(raw);
    } catch (error) {
      console.error(
        'Could not parse NVIDIA SSE chunk:',
        raw
      );

      return;
    }


    const choice = data?.choices?.[0];

    if (!choice) {
      sendJSON(data);
      return;
    }


    const delta = choice.delta;

    if (!delta) {
      sendJSON(data);
      return;
    }


    const reasoning =
      delta.reasoning_content ||
      delta.reasoning ||
      '';

    const content =
      delta.content ||
      '';


    // ========================================================
    // REASONING
    // ========================================================

    if (reasoning.length > 0) {

      if (!thinkingStarted) {
        thinkingStarted = true;

        console.log(
          '>>> Reasoning detected in stream'
        );

        sendThinkStart(data);
      }

      sendReasoning(
        data,
        reasoning
      );
    }


    // ========================================================
    // NORMAL ANSWER
    // ========================================================

    if (content.length > 0) {

      if (
        thinkingStarted &&
        !thinkingEnded
      ) {
        thinkingEnded = true;

        sendThinkEnd(data);
      }

      sendNormalContent(
        data,
        content
      );
    }


    // ========================================================
    // FINISH REASONING IF MODEL ENDS WITHOUT CONTENT
    // ========================================================

    if (
      choice.finish_reason &&
      thinkingStarted &&
      !thinkingEnded
    ) {
      thinkingEnded = true;

      sendThinkEnd(data);
    }


    // If this chunk only contains finish information,
    // make sure JanitorAI still receives it.
    if (
      choice.finish_reason &&
      !content &&
      !reasoning
    ) {
      sendJSON(data);
    }
  }


  // ==========================================================
  // RECEIVE NVIDIA STREAM
  // ==========================================================

  response.data.on('data', chunk => {

    buffer += chunk.toString();

    const lines =
      buffer.split(/\r?\n/);

    // Keep incomplete SSE line for next chunk
    buffer =
      lines.pop() || '';

    for (const line of lines) {
      processLine(line);
    }
  });


  // ==========================================================
  // STREAM FINISHED
  // ==========================================================

  response.data.on('end', () => {

    // Process anything left in buffer
    if (buffer.trim()) {
      processLine(buffer);
    }

    console.log(
      '>>> NVIDIA stream finished'
    );

    res.end();
  });


  // ==========================================================
  // STREAM ERROR
  // ==========================================================

  response.data.on('error', error => {

    console.error(
      'NVIDIA streaming error:',
      error.message
    );

    if (!res.writableEnded) {
      res.end();
    }
  });
}


// ============================================================
// CHAT COMPLETIONS
// ============================================================

app.post(
  '/v1/chat/completions',
  async (req, res) => {

    console.log(
      '>>> Request received - Model:',
      req.body?.model
    );


    if (!NIM_API_KEY) {

      return res.status(500).json({
        error: {
          message:
            'NIM_API_KEY not set'
        }
      });
    }


    try {

      const body = {
        ...req.body
      };


      // ======================================================
      // REMOVE UNSUPPORTED FIELDS
      // ======================================================

      delete body.extra_body;
      delete body.logit_bias;
      delete body.presence_penalty;
      delete body.frequency_penalty;
      delete body.n;
      delete body.seed;


      const modelName =
        String(
          body.model || ''
        ).toLowerCase();


      // ======================================================
      // KIMI K3
      // ======================================================

      if (
        modelName.includes('kimi-k3') ||
        modelName.includes('kimi_k3')
      ) {

        body.reasoning_effort = 'max';

        console.log(
          '>>> Kimi K3 detected'
        );

        console.log(
          '>>> reasoning_effort = max'
        );
      }


      // ======================================================
      // DEEPSEEK
      // ======================================================

      if (
        modelName.includes('deepseek')
      ) {

        body.reasoning_effort = 'high';

        console.log(
          '>>> DeepSeek detected'
        );

        console.log(
          '>>> reasoning_effort = high'
        );
      }


      // ======================================================
      // OTHER THINKING MODELS
      // ======================================================

      if (
        modelName.includes('gemma') ||
        modelName.includes('minimax')
      ) {

        body.chat_template_kwargs = {
          enable_thinking: true
        };
      }


      const isStreaming =
        body.stream === true;


      console.log(
        '>>> Streaming:',
        isStreaming
      );


      // ======================================================
      // NVIDIA REQUEST
      // ======================================================

      const response = await axios({
        method: 'post',

        url:
          `${NIM_BASE}/chat/completions`,

        headers: {

          Authorization:
            `Bearer ${NIM_API_KEY}`,

          'Content-Type':
            'application/json',

          'User-Agent':
            'Mozilla/5.0',

          ...(isStreaming
            ? {
                Accept:
                  'text/event-stream'
              }
            : {})
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


      // ======================================================
      // NVIDIA ERROR
      // ======================================================

      if (
        response.status !== 200
      ) {

        let errorMessage =
          'Unknown NVIDIA error';


        try {

          if (
            typeof response.data ===
            'string'
          ) {

            errorMessage =
              response.data;

          } else if (
            response.data?.error?.message
          ) {

            errorMessage =
              response.data.error.message;

          } else {

            errorMessage =
              JSON.stringify(
                response.data
              ).slice(0, 1000);
          }

        } catch (error) {

          errorMessage =
            `NVIDIA returned HTTP ${response.status}`;
        }


        console.error(
          '>>> NVIDIA Error:',
          response.status,
          errorMessage
        );


        return res.status(
          response.status
        ).json({

          error: {

            message:
              errorMessage,

            type:
              'upstream_error',

            code:
              response.status
          }
        });
      }


      // ======================================================
      // STREAMING
      // ======================================================

      if (isStreaming) {

        res.statusCode = 200;

        res.setHeader(
          'Content-Type',
          'text/event-stream'
        );

        res.setHeader(
          'Cache-Control',
          'no-cache, no-transform'
        );

        res.setHeader(
          'Connection',
          'keep-alive'
        );

        res.setHeader(
          'Access-Control-Allow-Origin',
          '*'
        );

        res.setHeader(
          'X-Accel-Buffering',
          'no'
        );


        if (
          typeof res.flushHeaders ===
          'function'
        ) {
          res.flushHeaders();
        }


        console.log(
          '>>> Streaming response started'
        );

        console.log(
          '>>> Reasoning translator active'
        );


        handleStreamingResponse(
          response,
          res
        );


        return;
      }


      // ======================================================
      // NON-STREAMING RESPONSE
      // ======================================================

      const data =
        response.data;


      const message =
        data?.choices?.[0]?.message;


      if (message) {

        const reasoning =
          message.reasoning_content ||
          message.reasoning ||
          '';


        if (
          reasoning &&
          reasoning.trim().length > 0
        ) {

          const originalContent =
            message.content || '';


          message.content =
            `<think>\n` +
            `${reasoning.trim()}` +
            `\n</think>\n\n` +
            `${originalContent}`;


          delete message.reasoning_content;
          delete message.reasoning;
        }
      }


      res.json(data);

    } catch (error) {

      console.error(
        '>>> Proxy error:',
        error.message
      );


      if (
        !res.headersSent
      ) {

        res.status(500).json({

          error: {

            message:
              error.message ||
              'Internal proxy error',

            type:
              'proxy_error',

            code:
              500
          }
        });

      } else {

        res.end();
      }
    }
  }
);


// ============================================================
// START SERVER
// ============================================================

app.listen(
  PORT,
  '0.0.0.0',
  () => {

    console.log(
      `NVIDIA NIM Proxy running on port ${PORT}`
    );

    console.log(
      'Kimi K3 reasoning: MAX'
    );

    console.log(
      'DeepSeek reasoning: HIGH'
    );
  }
);
