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
    service: 'Clean NIM Proxy - Diagnostic'
  });
});

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    keyConfigured: !!NIM_API_KEY
  });
});


// ============================================================
// KIMI STREAM DIAGNOSTIC
// IMPORTANT: THIS DOES NOT MODIFY THE STREAM
// ============================================================

function monitorKimiStream(stream) {
  let buffer = '';

  let chunkCount = 0;
  let reasoningChunks = 0;
  let contentChunks = 0;

  let firstChunkLogged = false;
  let firstReasoningLogged = false;

  function processLine(line) {
    const trimmed = line.trim();

    if (!trimmed) {
      return;
    }

    if (!trimmed.startsWith('data:')) {
      return;
    }

    const raw = trimmed.slice(5).trim();

    if (!raw || raw === '[DONE]') {
      return;
    }

    let data;

    try {
      data = JSON.parse(raw);
    } catch {
      return;
    }

    chunkCount++;

    const choice = data?.choices?.[0];

    if (!choice) {
      return;
    }

    const delta = choice.delta;

    if (!delta) {
      return;
    }

    const reasoning =
      delta.reasoning_content ||
      delta.reasoning ||
      '';

    const content =
      delta.content ||
      '';


    // --------------------------------------------------------
    // FIRST CHUNK
    // --------------------------------------------------------

    if (!firstChunkLogged) {
      firstChunkLogged = true;

      console.log(
        '========== KIMI FIRST STREAM CHUNK =========='
      );

      console.log(
        'delta keys:',
        Object.keys(delta)
      );

      console.log(
        'has reasoning_content:',
        Object.prototype.hasOwnProperty.call(
          delta,
          'reasoning_content'
        )
      );

      console.log(
        'has reasoning:',
        Object.prototype.hasOwnProperty.call(
          delta,
          'reasoning'
        )
      );

      console.log(
        'has content:',
        Object.prototype.hasOwnProperty.call(
          delta,
          'content'
        )
      );

      console.log(
        '=============================================='
      );
    }


    // --------------------------------------------------------
    // REASONING DETECTED
    // --------------------------------------------------------

    if (reasoning.length > 0) {

      reasoningChunks++;

      if (!firstReasoningLogged) {
        firstReasoningLogged = true;

        console.log(
          '!!!!!!!! KIMI REASONING DETECTED !!!!!!!!'
        );

        console.log(
          'reasoning field:',
          Object.prototype.hasOwnProperty.call(
            delta,
            'reasoning_content'
          )
            ? 'reasoning_content'
            : 'reasoning'
        );

        console.log(
          'reasoning length:',
          reasoning.length
        );

        console.log(
          '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
        );
      }
    }


    // --------------------------------------------------------
    // NORMAL CONTENT
    // --------------------------------------------------------

    if (content.length > 0) {
      contentChunks++;
    }
  }


  stream.on('data', chunk => {

    buffer += chunk.toString();

    const lines =
      buffer.split(/\r?\n/);

    buffer =
      lines.pop() || '';

    for (const line of lines) {
      processLine(line);
    }
  });


  stream.on('end', () => {

    if (buffer.trim()) {
      processLine(buffer);
    }

    console.log(
      '========== KIMI STREAM SUMMARY =========='
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
      reasoningChunks > 0 ? 'YES' : 'NO'
    );

    console.log(
      '=========================================='
    );
  });


  stream.on('error', error => {

    console.error(
      'KIMI STREAM ERROR:',
      error.message
    );
  });
}


// ============================================================
// CHAT COMPLETIONS
// ============================================================

app.post(
  '/v1/chat/completions',
  async (req, res) => {

    console.log(
      '>>> REQUEST RECEIVED'
    );

    console.log(
      '>>> Model:',
      req.body?.model
    );

    if (!NIM_API_KEY) {

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


      // ------------------------------------------------------
      // REMOVE UNSUPPORTED FIELDS
      // ------------------------------------------------------

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


      // ------------------------------------------------------
      // KIMI K3
      // ------------------------------------------------------

      const isKimi =
        modelName.includes('kimi-k3') ||
        modelName.includes('kimi_k3');


      if (isKimi) {

        body.reasoning_effort = 'max';

        console.log(
          '>>> KIMI K3 DETECTED'
        );

        console.log(
          '>>> reasoning_effort:',
          body.reasoning_effort
        );

        console.log(
          '>>> stream:',
          body.stream === true
        );

        console.log(
          '>>> max_tokens:',
          body.max_tokens
        );

        console.log(
          '>>> temperature:',
          body.temperature
        );
      }


      // ------------------------------------------------------
      // DEEPSEEK
      // ------------------------------------------------------

      if (
        modelName.includes('deepseek')
      ) {

        body.reasoning_effort = 'high';

        console.log(
          '>>> DeepSeek detected'
        );

        console.log(
          '>>> reasoning_effort: high'
        );
      }


      // ------------------------------------------------------
      // OTHER THINKING MODELS
      // ------------------------------------------------------

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


      // ------------------------------------------------------
      // SEND TO NVIDIA
      // ------------------------------------------------------

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

          ...(isStreaming
            ? {
                'Accept':
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


      // ------------------------------------------------------
      // NVIDIA ERROR
      // ------------------------------------------------------

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

        } catch {

          errorMessage =
            `NVIDIA returned HTTP ${response.status}`;
        }


        console.error(
          '>>> NVIDIA ERROR:',
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
          '>>> STREAMING RESPONSE STARTED'
        );


        // ----------------------------------------------------
        // KIMI DIAGNOSTIC
        //
        // Monitor the stream WITHOUT modifying it.
        // ----------------------------------------------------

        if (isKimi) {

          console.log(
            '>>> KIMI DIAGNOSTIC MONITOR ACTIVE'
          );

          monitorKimiStream(
            response.data
          );
        }


        // ----------------------------------------------------
        // PASS THE ORIGINAL NVIDIA STREAM THROUGH
        // ----------------------------------------------------

        response.data.pipe(res);

        return;
      }


      // ======================================================
      // NON-STREAMING RESPONSE
      // ======================================================

      const data =
        response.data;


      if (isKimi) {

        const message =
          data?.choices?.[0]?.message;

        const reasoning =
          message?.reasoning_content ||
          message?.reasoning ||
          '';

        console.log(
          '========== KIMI NON-STREAM =========='
        );

        console.log(
          'Reasoning received:',
          reasoning.length > 0
            ? 'YES'
            : 'NO'
        );

        console.log(
          'Reasoning length:',
          reasoning.length
        );

        console.log(
          '======================================'
        );
      }


      // ------------------------------------------------------
      // DO NOT MODIFY THE RESPONSE
      // ------------------------------------------------------

      res.json(data);

    } catch (error) {

      console.error(
        '>>> PROXY ERROR:',
        error.message
      );


      if (!res.headersSent) {

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

    console.log(
      'Kimi diagnostic mode: ENABLED'
    );
  }
);
