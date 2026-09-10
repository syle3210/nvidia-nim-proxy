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


/*
========================================================
 STREAMING SSE REASONING CONVERTER
========================================================

NVIDIA/Kimi can send:

reasoning_content: "some thinking..."

JanitorAI may not display that field correctly.

This converter turns the streamed reasoning into:

<think>
some thinking...
</think>

while preserving normal streamed content.
*/

function createReasoningStream(res) {
  let buffer = '';
  let thinkingStarted = false;
  let thinkingEnded = false;

  function sendChunk(json) {
    res.write(`data: ${JSON.stringify(json)}\n\n`);
  }

  function processLine(line) {
    // Ignore empty SSE lines
    if (!line.trim()) return;

    // Preserve [DONE]
    if (line.trim() === 'data: [DONE]') {
      res.write('data: [DONE]\n\n');
      return;
    }

    // Only process SSE data lines
    if (!line.startsWith('data:')) {
      res.write(line + '\n');
      return;
    }

    const raw = line.slice(5).trim();

    if (!raw) return;

    let json;

    try {
      json = JSON.parse(raw);
    } catch (err) {
      // If NVIDIA sends something that isn't JSON,
      // pass it through instead of breaking the stream.
      res.write(line + '\n\n');
      return;
    }

    const choice = json?.choices?.[0];

    if (!choice) {
      sendChunk(json);
      return;
    }

    const delta = choice.delta;

    if (!delta) {
      sendChunk(json);
      return;
    }

    const reasoning =
      delta.reasoning_content ||
      delta.reasoning ||
      '';

    const content =
      delta.content ||
      '';


    /*
    ====================================================
    KIMI REASONING
    ====================================================
    */

    if (reasoning && reasoning.length > 0) {

      // Start <think> only once
      if (!thinkingStarted) {
        thinkingStarted = true;

        const startChunk = {
          ...json,
          choices: json.choices.map(c => ({
            ...c,
            delta: {
              ...c.delta,
              reasoning_content: undefined,
              reasoning: undefined,
              content: '<think>\n'
            }
          }))
        };

        sendChunk(startChunk);
      }

      // Send the actual reasoning as content
      const reasoningChunk = {
        ...json,
        choices: json.choices.map(c => ({
          ...c,
          delta: {
            ...c.delta,
            reasoning_content: undefined,
            reasoning: undefined,
            content: reasoning
          }
        }))
      };

      sendChunk(reasoningChunk);
    }


    /*
    ====================================================
    NORMAL ANSWER
    ====================================================
    */

    if (content && content.length > 0) {

      // Close <think> before the normal answer
      if (thinkingStarted && !thinkingEnded) {
        thinkingEnded = true;

        const endChunk = {
          ...json,
          choices: json.choices.map(c => ({
            ...c,
            delta: {
              ...c.delta,
              reasoning_content: undefined,
              reasoning: undefined,
              content: '\n</think>\n\n'
            }
          }))
        };

        sendChunk(endChunk);
      }

      const contentChunk = {
        ...json,
        choices: json.choices.map(c => ({
          ...c,
          delta: {
            ...c.delta,
            reasoning_content: undefined,
            reasoning: undefined,
            content: content
          }
        }))
      };

      sendChunk(contentChunk);
    }


    /*
    ====================================================
    FINISH REASONING IF NVIDIA FINISHES WITHOUT CONTENT
    ====================================================
    */

    if (
      choice.finish_reason &&
      thinkingStarted &&
      !thinkingEnded
    ) {
      thinkingEnded = true;

      const endChunk = {
        ...json,
        choices: json.choices.map(c => ({
          ...c,
          delta: {
            ...c.delta,
            reasoning_content: undefined,
            reasoning: undefined,
            content: '\n</think>\n\n'
          }
        }))
      };

      sendChunk(endChunk);
    }
  }


  return new (require('stream').Transform)({
    transform(chunk, encoding, callback) {
      buffer += chunk.toString();

      const lines = buffer.split(/\r?\n/);

      // Keep incomplete final line
      buffer = lines.pop() || '';

      for (const line of lines) {
        processLine(line);
      }

      callback();
    },

    flush(callback) {
      if (buffer.trim()) {
        processLine(buffer);
      }

      callback();
    }
  });
}


/*
========================================================
 MAIN CHAT COMPLETIONS ENDPOINT
========================================================
*/

app.post('/v1/chat/completions', async (req, res) => {

  console.log(
    '>>> Request received - Model:',
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


    /*
    ====================================================
    CLEAN PROBLEMATIC FIELDS
    ====================================================
    */

    delete body.extra_body;
    delete body.logit_bias;
    delete body.presence_penalty;
    delete body.frequency_penalty;
    delete body.n;
    delete body.seed;


    const modelName =
      (body.model || '').toLowerCase();


    /*
    ====================================================
    REASONING SETTINGS
    ====================================================
    */


    // Kimi K3
    if (
      modelName.includes('kimi-k3') ||
      modelName.includes('kimi_k3')
    ) {
      body.reasoning_effort = 'max';

      console.log(
        '>>> Kimi K3 detected - reasoning_effort=max'
      );
    }


    // DeepSeek
    if (
      modelName.includes('deepseek')
    ) {
      body.reasoning_effort = 'high';

      console.log(
        '>>> DeepSeek detected - reasoning_effort=high'
      );
    }


    // Other thinking models
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


    /*
    ====================================================
    SEND REQUEST TO NVIDIA
    ====================================================
    */

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


    /*
    ====================================================
    NVIDIA ERROR
    ====================================================
    */

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

      } catch (e) {

        errorMsg =
          `NVIDIA returned status ${response.status}`;
      }


      console.error(
        'NVIDIA Error:',
        response.status,
        errorMsg
      );


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


    /*
    ====================================================
    STREAMING RESPONSE
    ====================================================
    */

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


      // Flush headers immediately
      if (res.flushHeaders) {
        res.flushHeaders();
      }


      console.log(
        '>>> Streaming response - reasoning translator active'
      );


      /*
      IMPORTANT:

      Instead of:

      response.data.pipe(res)

      we now pass NVIDIA's SSE stream
      through our reasoning converter.
      */

      const reasoningStream =
        createReasoningStream(res);


      response.data.on(
        'error',
        (err) => {

          console.error(
            'NVIDIA stream error:',
            err.message
          );

          if (!res.headersSent) {
            res.status(500);
          }

          res.end();
        }
      );


      reasoningStream.on(
        'error',
        (err) => {

          console.error(
            'Reasoning stream error:',
            err.message
          );

          res.end();
        }
      );


      reasoningStream.on(
        'finish',
        () => {

          console.log(
            '>>> Streaming response finished'
          );

          res.end();
        }
      );


      response.data.pipe(
        reasoningStream
      );

      return;
    }


    /*
    ====================================================
    NON-STREAMING RESPONSE
    ====================================================
    */

    const data =
      response.data;


    if (
      data?.choices?.[0]?.message
    ) {

      const msg =
        data.choices[0].message;


      const reasoning =
        msg.reasoning_content ||
        msg.reasoning ||
        '';


      if (
        reasoning &&
        reasoning.trim().length > 0
      ) {

        const originalContent =
          msg.content || '';


        msg.content =
          `<think>\n` +
          `${reasoning.trim()}` +
          `\n</think>\n\n` +
          `${originalContent}`;


        /*
        Prevent duplicate reasoning
        fields from confusing JanitorAI.
        */

        delete msg.reasoning_content;
        delete msg.reasoning;
      }
    }


    res.json(data);

  } catch (err) {

    console.error(
      'Proxy error:',
      err.message
    );


    if (!res.headersSent) {

      res.status(500).json({
        error: {
          message:
            err.message ||
            'Internal proxy error',

          type:
            'proxy_error',

          code: 500
        }
      });

    } else {

      res.end();
    }
  }
});


/*
========================================================
 START SERVER
========================================================
*/

app.listen(
  PORT,
  '0.0.0.0',
  () => {

    console.log(
      `Clean NIM Proxy running on port ${PORT}`
    );

    console.log(
      'Kimi K3: reasoning_effort=max'
    );

    console.log(
      'DeepSeek: reasoning_effort=high'
    );
  }
);
