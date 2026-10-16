import {describe, expect, it, vi} from 'vitest';
import {AskAiUnavailable, CHAT_URL, LIBRARY, streamAnswer, type ChatTurn} from './client';

const question: ChatTurn[] = [{role: 'user', text: 'How do I install ruststream?'}];

function streamOf(chunks: string[], init: ResponseInit = {status: 200}): Response {
  const encoder = new TextEncoder();
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(encoder.encode(chunk));
      }
      controller.close();
    },
  });
  return new Response(body, {
    headers: {'content-type': 'text/event-stream'},
    ...init,
  });
}

async function collect(fetchStub: typeof fetch, turns = question): Promise<string> {
  let text = '';
  for await (const delta of streamAnswer(turns, {fetch: fetchStub})) {
    text += delta;
  }
  return text;
}

// A trimmed capture of the endpoint's answer: tool calls first, then the text, then the end.
const answer = [
  'data: {"type":"start","messageId":"m"}\n\n',
  'data: {"type":"tool-input-available","toolCallId":"t","toolName":"queryDocs","input":{}}\n\n',
  'data: {"type":"tool-output-available","toolCallId":"t","output":"### Install"}\n\n',
  'data: {"type":"text-start","id":"g"}\n\ndata: {"type":"text-delta","id":"g","delta":"Add `rust',
  'stream`"}\n\ndata: {"type":"text-delta","id":"g","delta":" to Cargo.toml."}\n\n',
  'data: {"type":"finish"}\n\ndata: [DONE]\n\n',
];

describe('streamAnswer', () => {
  it('yields the text deltas of a normal stream and ignores the other events', async () => {
    const fetchStub = vi.fn(async () => streamOf(answer));
    expect(await collect(fetchStub)).toBe('Add `ruststream` to Cargo.toml.');
  });

  it('sends the conversation in the shape the widget endpoint expects', async () => {
    const fetchStub = vi.fn(async (_url: string | URL | Request, _init?: RequestInit) =>
      streamOf(answer),
    );
    await collect(fetchStub, [
      {role: 'user', text: 'first'},
      {role: 'assistant', text: 'reply'},
      {role: 'user', text: 'second'},
    ]);
    const [url, init] = fetchStub.mock.calls[0]!;
    expect(url).toBe(CHAT_URL);
    expect(init?.method).toBe('POST');
    expect(JSON.parse(init?.body as string)).toEqual({
      libraryName: LIBRARY,
      messages: [
        {id: 'm0', role: 'user', content: 'first', parts: [{type: 'text', text: 'first'}]},
        {id: 'm1', role: 'assistant', content: 'reply', parts: [{type: 'text', text: 'reply'}]},
        {id: 'm2', role: 'user', content: 'second', parts: [{type: 'text', text: 'second'}]},
      ],
    });
  });

  it('raises AskAiUnavailable with the server message on a non-2xx answer', async () => {
    const fetchStub = vi.fn(
      async () =>
        new Response(JSON.stringify({error: 'forbidden', message: 'Origin not allowed'}), {
          status: 403,
          headers: {'content-type': 'application/json'},
        }),
    );
    const failure = collect(fetchStub);
    await expect(failure).rejects.toBeInstanceOf(AskAiUnavailable);
    await expect(failure).rejects.toThrow('Origin not allowed');
  });

  it('raises AskAiUnavailable on a network error', async () => {
    const fetchStub = vi.fn(async () => {
      throw new TypeError('Failed to fetch');
    });
    await expect(collect(fetchStub)).rejects.toBeInstanceOf(AskAiUnavailable);
  });

  it('raises AskAiUnavailable on a body with no data lines', async () => {
    const fetchStub = vi.fn(async () => streamOf(['<!doctype html><p>maintenance</p>']));
    await expect(collect(fetchStub)).rejects.toBeInstanceOf(AskAiUnavailable);
  });

  it('raises AskAiUnavailable on a data line that is not JSON', async () => {
    const fetchStub = vi.fn(async () => streamOf(['data: {"type":"text-delta","delta":"a"}\n\n', 'data: oops\n\n']));
    await expect(collect(fetchStub)).rejects.toBeInstanceOf(AskAiUnavailable);
  });

  it('raises AskAiUnavailable on an error event inside the stream', async () => {
    const fetchStub = vi.fn(async () =>
      streamOf(['data: {"type":"error","errorText":"Widget is not enabled"}\n\n']),
    );
    await expect(collect(fetchStub)).rejects.toThrow('Widget is not enabled');
  });

  it('lets a cancellation through as an abort, not as an outage', async () => {
    const controller = new AbortController();
    controller.abort();
    const fetchStub = vi.fn(async (_url: string | URL | Request, init?: RequestInit) => {
      init?.signal?.throwIfAborted();
      return streamOf(answer);
    });
    const run = async () => {
      for await (const _ of streamAnswer(question, {fetch: fetchStub, signal: controller.signal})) {
        // drained
      }
    };
    await expect(run()).rejects.not.toBeInstanceOf(AskAiUnavailable);
  });
});
