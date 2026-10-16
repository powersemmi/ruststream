// The chat endpoint behind Context7's own widget. It is not a documented API, so every way it
// can fail (an HTTP error, a network error, a body in another format) surfaces as one error the
// panel answers by falling back to the official widget.
export const CHAT_URL = 'https://context7.com/api/v2/widget/chat';
export const LIBRARY = '/powersemmi/ruststream';

export class AskAiUnavailable extends Error {
  override name = 'AskAiUnavailable';
}

export interface ChatTurn {
  role: 'user' | 'assistant';
  text: string;
}

interface StreamEvent {
  type?: unknown;
  delta?: unknown;
  errorText?: unknown;
}

const DATA = 'data:';
const END_OF_STREAM = '[DONE]';

function requestBody(turns: ChatTurn[]): string {
  return JSON.stringify({
    messages: turns.map((turn, index) => ({
      id: `m${index}`,
      role: turn.role,
      content: turn.text,
      parts: [{type: 'text', text: turn.text}],
    })),
    libraryName: LIBRARY,
  });
}

async function failureReason(response: Response): Promise<string> {
  try {
    const body = (await response.json()) as {message?: unknown};
    if (typeof body.message === 'string') {
      return body.message;
    }
  } catch {
    // The status line is the whole story when the body is not the endpoint's JSON error.
  }
  return `HTTP ${response.status}`;
}

function isAbort(error: unknown, signal: AbortSignal | undefined): boolean {
  return signal?.aborted === true || (error instanceof Error && error.name === 'AbortError');
}

/** Text of one `data:` line, or `null` for lines that carry no text. */
function textOf(payload: string): string | null {
  if (payload === END_OF_STREAM) {
    return null;
  }
  let event: StreamEvent;
  try {
    event = JSON.parse(payload) as StreamEvent;
  } catch {
    throw new AskAiUnavailable(`Unexpected stream line: ${payload.slice(0, 80)}`);
  }
  if (event.type === 'error') {
    throw new AskAiUnavailable(String(event.errorText ?? 'The answer stream failed'));
  }
  if (event.type === 'text-delta' && typeof event.delta === 'string') {
    return event.delta;
  }
  return null;
}

/**
 * Asks Context7 about RustStream and yields the answer as it streams in.
 *
 * Throws `AskAiUnavailable` when the endpoint cannot answer; an abort through `signal` is
 * rethrown as it is, because a closed panel is not an outage.
 */
export async function* streamAnswer(
  turns: ChatTurn[],
  options: {fetch?: typeof fetch; signal?: AbortSignal} = {},
): AsyncGenerator<string> {
  const {fetch: fetchImpl = globalThis.fetch.bind(globalThis), signal} = options;
  let response: Response;
  try {
    response = await fetchImpl(CHAT_URL, {
      method: 'POST',
      headers: {'content-type': 'application/json'},
      body: requestBody(turns),
      signal,
    });
  } catch (error) {
    if (isAbort(error, signal)) {
      throw error;
    }
    throw new AskAiUnavailable(error instanceof Error ? error.message : String(error));
  }
  if (!response.ok) {
    throw new AskAiUnavailable(await failureReason(response));
  }
  if (!response.body) {
    throw new AskAiUnavailable('The answer has no body');
  }

  const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
  let pending = '';
  let answered = false;
  const linesOf = function* (text: string): Generator<string> {
    for (const line of text.split('\n')) {
      if (line.startsWith(DATA)) {
        const delta = textOf(line.slice(DATA.length).trim());
        if (delta) {
          answered = true;
          yield delta;
        }
      }
    }
  };
  try {
    for (;;) {
      const {done, value} = await reader.read();
      if (done) {
        break;
      }
      pending += value;
      const complete = pending.lastIndexOf('\n');
      if (complete >= 0) {
        yield* linesOf(pending.slice(0, complete));
        pending = pending.slice(complete + 1);
      }
    }
    yield* linesOf(pending);
  } catch (error) {
    if (error instanceof AskAiUnavailable || isAbort(error, signal)) {
      throw error;
    }
    throw new AskAiUnavailable(error instanceof Error ? error.message : String(error));
  } finally {
    reader.releaseLock();
  }
  if (!answered) {
    throw new AskAiUnavailable('The answer stream carried no text');
  }
}
