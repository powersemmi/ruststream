// The panel lives once per page, in the site root; the navbar button and the search entry reach
// it through a window event, so neither has to sit under the same React tree as the panel.
export const OPEN_EVENT = 'ruststream:ask-ai';
export const CLOSE_EVENT = 'ruststream:ask-ai-close';

export interface OpenRequest {
  /** A question to send at once in a new conversation; empty opens the panel as it was. */
  query: string;
}

export function openAskAi(query = ''): void {
  window.dispatchEvent(new CustomEvent<OpenRequest>(OPEN_EVENT, {detail: {query}}));
}

export function closeAskAi(): void {
  window.dispatchEvent(new Event(CLOSE_EVENT));
}
