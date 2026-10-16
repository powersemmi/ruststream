// Context7's official widget, loaded when the panel cannot reach the chat endpoint. It renders
// its own launcher in a corner of the page and keeps working whatever happens to the endpoint.
export const WIDGET_URL = 'https://context7.com/widget.js';
export const LIBRARY_PAGE = 'https://context7.com/powersemmi/ruststream';

type WidgetHost = Pick<Document, 'querySelector' | 'createElement' | 'body'>;

/** Adds the widget's script to the page once; later calls find it there and do nothing. */
export function loadOfficialWidget(accent: string, host: WidgetHost = document): void {
  if (host.querySelector(`script[src="${WIDGET_URL}"]`)) {
    return;
  }
  const script = host.createElement('script');
  script.src = WIDGET_URL;
  script.async = true;
  script.dataset.library = '/powersemmi/ruststream';
  script.dataset.color = accent;
  host.body.appendChild(script);
}
