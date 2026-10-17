import {describe, expect, it} from 'vitest';
import {loadOfficialWidget, WIDGET_URL} from './fallback';

// A stand-in for the few document calls the loader makes.
function fakeDocument() {
  const appended: Array<{src: string; async: boolean; dataset: Record<string, string>}> = [];
  return {
    appended,
    host: {
      querySelector: (selector: string) =>
        appended.find((script) => selector === `script[src="${script.src}"]`) ?? null,
      createElement: () => ({src: '', async: false, dataset: {} as Record<string, string>}),
      body: {appendChild: (script: (typeof appended)[number]) => appended.push(script)},
    } as unknown as Parameters<typeof loadOfficialWidget>[1],
  };
}

describe('loadOfficialWidget', () => {
  it('adds the widget for the library in the site accent, once', () => {
    const {appended, host} = fakeDocument();
    loadOfficialWidget('#6200ee', host);
    loadOfficialWidget('#6200ee', host);
    expect(appended).toEqual([
      {
        src: WIDGET_URL,
        async: true,
        dataset: {library: '/powersemmi/ruststream', color: '#6200ee'},
      },
    ]);
  });
});
