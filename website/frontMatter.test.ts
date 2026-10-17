import {describe, expect, it} from 'vitest';
import path from 'node:path';
import {checkPages, readPages, type Page} from './frontMatter';

const page = (overrides: Partial<Page>): Page => ({
  file: 'docs/a.md',
  locale: 'en',
  route: 'a',
  title: 'A page',
  description: 'What the reader finds on this page.',
  ...overrides,
});

describe('checkPages', () => {
  it('accepts pages with a title and a whole-sentence description', () => {
    expect(checkPages([page({}), page({route: 'b', title: 'B page'})])).toEqual([]);
  });

  it('refuses a page without a description', () => {
    expect(checkPages([page({description: undefined})])).toEqual([
      'docs/a.md: no description in the front matter',
    ]);
  });

  it('refuses a description over 160 characters', () => {
    const long = `${'word '.repeat(32)}end.`;
    expect(checkPages([page({description: long})])).toEqual([
      `docs/a.md: the description has ${long.length} characters, more than 160`,
    ]);
  });

  it('refuses a description that does not end a sentence', () => {
    expect(checkPages([page({description: 'This page shows how the real'})])).toEqual([
      'docs/a.md: the description does not end with a full stop, so it reads as cut off',
    ]);
  });

  it('accepts a sentence that ends in Chinese punctuation', () => {
    expect(checkPages([page({locale: 'zh', description: '本页说明如何安装。'})])).toEqual([]);
  });

  it('refuses a page without a title', () => {
    expect(checkPages([page({title: undefined})])).toEqual(['docs/a.md: no title']);
  });

  it('refuses two pages of one language with the same title', () => {
    expect(checkPages([page({}), page({file: 'docs/b.md', route: 'b'})])).toEqual([
      "docs/b.md: the title 'A page' is also the title of docs/a.md",
    ]);
  });

  it('allows the same title in two languages', () => {
    expect(checkPages([page({}), page({file: 'ru/a.md', locale: 'ru'})])).toEqual([]);
  });
});

describe('the pages of the site', () => {
  it('each carry a title unique in their language and a description a search result shows whole', async () => {
    const pages = await readPages(path.resolve(__dirname, '..'));
    expect(pages.length).toBeGreaterThan(100);
    expect(checkPages(pages)).toEqual([]);
  });
});
