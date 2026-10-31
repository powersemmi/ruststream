import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {afterAll, beforeAll, describe, expect, it} from 'vitest';
import {unified} from 'unified';
import remarkParse from 'remark-parse';
import type {Link, Root} from 'mdast';
import {visit} from 'unist-util-visit';
import {VFile} from 'vfile';
import remarkApiLinks from './remarkApiLinks';

const docsRs = 'https://docs.rs/ruststream/0.7/';
let rustdocDir: string;

// The parts of a `cargo doc` output the resolver reads: the crate's list of every item, the
// pages it links to, and a module's index page.
const allItems = [
  '<h3 id="structs">Structs</h3><ul class="all-items">',
  '<li><a href="struct.AppId.html">AppId</a></li>',
  '<li><a href="runtime/struct.RustStream.html">runtime::RustStream</a></li>',
  '</ul><h3 id="derives">Derive Macros</h3><ul class="all-items">',
  '<li><a href="derive.Outgoing.html">Outgoing</a></li>',
  '</ul>',
].join('');

function write(file: string, html: string): void {
  const target = path.join(rustdocDir, 'ruststream', file);
  fs.mkdirSync(path.dirname(target), {recursive: true});
  fs.writeFileSync(target, html);
}

beforeAll(() => {
  rustdocDir = fs.mkdtempSync(path.join(os.tmpdir(), 'remark-api-links-'));
  write('all.html', `<html><body>${allItems}</body></html>`);
  write('index.html', '<h2 id="modules">Modules</h2>');
  write('struct.AppId.html', '<h1>AppId</h1>');
  write('derive.Outgoing.html', '<h1>Outgoing</h1>');
  write(
    'runtime/struct.RustStream.html',
    '<section id="method.run"></section><section id="method.with_broker"></section>',
  );
  write('runtime/index.html', '<h2 id="subscribers">Subscribers</h2>');
});

afterAll(() => {
  fs.rmSync(rustdocDir, {recursive: true, force: true});
});

async function links(markdown: string, dir = rustdocDir): Promise<string[]> {
  const processor = unified().use(remarkParse).use(remarkApiLinks, {docsRs, rustdocDir: dir});
  const file = new VFile({path: 'docs/components/application/index.md', value: markdown});
  const tree = (await processor.run(processor.parse(file), file)) as Root;
  const urls: string[] = [];
  visit(tree, 'link', (node: Link) => {
    urls.push(node.url);
  });
  return urls;
}

describe('remarkApiLinks', () => {
  it('sends an item to its page on docs.rs, in the version the build documents', async () => {
    expect(await links('[`RustStream`](api:ruststream::runtime::RustStream)')).toEqual([
      'https://docs.rs/ruststream/0.7/ruststream/runtime/struct.RustStream.html',
    ]);
  });

  it('resolves items at the crate root and derive macros by their own pages', async () => {
    expect(
      await links('[a](api:ruststream::AppId) and [b](api:ruststream::Outgoing)'),
    ).toEqual([
      'https://docs.rs/ruststream/0.7/ruststream/struct.AppId.html',
      'https://docs.rs/ruststream/0.7/ruststream/derive.Outgoing.html',
    ]);
  });

  it('sends a module to its index page and keeps a section anchor that exists', async () => {
    expect(
      await links('[a](api:ruststream::runtime) [b](api:ruststream::runtime#subscribers) [c](api:ruststream)'),
    ).toEqual([
      'https://docs.rs/ruststream/0.7/ruststream/runtime/index.html',
      'https://docs.rs/ruststream/0.7/ruststream/runtime/index.html#subscribers',
      'https://docs.rs/ruststream/0.7/ruststream/index.html',
    ]);
  });

  it('sends a method to its anchor on the page of its type', async () => {
    expect(await links('[run](api:ruststream::runtime::RustStream::run)')).toEqual([
      'https://docs.rs/ruststream/0.7/ruststream/runtime/struct.RustStream.html#method.run',
    ]);
  });

  it('leaves every other link alone', async () => {
    expect(await links('[a](https://example.com) [b](../x.md)')).toEqual([
      'https://example.com',
      '../x.md',
    ]);
  });

  it('fails on an item the version does not have, naming the page and the item', async () => {
    await expect(links('[x](api:ruststream::runtime::Missing)')).rejects.toThrow(
      /docs\/components\/application\/index\.md.*ruststream::runtime::Missing/s,
    );
  });

  it('fails on a member or a section the page of the item does not have', async () => {
    await expect(links('[x](api:ruststream::runtime::RustStream::stop)')).rejects.toThrow(
      /ruststream::runtime::RustStream::stop/,
    );
    await expect(links('[x](api:ruststream::runtime#nowhere)')).rejects.toThrow(
      /ruststream::runtime#nowhere/,
    );
  });

  it('reads no rustdoc for a page without API links', async () => {
    expect(await links('[a](https://example.com)', '/nonexistent')).toEqual([
      'https://example.com',
    ]);
  });

  it('names the command that produces the rustdoc when it is missing', async () => {
    await expect(links('[x](api:ruststream::AppId)', '/nonexistent')).rejects.toThrow(
      /just doc/,
    );
  });
});
