import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {afterAll, beforeAll, describe, expect, it} from 'vitest';
import {unified} from 'unified';
import remarkParse from 'remark-parse';
import type {Code, Root} from 'mdast';
import remarkSnippets from './remarkSnippets';

const repoUrl = 'https://github.com/powersemmi/ruststream/blob/main';
let rootDir: string;

const source = [
  'use std::fmt;',
  '',
  '// --8<-- [start:outer]',
  'fn outer() {',
  '    // --8<-- [start:inner]',
  '    let x = 1;',
  '    // --8<-- [end:inner]',
  '}',
  '// --8<-- [end:outer]',
  '',
].join('\n');

beforeAll(() => {
  rootDir = fs.mkdtempSync(path.join(os.tmpdir(), 'remark-snippets-'));
  fs.mkdirSync(path.join(rootDir, 'examples'));
  fs.writeFileSync(path.join(rootDir, 'examples/demo.rs'), source);
});

afterAll(() => {
  fs.rmSync(rootDir, {recursive: true, force: true});
});

async function codeBlocks(markdown: string): Promise<Code[]> {
  const processor = unified().use(remarkParse).use(remarkSnippets, {rootDir, repoUrl});
  const tree = (await processor.run(processor.parse(markdown))) as Root;
  return tree.children.filter((node): node is Code => node.type === 'code');
}

describe('remarkSnippets', () => {
  it('extracts a named region, dedented, without the markers of nested regions', async () => {
    const [code] = await codeBlocks('```rust reference="examples/demo.rs#outer"\n```\n');
    expect(code!.value).toBe('fn outer() {\n    let x = 1;\n}');
  });

  it('dedents a region nested in a block', async () => {
    const [code] = await codeBlocks('```rust reference="examples/demo.rs#inner"\n```\n');
    expect(code!.value).toBe('let x = 1;');
  });

  it('includes a whole file without any region marker', async () => {
    const [code] = await codeBlocks('```rust reference="examples/demo.rs"\n```\n');
    expect(code!.value).toBe('use std::fmt;\n\nfn outer() {\n    let x = 1;\n}');
  });

  it('titles the block with a link to the source file', async () => {
    const [code] = await codeBlocks('```rust reference="examples/demo.rs#inner"\n```\n');
    expect(code!.meta).toBe(`title="${repoUrl}/examples/demo.rs"`);
  });

  it('keeps a title the page sets', async () => {
    const [code] = await codeBlocks(
      '```rust title="src/main.rs" reference="examples/demo.rs#inner"\n```\n',
    );
    expect(code!.meta).toBe('title="src/main.rs"');
  });

  it('puts the lines written in the fence before the snippet', async () => {
    const [code] = await codeBlocks(
      '```rust reference="examples/demo.rs#inner"\nuse demo::prelude::*;\n\n```\n',
    );
    expect(code!.value).toBe('use demo::prelude::*;\n\nlet x = 1;');
  });

  it('leaves a fence with no reference alone', async () => {
    const [code] = await codeBlocks('```rust title="a.rs"\nfn main() {}\n```\n');
    expect(code!.value).toBe('fn main() {}');
    expect(code!.meta).toBe('title="a.rs"');
  });

  it('fails naming the file when it does not exist', async () => {
    await expect(codeBlocks('```rust reference="examples/missing.rs#x"\n```\n')).rejects.toThrow(
      /examples\/missing\.rs/,
    );
  });

  it('fails naming the region when the file has no such region', async () => {
    await expect(codeBlocks('```rust reference="examples/demo.rs#absent"\n```\n')).rejects.toThrow(
      /region 'absent' not found in examples\/demo\.rs/,
    );
  });
});
