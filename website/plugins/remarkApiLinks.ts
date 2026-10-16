import fs from 'node:fs';
import path from 'node:path';
import type {Link, Root} from 'mdast';
import type {VFile} from 'vfile';
import {visit} from 'unist-util-visit';

/**
 * Turns a link to a Rust item into its page on docs.rs, in the version the build documents.
 *
 * A page writes `[RustStream](api:ruststream::runtime::RustStream)`: an item, a module
 * (`api:ruststream::runtime`), a member of a type (`api:ruststream::runtime::RustStream::run`),
 * or a section of a page (`api:ruststream::runtime#subscribers`). The link resolves against the
 * rustdoc of the checkout being built, `cargo doc --all-features --no-deps`, which is what
 * docs.rs renders for that version; a path or a section that rustdoc does not have fails the
 * build with the page and the path. The rustdoc is read on the first such link, so a build
 * whose pages have none (an archive) needs none.
 */
export interface Options {
  /** The crate's address on docs.rs for the version built, with a trailing slash. */
  docsRs: string;
  /** The rustdoc output of the checkout being built (`target/doc`). */
  rustdocDir: string;
}

const SCHEME = 'api:';
// The anchor prefixes rustdoc gives the members on the page of their type.
const MEMBER_ANCHORS = [
  'method',
  'tymethod',
  'structfield',
  'variant',
  'associatedtype',
  'associatedconstant',
];
const ITEM = /<li><a href="([^"]+)">([^<]+)<\/a><\/li>/g;
const ID = /\bid="([^"]+)"/g;

class Rustdoc {
  private readonly items = new Map<string, Map<string, string>>();
  private readonly anchors = new Map<string, Set<string>>();

  constructor(private readonly dir: string) {}

  /** The page of an item or module, relative to the crate's directory, or undefined. */
  page(crate: string, itemPath: string[]): string | undefined {
    const items = this.itemsOf(crate);
    if (itemPath.length === 0) {
      return 'index.html';
    }
    const page = items.get(itemPath.join('::'));
    if (page) {
      return page;
    }
    const module = `${itemPath.join('/')}/index.html`;
    return fs.existsSync(path.join(this.dir, crate, module)) ? module : undefined;
  }

  hasAnchor(crate: string, page: string, anchor: string): boolean {
    const file = path.join(this.dir, crate, page);
    let ids = this.anchors.get(file);
    if (!ids) {
      ids = new Set(Array.from(fs.readFileSync(file, 'utf8').matchAll(ID), (m) => m[1]!));
      this.anchors.set(file, ids);
    }
    return ids.has(anchor);
  }

  private itemsOf(crate: string): Map<string, string> {
    let items = this.items.get(crate);
    if (!items) {
      const list = path.join(this.dir, crate, 'all.html');
      if (!fs.existsSync(list)) {
        throw new Error(
          `API links resolve against the rustdoc of this checkout, and ${list} is missing: ` +
            'run `cargo doc --all-features --no-deps` in the repository first',
        );
      }
      items = new Map(
        Array.from(fs.readFileSync(list, 'utf8').matchAll(ITEM), (m) => [m[2]!, m[1]!]),
      );
      this.items.set(crate, items);
    }
    return items;
  }
}

/** The URL of `target` (`crate::path::Item[::member][#section]`), or the reason it has none. */
function resolve(rustdoc: Rustdoc, docsRs: string, target: string): string | Error {
  const [itemPath, section] = target.split('#') as [string, string | undefined];
  const [crate, ...rest] = itemPath.split('::');
  if (!crate) {
    return new Error('the path names no crate');
  }
  let page = rustdoc.page(crate, rest);
  let anchor = section;
  if (!page && rest.length > 1 && !section) {
    // The last segment may be a member on the page of its type.
    const member = rest[rest.length - 1]!;
    const owner = rustdoc.page(crate, rest.slice(0, -1));
    const found = owner
      ? MEMBER_ANCHORS.map((kind) => `${kind}.${member}`).find((id) =>
          rustdoc.hasAnchor(crate, owner, id),
        )
      : undefined;
    if (owner && found) {
      page = owner;
      anchor = found;
    }
  }
  if (!page) {
    return new Error('the rustdoc of this version has no such item');
  }
  if (section && !rustdoc.hasAnchor(crate, page, section)) {
    return new Error(`the page of the item has no section '${section}'`);
  }
  return `${docsRs}${crate}/${page}${anchor ? `#${anchor}` : ''}`;
}

export default function remarkApiLinks({docsRs, rustdocDir}: Options) {
  const rustdoc = new Rustdoc(rustdocDir);
  return (tree: Root, file: VFile): void => {
    visit(tree, 'link', (node: Link) => {
      if (!node.url.startsWith(SCHEME)) {
        return;
      }
      const target = node.url.slice(SCHEME.length);
      const url = resolve(rustdoc, docsRs, target);
      if (url instanceof Error) {
        file.fail(`${file.path}: API link '${target}': ${url.message}`, node);
      }
      node.url = url;
    });
  };
}
