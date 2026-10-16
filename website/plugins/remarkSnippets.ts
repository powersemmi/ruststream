import type {Code, Root} from 'mdast';
import type {Processor} from 'unified';
import type {VFile} from 'vfile';
import {visit} from 'unist-util-visit';
import remarkCodeRegion from 'remark-code-region';

/**
 * Fills code fences from compiled sources, so every excerpt on the site is code that builds.
 *
 * A fence names its source with `reference="<path>#<region>"` (the whole file without
 * `#<region>`); the path is relative to the repository root, and a region is the code between
 * `// --8<-- [start:<region>]` and `// --8<-- [end:<region>]`. `remark-code-region` reads and
 * dedents it and fails the build on a missing file or region. On top of it this plugin:
 *
 * - drops every other region marker, so a region that encloses others, or a whole file, shows
 *   none of them;
 * - keeps the lines written inside the fence in front of the excerpt (an import the excerpt
 *   needs but its source declares elsewhere);
 * - titles the block with the source file's address in the repository, which the code block
 *   title renders as a link, unless the page sets a title of its own.
 */
export interface Options {
  /** The repository root the paths are relative to. */
  rootDir: string;
  /** The address source paths are appended to, without a trailing slash. */
  repoUrl: string;
}

const REFERENCE = /(?:^|\s)reference="([^"#?]+)[^"]*"/;
const TITLE = /(?:^|\s)title=/;
const START = /^\s*\/\/ --8<-- \[start:([^\]]+)\]\s*$/;
const END = /^\s*\/\/ --8<-- \[end:([^\]]+)\]\s*$/;
const ANY_MARKER = /^\s*\/\/ --8<-- \[(?:start|end):[^\]]+\]\s*$/;

type Transformer = (tree: Root, file: VFile) => void;
type Plugin = (this: Processor, options: object) => Transformer;

export default function remarkSnippets(this: Processor, {rootDir, repoUrl}: Options): Transformer {
  const extract = (remarkCodeRegion as unknown as Plugin).call(this, {
    rootDir,
    regionMarkers: [{start: START, end: END}],
    // The only lines removed are markers; the default strip set would also drop the
    // assertions a compiled example shows on purpose.
    strip: [ANY_MARKER],
  });
  return (tree, file) => {
    const written = new Map<Code, string>();
    visit(tree, 'code', (node) => {
      const reference = node.meta ? REFERENCE.exec(node.meta) : null;
      if (!reference) {
        return;
      }
      if (node.value.trim()) {
        written.set(node, node.value);
      }
      node.value = '';
      if (!TITLE.test(node.meta!)) {
        node.meta = `${node.meta} title="${repoUrl}/${reference[1]}"`;
      }
    });
    extract(tree, file);
    for (const [node, lines] of written) {
      node.value = `${lines}\n${node.value}`;
    }
  };
}
