import type {ReactNode} from 'react';
import type {Props} from '@theme/CodeBlock/Title';

// The snippet plugin titles an excerpt with its source file's address in the repository; the
// header shows the path and links to the whole compiled file.
const sourceRoot = 'https://github.com/powersemmi/ruststream/blob/main/';

export default function CodeBlockTitle({children}: Props): ReactNode {
  if (typeof children === 'string' && children.startsWith(sourceRoot)) {
    return <a href={children}>{children.slice(sourceRoot.length)}</a>;
  }
  return children;
}
