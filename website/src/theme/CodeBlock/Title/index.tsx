import type {ReactNode} from 'react';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import type {Props} from '@theme/CodeBlock/Title';

// The snippet plugin titles an excerpt with its source file's address in the repository, at the
// ref the pages were built from (`main`, or an archive branch); the header shows the path and
// links to the whole compiled file.
export default function CodeBlockTitle({children}: Props): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  const sourceRoot = `${String(siteConfig.customFields?.sourceUrl ?? '')}/`;
  if (sourceRoot !== '/' && typeof children === 'string' && children.startsWith(sourceRoot)) {
    return <a href={children}>{children.slice(sourceRoot.length)}</a>;
  }
  return children;
}
