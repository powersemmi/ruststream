import React, {type ReactNode} from 'react';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import shared from './landing.module.css';

type Target =
  /** A page of this site, site-relative (`/components/...`); the locale prefix comes from `Link`. */
  | {to: string}
  /** An item of the crate's rustdoc, relative to the docs.rs root of the version built. */
  | {api: string}
  /** Any other address. */
  | {href: string};

/** A code term of the landing copy that links to the page documenting it. */
export default function CodeLink(props: Target & {children: ReactNode}): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  const docsRs = siteConfig.customFields?.docsRs as string;
  const code = <code>{props.children}</code>;
  if ('to' in props) {
    return (
      <Link className={shared.codeLink} to={props.to}>
        {code}
      </Link>
    );
  }
  const href = 'api' in props ? `${docsRs}${props.api}` : props.href;
  return (
    <Link className={shared.codeLink} href={href}>
      {code}
    </Link>
  );
}
