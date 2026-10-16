import React, {useEffect, useState, type MouseEvent, type ReactNode} from 'react';
import clsx from 'clsx';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import {translate} from '@docusaurus/Translate';
import {currentEntry, parseVersions, samePageIn, versionRoot, type VersionEntry} from './versions';

interface Props {
  /** Set when the navbar renders the item in its mobile sidebar. */
  mobile?: boolean;
  position?: 'left' | 'right';
}

// Each version is a separate build, so the list comes from the shared file at run time: an
// older build lists the versions released after it.
function usePublishedVersions(siteRoot: string): VersionEntry[] {
  const [versions, setVersions] = useState<VersionEntry[]>([]);
  useEffect(() => {
    let cancelled = false;
    fetch(`${siteRoot}versions.json`)
      .then((response) => (response.ok ? response.json() : []))
      .then((json: unknown) => {
        if (!cancelled) {
          setVersions(parseVersions(json));
        }
      })
      .catch(() => {
        // Without the file the dropdown shows only the version being read.
      });
    return () => {
      cancelled = true;
    };
  }, [siteRoot]);
  return versions;
}

// The same page may not exist in another version; asking first avoids landing on a 404.
async function navigateTo(siteRoot: string, baseUrl: string, version: string): Promise<void> {
  const {pathname, hash} = window.location;
  const target = samePageIn(siteRoot, baseUrl, pathname, version);
  let exists = false;
  try {
    exists = (await fetch(target, {method: 'HEAD'})).ok;
  } catch {
    // An unreachable page is treated like a missing one.
  }
  window.location.assign(exists ? target + hash : versionRoot(siteRoot, version));
}

export default function VersionDropdown({mobile, position}: Props): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  const siteRoot = String(siteConfig.customFields?.siteRoot ?? '/');
  const {baseUrl} = siteConfig;
  const versions = usePublishedVersions(siteRoot);
  const current = currentEntry(versions, siteRoot, baseUrl);
  const label = current?.title ?? baseUrl.slice(siteRoot.length).replace(/\/$/, '');

  const links = versions.map((entry) => {
    const href = versionRoot(siteRoot, entry.version);
    const onClick = (event: MouseEvent<HTMLAnchorElement>) => {
      event.preventDefault();
      void navigateTo(siteRoot, baseUrl, entry.version);
    };
    const active = entry.version === current?.version;
    const text =
      entry.aliases.length > 0 ? `${entry.title} (${entry.aliases.join(', ')})` : entry.title;
    return {href, onClick, active, text, key: entry.version};
  });

  const title = translate({id: 'versions.label', message: 'Version'});
  if (mobile) {
    return (
      <li className="menu__list-item">
        <span className="menu__link" aria-label={title}>
          {label}
        </span>
        <ul className="menu__list">
          {links.map((link) => (
            <li key={link.key} className="menu__list-item">
              <a
                className={clsx('menu__link', link.active && 'menu__link--active')}
                href={link.href}
                onClick={link.onClick}>
                {link.text}
              </a>
            </li>
          ))}
        </ul>
      </li>
    );
  }
  return (
    <div
      className={clsx('navbar__item', 'dropdown', 'dropdown--hoverable', {
        'dropdown--right': position === 'right',
      })}>
      <a
        className="navbar__link"
        href={versionRoot(siteRoot, current?.version ?? label)}
        aria-label={title}
        aria-haspopup="true"
        onClick={(event) => event.preventDefault()}>
        {label}
      </a>
      <ul className="dropdown__menu">
        {links.map((link) => (
          <li key={link.key}>
            <a
              className={clsx('dropdown__link', link.active && 'dropdown__link--active')}
              href={link.href}
              onClick={link.onClick}>
              {link.text}
            </a>
          </li>
        ))}
      </ul>
    </div>
  );
}
