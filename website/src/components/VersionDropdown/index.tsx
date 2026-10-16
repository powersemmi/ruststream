import React, {type MouseEvent, type ReactNode} from 'react';
import clsx from 'clsx';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import {translate} from '@docusaurus/Translate';
import {currentEntry, versionRoot} from './versions';
import {openSamePageIn, usePublishedVersions} from './client';

interface Props {
  /** Set when the navbar renders the item in its mobile sidebar. */
  mobile?: boolean;
  position?: 'left' | 'right';
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
      void openSamePageIn(siteRoot, baseUrl, entry.version);
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
