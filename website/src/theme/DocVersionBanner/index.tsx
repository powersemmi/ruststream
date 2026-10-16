import type {ReactNode} from 'react';
import clsx from 'clsx';
import {useLocation} from '@docusaurus/router';
import Translate from '@docusaurus/Translate';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import {ThemeClassNames} from '@docusaurus/theme-common';
import {useDocsVersion} from '@docusaurus/plugin-content-docs/client';
import type {Props} from '@theme/DocVersionBanner';
import {latestEntry, samePageIn} from '@site/src/components/VersionDropdown/versions';
import {
  openSamePageIn,
  usePublishedVersions,
} from '@site/src/components/VersionDropdown/client';

/**
 * The banner of a `next` or archive page, set by the build's kind of version.
 *
 * Each version is a separate build, so the theme's own banner, which links to the newest
 * version inside the same build, would link back to the page itself. This one links to the
 * same page under `latest`, or to the root of `latest` when the page is gone there, and names
 * the release `latest` points at once the versions file is read. The texts keep the theme's
 * translation ids, so the localized builds show the theme's translations.
 */
export default function DocVersionBanner({className}: Props): ReactNode {
  const version = useDocsVersion();
  const {siteConfig} = useDocusaurusContext();
  const {pathname} = useLocation();
  const siteRoot = String(siteConfig.customFields?.siteRoot ?? '/');
  const versions = usePublishedVersions(siteRoot);
  if (!version.banner) {
    return null;
  }
  const {baseUrl, title: siteTitle} = siteConfig;
  const values = {siteTitle, versionLabel: <b>{version.label}</b>};
  const target = samePageIn(siteRoot, baseUrl, pathname, 'latest');
  const latestLabel = latestEntry(versions)?.title ?? 'latest';
  return (
    <div
      className={clsx(
        className,
        ThemeClassNames.docs.docVersionBanner,
        'alert alert--warning margin-bottom--md',
      )}
      role="alert">
      <div>
        {version.banner === 'unreleased' ? (
          <Translate id="theme.docs.versions.unreleasedVersionLabel" values={values}>
            {'This is unreleased documentation for {siteTitle} {versionLabel} version.'}
          </Translate>
        ) : (
          <Translate id="theme.docs.versions.unmaintainedVersionLabel" values={values}>
            {
              'This is documentation for {siteTitle} {versionLabel}, which is no longer actively maintained.'
            }
          </Translate>
        )}
      </div>
      <div className="margin-top--md">
        <Translate
          id="theme.docs.versions.latestVersionSuggestionLabel"
          values={{
            versionLabel: latestLabel,
            latestVersionLink: (
              <b>
                <a
                  href={target}
                  onClick={(event) => {
                    event.preventDefault();
                    void openSamePageIn(siteRoot, baseUrl, 'latest');
                  }}>
                  <Translate id="theme.docs.versions.latestVersionLinkLabel">
                    latest version
                  </Translate>
                </a>
              </b>
            ),
          }}>
          {'For up-to-date documentation, see the {latestVersionLink} ({versionLabel}).'}
        </Translate>
      </div>
    </div>
  );
}
