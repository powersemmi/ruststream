import type {ReactNode} from 'react';
import Admonition from '@theme/Admonition';
import Translate from '@docusaurus/Translate';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import {useDoc} from '@docusaurus/plugin-content-docs/client';

// A translated page lives in the locale's own tree; any other page a localized site serves is
// the English file, shown because the page has no translation yet.
const translationTree = '@site/i18n/';

/**
 * Tells the reader of a localized page how the text relates to the English original: a
 * translated page names English as normative and links to it; a page with no translation says
 * the text below is the English original.
 */
export default function TranslationBanner(): ReactNode {
  const {siteConfig, i18n} = useDocusaurusContext();
  const {metadata} = useDoc();
  if (i18n.currentLocale === i18n.defaultLocale) {
    return null;
  }
  if (!metadata.source.startsWith(translationTree)) {
    return (
      <Admonition
        type="info"
        title={
          <Translate id="banner.untranslated.title">
            This page is not translated yet
          </Translate>
        }>
        <p>
          <Translate id="banner.untranslated.body">The English original is shown below.</Translate>
        </p>
      </Admonition>
    );
  }
  // The localized site's base URL ends with the locale segment; the English page sits at the
  // same path without it. It is another build, so the link leaves the client-side router.
  const localeSegment = `${i18n.currentLocale}/`;
  const englishBase = siteConfig.baseUrl.endsWith(localeSegment)
    ? siteConfig.baseUrl.slice(0, -localeSegment.length)
    : siteConfig.baseUrl;
  const original = englishBase + metadata.permalink.slice(siteConfig.baseUrl.length);
  return (
    <Admonition
      type="info"
      title={
        <Translate id="banner.translated.title">English is the normative version</Translate>
      }>
      <p>
        <Translate
          id="banner.translated.body"
          values={{
            original: (
              <a href={original} target="_self">
                <Translate id="banner.translated.original">English original</Translate>
              </a>
            ),
          }}>
          {
            'This page was translated from English by a language model. Where the two differ, the {original} is correct.'
          }
        </Translate>
      </p>
    </Admonition>
  );
}
