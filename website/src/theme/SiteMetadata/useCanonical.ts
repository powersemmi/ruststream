import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import {canonicalFor} from './canonical';

/**
 * Where this build's pages are served and indexed. A language's build has its own base URL
 * (`/ruststream/0.7/ru/`); the canonical mapping works on the base URL of the version, which the
 * site configuration carries for it.
 */
export function useCanonicalSite(): {siteUrl: string; baseUrl: string; canonicalBaseUrl: string} {
  const {
    siteConfig: {url: siteUrl, baseUrl, customFields},
  } = useDocusaurusContext();
  const buildBaseUrl = (customFields?.buildBaseUrl as string | undefined) ?? baseUrl;
  const canonicalBaseUrl = (customFields?.canonicalBaseUrl as string | undefined) ?? buildBaseUrl;
  return {siteUrl, baseUrl: buildBaseUrl, canonicalBaseUrl};
}

/** Maps an address of this build to the address of its indexed copy. */
export function useCanonical(): (url: string) => string {
  const {siteUrl, baseUrl, canonicalBaseUrl} = useCanonicalSite();
  return (url) => canonicalFor(url, siteUrl, baseUrl, canonicalBaseUrl);
}
