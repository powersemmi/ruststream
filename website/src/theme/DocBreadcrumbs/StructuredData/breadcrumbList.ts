import {canonicalFor} from '../../SiteMetadata/canonical';

/** One step of a page's breadcrumb trail; a category without a page of its own has no link. */
export interface Breadcrumb {
  label: string;
  href?: string;
}

/** Where this build's pages are served and indexed; see `canonicalFor`. */
export interface Site {
  siteUrl: string;
  baseUrl: string;
  canonicalBaseUrl: string;
}

/**
 * The schema.org `BreadcrumbList` of a page: its linked steps at the addresses search engines
 * index. The site's pages end with a slash, and an address without one answers with a redirect.
 * Google accepts a list of two steps or more, so a page at the top of its trail gets none.
 */
export function breadcrumbList(breadcrumbs: Breadcrumb[], site: Site) {
  const linked = breadcrumbs.filter(
    (breadcrumb): breadcrumb is Required<Breadcrumb> => Boolean(breadcrumb.href),
  );
  if (linked.length < 2) {
    return undefined;
  }
  return {
    '@context': 'https://schema.org',
    '@type': 'BreadcrumbList',
    itemListElement: linked.map(({label, href}, index) => ({
      '@type': 'ListItem',
      position: index + 1,
      name: label,
      item: canonicalFor(
        `${site.siteUrl}${href.endsWith('/') ? href : `${href}/`}`,
        site.siteUrl,
        site.baseUrl,
        site.canonicalBaseUrl,
      ),
    })),
  };
}
