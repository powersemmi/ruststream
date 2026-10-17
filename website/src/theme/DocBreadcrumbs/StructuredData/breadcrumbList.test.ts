import {describe, expect, it} from 'vitest';
import {breadcrumbList} from './breadcrumbList';

const site = {
  siteUrl: 'https://powersemmi.github.io',
  baseUrl: '/ruststream/0.7/',
  canonicalBaseUrl: '/ruststream/latest/',
};

describe('breadcrumbList', () => {
  it('lists the trail at the addresses of the indexed copy, each with its trailing slash', () => {
    expect(
      breadcrumbList(
        [
          {label: 'Application', href: '/ruststream/0.7/components/application/'},
          {label: 'Testing'},
          {label: 'with', href: '/ruststream/0.7/components/application/testing/with'},
        ],
        site,
      ),
    ).toEqual({
      '@context': 'https://schema.org',
      '@type': 'BreadcrumbList',
      itemListElement: [
        {
          '@type': 'ListItem',
          position: 1,
          name: 'Application',
          item: 'https://powersemmi.github.io/ruststream/latest/components/application/',
        },
        {
          '@type': 'ListItem',
          position: 2,
          name: 'with',
          item: 'https://powersemmi.github.io/ruststream/latest/components/application/testing/with/',
        },
      ],
    });
  });

  it('gives no list for a trail of one linked page, which search engines refuse', () => {
    expect(
      breadcrumbList([{label: 'Tutorial', href: '/ruststream/0.7/guides/tutorial'}], site),
    ).toBeUndefined();
  });
});
