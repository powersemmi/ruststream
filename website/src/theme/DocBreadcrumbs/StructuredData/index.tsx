import React, {type ReactNode} from 'react';
import Head from '@docusaurus/Head';
import type {Props} from '@theme/DocBreadcrumbs/StructuredData';
import {useCanonicalSite} from '@site/src/theme/SiteMetadata/useCanonical';
import {jsonLd} from '@site/src/components/Landing/structuredData';
import {breadcrumbList} from './breadcrumbList';

// Replaces the theme's component, which lists a trail of one step and the pages' addresses
// without their trailing slash; see `breadcrumbList`.
export default function DocBreadcrumbsStructuredData({breadcrumbs}: Props): ReactNode {
  const list = breadcrumbList(breadcrumbs, useCanonicalSite());
  if (!list) {
    return null;
  }
  return (
    <Head>
      <script type="application/ld+json">{jsonLd(list)}</script>
    </Head>
  );
}
