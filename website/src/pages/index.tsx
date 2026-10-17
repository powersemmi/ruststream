import React, {type ReactNode} from 'react';
import Head from '@docusaurus/Head';
import Layout from '@theme/Layout';
import {translate} from '@docusaurus/Translate';
import useBaseUrl from '@docusaurus/useBaseUrl';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Hero from '@site/src/components/Landing/Hero';
import BrokerStrip from '@site/src/components/Landing/BrokerStrip';
import OldWayNewWay from '@site/src/components/Landing/OldWayNewWay';
import Architecture from '@site/src/components/Landing/Architecture';
import Principles from '@site/src/components/Landing/Principles';
import Numbers from '@site/src/components/Landing/Numbers';
import CallToAction from '@site/src/components/Landing/CallToAction';
import {jsonLd, softwareSourceCode} from '@site/src/components/Landing/structuredData';
import {useCanonical} from '@site/src/theme/SiteMetadata/useCanonical';
import styles from './index.module.css';

export default function Home(): ReactNode {
  const description = translate({
    id: 'landing.meta.description',
    message:
      'RustStream is an async messaging framework for Rust: #[subscriber] handlers over a broker-agnostic core, fourteen transports, AsyncAPI and in-process tests.',
  });
  const canonical = useCanonical();
  const {siteConfig} = useDocusaurusContext();
  const url = canonical(`${siteConfig.url}${useBaseUrl('/')}`);
  return (
    <Layout
      title={translate({id: 'landing.meta.title', message: 'Async messaging for Rust'})}
      description={description}>
      <Head>
        <script type="application/ld+json">
          {jsonLd(softwareSourceCode({description, url}))}
        </script>
      </Head>
      <main className={styles.landing}>
        <div className={styles.frame}>
          <Hero />
          <BrokerStrip />
          <OldWayNewWay />
          <Architecture />
          <Principles />
          <Numbers />
          <CallToAction />
        </div>
      </main>
    </Layout>
  );
}
