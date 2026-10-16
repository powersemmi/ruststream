import React, {type ReactNode} from 'react';
import Layout from '@theme/Layout';
import {translate} from '@docusaurus/Translate';
import Hero from '@site/src/components/Landing/Hero';
import BrokerStrip from '@site/src/components/Landing/BrokerStrip';
import styles from './index.module.css';

export default function Home(): ReactNode {
  return (
    <Layout
      title={translate({id: 'landing.meta.title', message: 'Async messaging for Rust'})}
      description={translate({
        id: 'landing.meta.description',
        message:
          'RustStream is an async messaging framework for Rust: #[subscriber] handlers over a broker-agnostic core, thirteen broker crates, AsyncAPI generation and in-process tests.',
      })}>
      <main className={styles.landing}>
        <div className={styles.frame}>
          <Hero />
          <BrokerStrip />
        </div>
      </main>
    </Layout>
  );
}
