import React, {type ReactNode} from 'react';
import CodeBlock from '@theme/CodeBlock';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import Translate, {translate} from '@docusaurus/Translate';
import macros from '../../../../examples/quickstart.rs';
import manual from '../../../../examples/manual/quickstart.rs';
import {exampleSource} from './source';
import shared from './landing.module.css';
import styles from './cta.module.css';
import CodeLink from './CodeLink';

/** The core's quick start service, with the macros and written by hand, read from `examples/`. */
export default function Quickstart(): ReactNode {
  return (
    <div className={styles.quickstart}>
      <div className={shared.window}>
        <Tabs groupId="landing-quickstart" className={styles.tabs}>
          <TabItem
            value="macros"
            label={translate({id: 'landing.quickstart.macros', message: 'Macros'})}>
            <CodeBlock language="rust" title="examples/quickstart.rs">
              {exampleSource(macros)}
            </CodeBlock>
          </TabItem>
          <TabItem
            value="manual"
            label={translate({id: 'landing.quickstart.manual', message: 'Manual'})}>
            <CodeBlock language="rust" title="examples/manual/quickstart.rs">
              {exampleSource(manual)}
            </CodeBlock>
          </TabItem>
        </Tabs>
      </div>
      <p className={styles.quickstartNote}>
        <Translate
          id="landing.quickstart.note"
          values={{
            app: <CodeLink to="/components/application/app-macro/">#[ruststream::app]</CodeLink>,
            main: <CodeLink to="/components/application/app-macro/">main</CodeLink>,
            run: <CodeLink to="/components/application/running/run/">cargo run -- run</CodeLink>,
            gen: <CodeLink to="/components/asyncapi/generation/cli-gen/">cargo run -- asyncapi gen</CodeLink>,
          }}>
          {
            '{app} generates {main} with all the runtime boilerplate: {run} starts the service, and {gen} prints its AsyncAPI document.'
          }
        </Translate>
      </p>
    </div>
  );
}
