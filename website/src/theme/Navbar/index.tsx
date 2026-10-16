import React, {type ReactNode} from 'react';
import Navbar from '@theme-original/Navbar';
import type NavbarType from '@theme/Navbar';
import type {WrapperProps} from '@docusaurus/types';
import AskAiHost from '@site/src/components/AskAi/Host';

type Props = WrapperProps<typeof NavbarType>;

// Every page with a navbar, the landing page included, hosts the Ask AI panel.
export default function NavbarWrapper(props: Props): ReactNode {
  return (
    <>
      <Navbar {...props} />
      <AskAiHost />
    </>
  );
}
