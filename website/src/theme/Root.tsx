import React, {type ReactNode} from 'react';
import {AskAiProvider} from '@site/src/components/AskAi/session';

export default function Root({children}: {children: ReactNode}): ReactNode {
  return <AskAiProvider>{children}</AskAiProvider>;
}
