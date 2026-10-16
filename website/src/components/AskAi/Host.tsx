import React, {lazy, Suspense, type ReactNode} from 'react';
import {useAskAiSession} from './session';

// The panel and its Markdown renderer load on the first request to open it, so a page whose
// reader never asks carries none of that code.
const Panel = lazy(() => import('./Panel'));

export default function AskAiHost(): ReactNode {
  const {started, open} = useAskAiSession();
  if (!started) {
    return null;
  }
  return (
    <Suspense fallback={null}>
      <div hidden={!open}>
        <Panel />
      </div>
    </Suspense>
  );
}
