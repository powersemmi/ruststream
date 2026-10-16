import {forwardRef, useImperativeHandle, type ReactNode} from 'react';
import {closeAskAi, openAskAi} from './events';

// The search plugin renders this in place of its default Ask AI widget and calls these
// methods when the reader picks "Ask AI" in the results; the site's panel answers instead.
export interface AskAIWidgetRef {
  open: () => void;
  close: () => void;
  openWithNewSession: (message: string) => void;
}

export const AskAIWidget = forwardRef<AskAIWidgetRef, {children?: ReactNode}>(
  function AskAIWidget(_props, ref) {
    useImperativeHandle(ref, () => ({
      open: () => openAskAi(),
      close: closeAskAi,
      openWithNewSession: (message) => openAskAi(message),
    }));
    return null;
  },
);
