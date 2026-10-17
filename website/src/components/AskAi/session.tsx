import React, {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from 'react';
import {AskAiUnavailable, streamAnswer, type ChatTurn} from './client';
import {CLOSE_EVENT, OPEN_EVENT, type OpenRequest} from './events';
import {loadOfficialWidget} from './fallback';

export interface AskAiSession {
  /** The panel has been asked for at least once on this page load. */
  started: boolean;
  open: boolean;
  turns: ChatTurn[];
  streaming: boolean;
  unavailable: boolean;
  ask: (question: string) => void;
  restart: () => void;
  close: () => void;
}

const SessionContext = createContext<AskAiSession | null>(null);

export function useAskAiSession(): AskAiSession {
  const session = useContext(SessionContext);
  if (!session) {
    throw new Error('useAskAiSession is used outside AskAiProvider');
  }
  return session;
}

// The conversation lives above the router, so it survives moving between pages while the
// panel itself is rendered inside the theme, where the code blocks of an answer get their
// colors.
export function AskAiProvider({children}: {children: ReactNode}): ReactNode {
  const [started, setStarted] = useState(false);
  const [open, setOpen] = useState(false);
  const [turns, setTurns] = useState<ChatTurn[]>([]);
  const [streaming, setStreaming] = useState(false);
  const [unavailable, setUnavailable] = useState(false);
  const abort = useRef<AbortController | null>(null);
  const history = useRef<ChatTurn[]>([]);

  const show = useCallback((next: ChatTurn[]) => {
    history.current = next;
    setTurns(next);
  }, []);

  const send = useCallback(
    async (question: string, before: ChatTurn[]) => {
      abort.current?.abort();
      const controller = new AbortController();
      abort.current = controller;
      const asked: ChatTurn[] = [...before, {role: 'user', text: question}];
      show([...asked, {role: 'assistant', text: ''}]);
      setStreaming(true);
      let answer = '';
      try {
        for await (const delta of streamAnswer(asked, {signal: controller.signal})) {
          answer += delta;
          show([...asked, {role: 'assistant', text: answer}]);
        }
      } catch (error) {
        if (error instanceof AskAiUnavailable) {
          show(asked);
          setUnavailable(true);
          // The widget takes a literal color; the site's palette names it once, in CSS.
          loadOfficialWidget(
            getComputedStyle(document.documentElement).getPropertyValue('--rs-brand').trim(),
          );
        }
      } finally {
        if (abort.current === controller) {
          setStreaming(false);
        }
      }
    },
    [show],
  );

  const restart = useCallback(() => {
    abort.current?.abort();
    show([]);
    setStreaming(false);
  }, [show]);

  useEffect(() => {
    const onOpen = (event: Event) => {
      const {query} = (event as CustomEvent<OpenRequest>).detail;
      setStarted(true);
      setOpen(true);
      if (query) {
        void send(query, []);
      }
    };
    const onClose = () => setOpen(false);
    window.addEventListener(OPEN_EVENT, onOpen);
    window.addEventListener(CLOSE_EVENT, onClose);
    return () => {
      window.removeEventListener(OPEN_EVENT, onOpen);
      window.removeEventListener(CLOSE_EVENT, onClose);
      abort.current?.abort();
    };
  }, [send]);

  const session = useMemo<AskAiSession>(
    () => ({
      started,
      open,
      turns,
      streaming,
      unavailable,
      ask: (question) => void send(question, history.current),
      restart,
      close: () => setOpen(false),
    }),
    [started, open, turns, streaming, unavailable, send, restart],
  );
  return <SessionContext.Provider value={session}>{children}</SessionContext.Provider>;
}
