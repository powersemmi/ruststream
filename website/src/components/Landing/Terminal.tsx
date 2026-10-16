import React, {type ReactNode, useCallback, useEffect, useRef, useState} from 'react';
import {translate} from '@docusaurus/Translate';
import shared from './landing.module.css';
import styles from './compare.module.css';

export interface TerminalLine {
  kind: 'command' | 'log' | 'note' | 'output';
  text: string;
}

interface Props {
  title: string;
  lines: TerminalLine[];
}

const STEP_MS = 420;

/**
 * A terminal that prints its lines one by one once it scrolls into view, with a button that
 * plays them again. Under `prefers-reduced-motion: reduce` every line is there from the start.
 */
export default function Terminal({title, lines}: Props): ReactNode {
  const [shown, setShown] = useState(lines.length);
  const [started, setStarted] = useState(false);
  const root = useRef<HTMLDivElement>(null);
  const timer = useRef<number | undefined>(undefined);

  const play = useCallback(() => {
    window.clearInterval(timer.current);
    if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
      setShown(lines.length);
      return;
    }
    setShown(0);
    timer.current = window.setInterval(() => {
      setShown((count) => {
        if (count + 1 >= lines.length) window.clearInterval(timer.current);
        return Math.min(count + 1, lines.length);
      });
    }, STEP_MS);
  }, [lines.length]);

  useEffect(() => {
    const element = root.current;
    if (!element || started) return undefined;
    const observer = new IntersectionObserver(([entry]) => {
      if (entry?.isIntersecting) {
        setStarted(true);
        play();
      }
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, [play, started]);

  useEffect(() => () => window.clearInterval(timer.current), []);

  return (
    <div ref={root} className={shared.window}>
      <div className={shared.windowBar}>
        <span className={shared.dots} aria-hidden="true">
          <span />
          <span />
          <span />
        </span>
        <span>{title}</span>
        <button
          type="button"
          className={styles.replay}
          onClick={play}
          aria-label={translate({
            id: 'landing.compare.replayLabel',
            message: 'Play the run again',
          })}>
          <svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true">
            <path
              d="M13 8a5 5 0 1 1-1.6-3.7M13 2.5v3h-3"
              fill="none"
              stroke="currentColor"
              strokeWidth="1.6"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
          {translate({id: 'landing.compare.replay', message: 'Replay'})}
        </button>
      </div>
      <pre className={styles.terminal} aria-live="off">
        {lines.map((line, index) => (
          <span
            key={index}
            className={styles[line.kind]}
            style={{visibility: index < shown ? 'visible' : 'hidden'}}>
            {line.kind === 'command' && <span className={styles.prompt}>$ </span>}
            {line.text}
            {'\n'}
          </span>
        ))}
      </pre>
    </div>
  );
}
