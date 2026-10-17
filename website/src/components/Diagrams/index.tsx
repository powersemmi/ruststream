import type {ReactNode} from 'react';
import {asyncShutdown, asyncTimeline, asyncTopology} from './async';
import {design} from './design';
import {guarantees} from './guarantees';
import {lifecycle} from './lifecycle';
import {path} from './path';
import {settlement} from './settlement';
import {Flow, type Lane} from './Flow';
import type {Box, Link} from './flow';
import {word, type Words} from './words';

/*
 * The diagrams of the Architecture pages. A layout places the boxes; the page supplies every word,
 * so a translation changes the words and never the drawing.
 */

interface Layout {
  boxes: Box[];
  links: Link[];
  lanes?: Lane[];
}

const LAYOUTS = {
  design,
  path,
  asyncTopology,
  asyncTimeline,
  asyncShutdown,
  lifecycle,
  settlement,
  guarantees,
} satisfies Record<string, (words: Words) => Layout>;

export type DiagramName = keyof typeof LAYOUTS;

export function Diagram({name, words}: {name: DiagramName; words: Words}): ReactNode {
  const layout: Layout = LAYOUTS[name](words);
  return <Flow title={word(words, 'title')} text={word(words, 'text')} {...layout} />;
}
