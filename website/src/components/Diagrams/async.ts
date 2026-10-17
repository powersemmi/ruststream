import type {Lane} from './Flow';
import type {Box, Link} from './flow';
import {chain} from './chain';
import {lines, word, type Words} from './words';

/*
 * Async: the tasks a running app holds, one delivery's way through them, and the order a
 * shutdown takes them down in.
 */

const COLUMN = 104;
const columnX = (index: number): number => 12 + index * (COLUMN + 12);

/** The tasks of each dispatch form under the app runtime, and the dedicated threads beside it. */
export function asyncTopology(words: Words): {boxes: Box[]; links: Link[]} {
  const column = (index: number, id: string, y: number, h: number, fields: Partial<Box> = {}) => ({
    id,
    x: columnX(index),
    y,
    w: COLUMN,
    h,
    lines: lines(words, id),
    ...fields,
  });
  const tracker = {x: 12, w: 2 * COLUMN + 12};
  const enter = (index: number): number => (columnX(index) + COLUMN / 2 - tracker.x) / tracker.w;
  const boxes: Box[] = [
    {id: 'broker', x: 0, y: 0, w: 360, h: 34, kind: 'broker', lines: lines(words, 'broker')},
    {id: 'runtime', x: 0, y: 58, w: 360, h: 242, kind: 'group', lines: lines(words, 'runtime')},
    column(0, 'loop', 86, 58, {kind: 'core'}),
    column(1, 'pool', 86, 58, {kind: 'core', href: '/components/subscribers/dispatch/workers/'}),
    column(2, 'feeder', 86, 58, {kind: 'core', href: '/components/subscribers/dispatch/threads/'}),
    column(1, 'workerTasks', 172, 44, {kind: 'core'}),
    column(2, 'rings', 172, 44),
    {
      id: 'tracker',
      ...tracker,
      y: 244,
      h: 44,
      kind: 'core',
      lines: lines(words, 'tracker'),
      href: '/components/subscribers/messages/and-after/',
    },
    column(2, 'osThreads', 324, 58, {kind: 'core'}),
  ];
  const links: Link[] = [
    {from: 'broker', to: 'loop', label: word(words, 'poll')},
    {from: 'broker', to: 'pool'},
    {from: 'broker', to: 'feeder'},
    {from: 'pool', to: 'workerTasks', label: word(words, 'inbox')},
    {from: 'feeder', to: 'rings', label: word(words, 'push')},
    {from: 'rings', to: 'osThreads', label: word(words, 'pop')},
    {from: 'loop', to: 'tracker', enterAt: enter(0), label: word(words, 'spawn')},
    {from: 'workerTasks', to: 'tracker', enterAt: enter(1)},
  ];
  return {boxes, links};
}

const LANES = [60, 180, 300];

/** One delivery, step by step, in the column of the task that runs the step. */
export function asyncTimeline(words: Words): {boxes: Box[]; links: Link[]; lanes: Lane[]} {
  const step = (id: string, lane: number, row: number, fields: Partial<Box> = {}): Box => ({
    id,
    x: LANES[lane]! - 54,
    y: 30 + row * 46,
    w: 108,
    h: 34,
    lines: lines(words, id),
    kind: 'step',
    ...fields,
  });
  const boxes: Box[] = [
    step('pollStep', 0, 0, {kind: 'broker'}),
    step('context', 1, 1),
    step('layers', 1, 2, {href: '/components/subscribers/middleware/'}),
    step('decode', 1, 3, {href: '/components/subscribers/codecs/'}),
    step('body', 1, 4, {kind: 'service', href: '/components/subscribers/'}),
    step('reply', 1, 5, {href: '/components/subscribers/publishing/replies/'}),
    step('settle', 1, 6, {kind: 'core', href: '/architecture/settlement/'}),
    step('next', 0, 7, {kind: 'broker'}),
    step('after', 2, 7, {kind: 'core', href: '/components/subscribers/messages/and-after/'}),
  ];
  const links: Link[] = [
    {from: 'pollStep', to: 'context'},
    {from: 'context', to: 'layers'},
    {from: 'layers', to: 'decode'},
    {from: 'decode', to: 'body'},
    {from: 'body', to: 'reply'},
    {from: 'reply', to: 'settle'},
    {from: 'settle', to: 'next', exit: 'left', enter: 'top', label: word(words, 'nextTurn')},
    {
      from: 'settle',
      to: 'after',
      exit: 'right',
      enter: 'top',
      label: word(words, 'spawn'),
      dashed: true,
    },
  ];
  const lanes = ['loopLane', 'handlerLane', 'laterLane'].map((key, index) => ({
    x: LANES[index]!,
    label: word(words, key),
  }));
  return {boxes, links, lanes};
}

/** What a graceful shutdown does, in order. */
export function asyncShutdown(words: Words): {boxes: Box[]; links: Link[]} {
  const flow = chain(words, [
    {key: 'stop', kind: 'broker', href: '/components/application/running/shutdown/'},
    {key: 'onShutdown'},
    {key: 'cancel', kind: 'core'},
    {key: 'finish', kind: 'core'},
    {key: 'drain', kind: 'core'},
    {key: 'brokers', kind: 'broker', href: '/architecture/lifecycle/'},
    {key: 'afterShutdown'},
  ]);
  return flow;
}
