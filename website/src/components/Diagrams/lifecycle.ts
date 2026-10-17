import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * Lifecycle: the broker's states as types on the left, the app's steps on the right, and where
 * the app moves the broker from one state to the next.
 */
export function lifecycle(words: Words): {boxes: Box[]; links: Link[]} {
  const left = (id: string, y: number, h = 58): Box => ({
    id,
    x: 12,
    y,
    w: 156,
    h,
    kind: 'broker',
    code: true,
    lines: lines(words, id),
  });
  const right = (id: string, y: number, h = 58, fields: Partial<Box> = {}): Box => ({
    id,
    x: 192,
    y,
    w: 156,
    h,
    kind: 'core',
    lines: lines(words, id),
    ...fields,
  });
  const boxes: Box[] = [
    {
      id: 'brokerSide',
      x: 0,
      y: 0,
      w: 176,
      h: 330,
      kind: 'group',
      lines: lines(words, 'brokerSide'),
    },
    {id: 'appSide', x: 184, y: 0, w: 176, h: 330, kind: 'group', lines: lines(words, 'appSide')},
    left('unconnected', 28),
    left('connected', 138),
    left('closed', 248),
    right('builder', 28, 58, {kind: 'service', href: '/components/application/app-macro/'}),
    right('running', 138, 58, {href: '/components/application/running/start/'}),
    right('stopped', 248, 58, {href: '/components/application/running/shutdown/'}),
    {
      id: 'afterShutdown',
      x: 0,
      y: 352,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'afterShutdown'),
    },
  ];
  const links: Link[] = [
    {from: 'unconnected', to: 'connected', label: word(words, 'connect')},
    {from: 'connected', to: 'closed', label: word(words, 'shutdown')},
    {from: 'builder', to: 'running', label: word(words, 'start')},
    {from: 'running', to: 'stopped', label: word(words, 'stop')},
  ];
  return {boxes, links};
}
