import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * Settlement: what decides how a delivery is settled, from its decode to the dead letter. The
 * main column is the delivery's way; the right column holds the failure policies and the retry
 * path, so every branch leaves the main way sideways.
 */
export function settlement(words: Words): {boxes: Box[]; links: Link[]} {
  const main = (id: string, y: number, fields: Partial<Box> = {}): Box => ({
    id,
    x: 12,
    y,
    w: 150,
    h: 40,
    kind: 'step',
    lines: lines(words, id),
    ...fields,
  });
  const side = (id: string, y: number, fields: Partial<Box> = {}): Box => ({
    id,
    x: 198,
    y,
    w: 140,
    h: 40,
    kind: 'step',
    lines: lines(words, id),
    ...fields,
  });
  const outcome = (id: string, index: number, fields: Partial<Box> = {}): Box => ({
    id,
    x: index * 92,
    y: 262,
    w: 84,
    h: 44,
    lines: lines(words, id),
    ...fields,
  });
  const failures = '/components/subscribers/failure-policy/';
  const boxes: Box[] = [
    main('delivery', 0, {kind: 'broker'}),
    main('decode', 62, {href: '/components/subscribers/codecs/'}),
    main('handler', 124, {kind: 'service', href: '/components/subscribers/'}),
    main('outcome', 186, {kind: 'core', href: '/components/subscribers/messages/handler-outcome/'}),
    // Each policy sits level with the step that hands it the delivery, so the link runs straight.
    side('decodePolicy', 56, {h: 52, href: failures}),
    side('panicPolicy', 118, {h: 52, href: failures}),
    outcome('ack', 0, {kind: 'good'}),
    outcome('drop', 1, {kind: 'bad'}),
    outcome('retry', 2),
    outcome('retryAfter', 3, {href: '/components/subscribers/messages/retry-after/'}),
    {
      id: 'redeliver',
      x: 184,
      y: 336,
      w: 176,
      h: 48,
      kind: 'step',
      lines: lines(words, 'redeliver'),
      href: '/components/subscribers/retry-destination/',
    },
    {
      id: 'deadLetter',
      x: 184,
      y: 414,
      w: 176,
      h: 48,
      kind: 'bad',
      lines: lines(words, 'deadLetter'),
      href: '/components/subscribers/retry-cap/',
    },
    {
      id: 'after',
      x: 0,
      y: 336,
      w: 168,
      h: 48,
      kind: 'note',
      lines: lines(words, 'after'),
      href: '/components/subscribers/messages/and-after/',
    },
  ];
  const below = (x: number): number => (x - 184) / 176;
  const links: Link[] = [
    {from: 'delivery', to: 'decode'},
    {from: 'decode', to: 'handler', label: word(words, 'decoded')},
    {from: 'decode', to: 'decodePolicy'},
    {from: 'handler', to: 'outcome', label: word(words, 'returns')},
    {from: 'handler', to: 'panicPolicy'},
    {from: 'decodePolicy', to: 'outcome', exit: 'right', enter: 'right', dashed: true},
    {from: 'panicPolicy', to: 'outcome', exit: 'bottom', enter: 'right', dashed: true},
    {from: 'outcome', to: 'ack', enterAt: 0.5},
    {from: 'outcome', to: 'drop'},
    {from: 'outcome', to: 'retry'},
    {from: 'outcome', to: 'retryAfter'},
    {from: 'retry', to: 'redeliver', enterAt: below(226)},
    {from: 'retryAfter', to: 'redeliver', enterAt: below(318)},
    {from: 'redeliver', to: 'deadLetter', label: word(words, 'atCap')},
  ];
  return {boxes, links};
}
