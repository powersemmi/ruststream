import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * Settlement: what decides how a delivery is settled, from its decode to the dead letter. The
 * main column is the delivery's way and the right column holds the failure policies, so every
 * policy leaves the main way sideways. Under the outcomes the two retries meet the attempt cap,
 * whose three answers are alternatives side by side.
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
  const branch = (id: string, index: number, fields: Partial<Box> = {}): Box => ({
    id,
    x: index * 124,
    y: 396,
    w: 112,
    h: 72,
    kind: 'step',
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
    // The cap is read before a retry, and each of its answers is a branch of its own.
    {
      id: 'capCheck',
      x: 92,
      y: 330,
      w: 268,
      h: 36,
      kind: 'core',
      lines: lines(words, 'capCheck'),
      href: '/components/subscribers/retry-cap/',
    },
    branch('belowCap', 0, {href: '/components/subscribers/retry-destination/'}),
    branch('deadLetter', 1, {kind: 'bad', href: '/components/subscribers/retry-cap/'}),
    branch('reject', 2, {kind: 'bad'}),
    {
      id: 'after',
      x: 0,
      y: 492,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'after'),
      href: '/components/subscribers/messages/and-after/',
    },
  ];
  // Where a column's centre meets the cap check, so the links into and out of it run straight.
  const along = (x: number): number => (x - 92) / 268;
  const links: Link[] = [
    {from: 'delivery', to: 'decode'},
    {from: 'decode', to: 'handler', label: word(words, 'decoded')},
    {from: 'decode', to: 'decodePolicy'},
    {from: 'handler', to: 'outcome', label: word(words, 'returns')},
    {from: 'handler', to: 'panicPolicy'},
    {from: 'decodePolicy', to: 'outcome', exit: 'right', enter: 'right', dashed: true},
    // The default `fail_fast` stops the service with the delivery unsettled, so only the other
    // panic policies reach an outcome.
    {
      from: 'panicPolicy',
      to: 'outcome',
      exit: 'bottom',
      enter: 'right',
      dashed: true,
      label: word(words, 'otherPolicies'),
    },
    {from: 'outcome', to: 'ack', enterAt: 0.5},
    {from: 'outcome', to: 'drop'},
    {from: 'outcome', to: 'retry'},
    {from: 'outcome', to: 'retryAfter'},
    {from: 'retry', to: 'capCheck', enterAt: along(226)},
    {from: 'retryAfter', to: 'capCheck', enterAt: along(318)},
    {from: 'capCheck', to: 'belowCap', exit: 'left', enter: 'top'},
    {from: 'capCheck', to: 'deadLetter', exitAt: along(180)},
    {from: 'capCheck', to: 'reject', exitAt: along(304)},
  ];
  return {boxes, links};
}
