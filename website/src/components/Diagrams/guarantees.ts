import type {Box, Link} from './flow';
import {lines, type Words} from './words';

// The misuses the page draws, each with the error the compiler prints for it.
export const GUARANTEES = [
  'reply',
  'naming',
  'codec',
  'capability',
  'concurrency',
  'destination',
  'headers',
] as const;

/** What a service writes on the left, what the compiler answers on the right. */
export function guarantees(words: Words): {boxes: Box[]; links: Link[]} {
  const boxes: Box[] = [];
  const links: Link[] = [];
  GUARANTEES.forEach((key, index) => {
    const y = 28 + index * 66;
    boxes.push(
      {id: key, x: 0, y, w: 150, h: 52, kind: 'service', lines: lines(words, key)},
      {
        id: `${key}Error`,
        x: 178,
        y,
        w: 182,
        h: 52,
        kind: 'bad',
        lines: lines(words, `${key}Error`),
      },
    );
    links.push({from: key, to: `${key}Error`});
  });
  boxes.push(
    {id: 'writes', x: 0, y: 0, w: 150, h: 20, kind: 'note', lines: lines(words, 'writes')},
    {id: 'answers', x: 178, y: 0, w: 182, h: 20, kind: 'note', lines: lines(words, 'answers')},
  );
  return {boxes, links};
}
