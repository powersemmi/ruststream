import type {Box, BoxKind, Link} from './flow';
import {lines, type Words} from './words';

export interface Step {
  key: string;
  kind?: BoxKind;
  code?: boolean;
  href?: string;
  // Lines the box takes, when it holds more than the default two.
  rows?: number;
}

/**
 * Steps one under another down the middle of the drawing, each linked to the next: the shape of
 * a sequence where nothing branches.
 */
export function chain(
  words: Words,
  steps: Step[],
  {x = 50, w = 260, top = 0, gap = 26}: {x?: number; w?: number; top?: number; gap?: number} = {},
): {boxes: Box[]; links: Link[]} {
  let y = top;
  const boxes = steps.map((step): Box => {
    const h = 14 + 15 * (step.rows ?? 2);
    const box: Box = {
      id: step.key,
      x,
      y,
      w,
      h,
      lines: lines(words, step.key),
      kind: step.kind ?? 'step',
      code: step.code,
      href: step.href,
    };
    y += h + gap;
    return box;
  });
  const links = boxes.slice(1).map((box, index) => ({from: boxes[index]!.id, to: box.id}));
  return {boxes, links};
}
