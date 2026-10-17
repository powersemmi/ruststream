/*
 * The geometry of the architecture diagrams. A diagram is a list of boxes placed on a fixed
 * drawing area and the links between them; these functions turn that into SVG paths and into the
 * text version a screen reader reads, so both come from one description and cannot disagree.
 */

export type Side = 'top' | 'bottom' | 'left' | 'right';

/**
 * What a box stands for, which picks its colours: the service's code, the core, a broker crate or
 * the broker itself, a step of a flow, an outcome, a frame around other boxes, or a note.
 */
export type BoxKind = 'service' | 'core' | 'broker' | 'step' | 'good' | 'bad' | 'group' | 'note';

export interface Box {
  id: string;
  x: number;
  y: number;
  w: number;
  h: number;
  // Lines are broken by the author: a translation breaks them where its own language reads well.
  lines: string[];
  kind?: BoxKind;
  // Lines set in the code font: type and method names.
  code?: boolean;
  // The page that explains the box.
  href?: string;
}

export interface Link {
  from: string;
  to: string;
  label?: string;
  dashed?: boolean;
  exit?: Side;
  enter?: Side;
  // Where along its side the link leaves or meets a box, from 0 to 1: a wide box takes links
  // from several columns, each straight above its own column.
  exitAt?: number;
  enterAt?: number;
}

export interface Route {
  d: string;
  // A label beside a vertical stretch starts at the line; one above a horizontal stretch is centred.
  label?: {x: number; y: number; align: 'start' | 'middle'};
}

// How far a link that leaves and enters on the same side stands off the boxes.
const STANDOFF = 20;
// Space between a label and the line it names.
const GAP = 6;
// Space under the lowest box, so its outline is not cut by the edge of the drawing.
const MARGIN = 8;

function anchor(box: Box, side: Side, along = 0.5): {x: number; y: number} {
  switch (side) {
    case 'top':
      return {x: box.x + box.w * along, y: box.y};
    case 'bottom':
      return {x: box.x + box.w * along, y: box.y + box.h};
    case 'left':
      return {x: box.x, y: box.y + box.h * along};
    case 'right':
      return {x: box.x + box.w, y: box.y + box.h * along};
  }
}

// The sides a link takes when it names none: down to a box below, up to a box above, else across.
function sides(from: Box, to: Box): [Side, Side] {
  if (to.y >= from.y + from.h) {
    return ['bottom', 'top'];
  }
  if (to.y + to.h <= from.y) {
    return ['top', 'bottom'];
  }
  return to.x >= from.x + from.w ? ['right', 'left'] : ['left', 'right'];
}

const vertical = (side: Side): boolean => side === 'top' || side === 'bottom';

/** The path of a link: straight where the two anchors line up, else with right-angle bends. */
export function route(from: Box, to: Box, link: Link): Route {
  const [exit, enter] = link.exit && link.enter ? [link.exit, link.enter] : sides(from, to);
  const a = anchor(from, exit, link.exitAt);
  const b = anchor(to, enter, link.enterAt);
  let d: string;
  let label: {x: number; y: number};
  let align: 'start' | 'middle' = 'start';
  if (vertical(exit) && vertical(enter)) {
    const middle = exit === enter ? Math.max(a.y, b.y) + STANDOFF : (a.y + b.y) / 2;
    d =
      a.x === b.x && exit !== enter
        ? `M ${a.x} ${a.y} V ${b.y}`
        : `M ${a.x} ${a.y} V ${middle} H ${b.x} V ${b.y}`;
    if (a.x === b.x) {
      label = {x: a.x + GAP, y: (a.y + b.y) / 2};
    } else {
      // A bend names its link over the stretch across, where the label has room.
      label = {x: (a.x + b.x) / 2, y: middle - GAP};
      align = 'middle';
    }
  } else if (!vertical(exit) && !vertical(enter)) {
    const middle =
      exit === enter
        ? exit === 'right'
          ? Math.max(a.x, b.x) + STANDOFF
          : Math.min(a.x, b.x) - STANDOFF
        : (a.x + b.x) / 2;
    d =
      a.y === b.y && exit !== enter
        ? `M ${a.x} ${a.y} H ${b.x}`
        : `M ${a.x} ${a.y} H ${middle} V ${b.y} H ${b.x}`;
    label = {x: (a.x + (a.y === b.y ? b.x : middle)) / 2, y: a.y - GAP};
    align = 'middle';
  } else if (vertical(exit)) {
    d = `M ${a.x} ${a.y} V ${b.y} H ${b.x}`;
    label = {x: a.x + GAP, y: (a.y + b.y) / 2};
  } else {
    d = `M ${a.x} ${a.y} H ${b.x} V ${b.y}`;
    label = {x: (a.x + b.x) / 2, y: a.y - GAP};
    align = 'middle';
  }
  return link.label === undefined ? {d} : {d, label: {...label, align}};
}

/** The height of the drawing: the lowest box, with a margin. */
export const extent = (boxes: Box[]): number =>
  Math.max(...boxes.map((box) => box.y + box.h)) + MARGIN;

const name = (box: Box | undefined): string => box?.lines.join(' ') ?? '';

/** A column of a timeline: a header with a guide line down the drawing, naming who runs a step. */
export interface Lane {
  x: number;
  label: string;
}

const inside = (outer: Box, inner: Box): boolean =>
  inner.x >= outer.x &&
  inner.y >= outer.y &&
  inner.x + inner.w <= outer.x + outer.w &&
  inner.y + inner.h <= outer.y + outer.h;

/**
 * The diagram read aloud, with everything the picture shows: each frame and the boxes inside it,
 * each column of a timeline and the steps it runs, every link as its source, its target and what
 * passes along it, and last every box that none of these has named. A list is joined by
 * semicolons, because a box's own words may hold commas.
 */
export function textVersion(boxes: Box[], links: Link[], lanes: Lane[] = []): string[] {
  const byId = new Map(boxes.map((box) => [box.id, box]));
  const named = new Set<string>();
  const list = (title: string, members: Box[]): string => {
    members.forEach((box) => named.add(box.id));
    return `${title}: ${members.map(name).join('; ')}`;
  };
  const frames = boxes.filter((box) => box.kind === 'group');
  const contents = boxes.filter((box) => box.kind !== 'group');
  const framed = frames.map((frame) =>
    list(
      name(frame),
      contents.filter((box) => inside(frame, box)),
    ),
  );
  const columns = lanes.map((lane) =>
    list(
      lane.label,
      contents.filter((box) => box.x + box.w / 2 === lane.x),
    ),
  );
  const arrows = links.map((link) => {
    named.add(link.from).add(link.to);
    const line = `${name(byId.get(link.from))} → ${name(byId.get(link.to))}`;
    return link.label ? `${line}: ${link.label}` : line;
  });
  const alone = contents.filter((box) => !named.has(box.id)).map(name);
  return [...framed, ...columns, ...arrows, ...alone];
}
