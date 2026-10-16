// The model behind the hero's dot matrix: where the brokers, the core and the handlers sit on a
// grid of cells, which cells each message walks, and the loop that draws it. Pure, so the
// drawing component stays a thin layer over the canvas.

export interface Cell {
  col: number;
  row: number;
}

export interface Box extends Cell {
  w: number;
  h: number;
}

export interface FlowLayout {
  cols: number;
  rows: number;
  brokers: Box[];
  core: Box;
  handlers: Box[];
  /** One route per broker, in broker order: the cells a message walks, one step at a time. */
  routes: Cell[][];
}

export interface Packet extends Cell {
  /** 1 for the message itself, less for the fading trail behind it. */
  level: number;
}

/** The moment the static frame shows; the moving frames start from it too. */
export const STATIC_TIME_MS = 2400;

const CELLS_PER_SECOND = 9;
const TRAIL = [0.55, 0.3, 0.12];

// Connects the corners with straight runs, one cell per step.
function walk(corners: Cell[]): Cell[] {
  const cells: Cell[] = [{...corners[0]!}];
  for (const corner of corners.slice(1)) {
    let {col, row} = cells[cells.length - 1]!;
    while (col !== corner.col || row !== corner.row) {
      if (col !== corner.col) col += Math.sign(corner.col - col);
      else row += Math.sign(corner.row - row);
      cells.push({col, row});
    }
  }
  return cells;
}

// Spreads `count` boxes of height `h` evenly over `rows`, top to bottom.
function column(count: number, col: number, w: number, h: number, top: number, rows: number) {
  return Array.from({length: count}, (_, index) => ({
    col,
    row: top + Math.round(((index + 0.5) * rows) / count - h / 2),
    w,
    h,
  }));
}

export function layoutFlow(cols: number, rows: number): FlowLayout {
  const w = Math.max(3, Math.round(cols * 0.1));
  const h = rows >= 30 ? 3 : 2;
  const brokers = column(rows >= 22 ? 5 : 3, 1, w, h, 0, rows);
  const handlers = column(3, cols - 1 - w, w, h, Math.round(rows * 0.1), Math.round(rows * 0.8));
  const coreTop = Math.round(rows * 0.18);
  const core = {
    col: Math.round(cols * 0.41),
    row: coreTop,
    w: Math.max(4, Math.round(cols * 0.18)),
    h: rows - 2 * coreTop,
  };
  const middle = core.row + Math.floor(core.h / 2);
  const inLeft = core.col - 1;
  const outRight = core.col + core.w;
  const routes = brokers.map((broker, index) => {
    const handler = handlers[index % handlers.length]!;
    const start = {col: broker.col + broker.w, row: broker.row + Math.floor(h / 2)};
    const target = {col: handler.col - 1, row: handler.row + Math.floor(h / 2)};
    // Each route takes its own lane on the way in, so the lanes read as separate wires.
    const laneIn = start.col + 2 + Math.round(((inLeft - start.col - 4) * index) / brokers.length);
    const laneOut = outRight + 2 + (index % handlers.length) * 2;
    const entry = core.row + 1 + Math.round(((core.h - 3) * index) / (brokers.length - 1));
    return walk([
      start,
      {col: laneIn, row: start.row},
      {col: laneIn, row: entry},
      {col: core.col + Math.floor(core.w / 2), row: entry},
      {col: core.col + Math.floor(core.w / 2), row: middle},
      {col: core.col + Math.floor(core.w / 2), row: middle + (index % 3) - 1},
      {col: laneOut, row: middle + (index % 3) - 1},
      {col: laneOut, row: target.row},
      target,
    ]);
  });
  return {cols, rows, brokers, core, handlers, routes};
}

export function packetsAt(layout: FlowLayout, timeMs: number): Packet[] {
  const travelled = (timeMs / 1000) * CELLS_PER_SECOND;
  return layout.routes.flatMap((route, index) => {
    const count = Math.max(1, Math.floor(route.length / 22));
    return Array.from({length: count}, (_, k) => {
      const offset = index * 5.3 + (k * route.length) / count;
      const head = Math.floor(travelled + offset) % route.length;
      const trail = TRAIL.map((level, step) => ({step: step + 1, level})).filter(
        ({step}) => head - step >= 0,
      );
      return [
        {...route[head]!, level: 1},
        ...trail.map(({step, level}) => ({...route[head - step]!, level})),
      ];
    }).flat();
  });
}

export interface FlowLoopOptions {
  reducedMotion: boolean;
  /** Draws the frame for an animation time, in milliseconds. */
  draw: (timeMs: number) => void;
  request: (callback: (now: number) => void) => number;
  cancel: (handle: number) => void;
  /** The shortest gap between two drawn frames; the default holds the canvas near 30 fps. */
  frameIntervalMs?: number;
}

export interface FlowLoop {
  /** Starts the animation when the canvas comes into view and stops it when it leaves. */
  setVisible: (visible: boolean) => void;
  /** Draws the current frame again, for a new palette or a new size. */
  redraw: () => void;
  dispose: () => void;
}

// The longest step a frame may take: a tab that wakes after a while resumes, it does not jump.
const MAX_STEP_MS = 100;

export function createFlowLoop(options: FlowLoopOptions): FlowLoop {
  const {reducedMotion, draw, request, cancel, frameIntervalMs = 30} = options;
  let elapsed = STATIC_TIME_MS;
  let lastNow: number | undefined;
  let sinceDraw = 0;
  let handle: number | undefined;
  let drawnStatic = false;
  let disposed = false;

  const tick = (now: number) => {
    if (lastNow !== undefined) {
      const step = Math.min(now - lastNow, MAX_STEP_MS);
      elapsed += step;
      sinceDraw += step;
      if (sinceDraw >= frameIntervalMs) {
        sinceDraw = 0;
        draw(elapsed);
      }
    }
    lastNow = now;
    handle = request(tick);
  };

  const stop = () => {
    if (handle !== undefined) cancel(handle);
    handle = undefined;
    lastNow = undefined;
  };

  return {
    setVisible(visible) {
      if (disposed) return;
      if (reducedMotion) {
        if (visible && !drawnStatic) {
          drawnStatic = true;
          draw(STATIC_TIME_MS);
        }
        return;
      }
      if (!visible) stop();
      else if (handle === undefined) handle = request(tick);
    },
    redraw() {
      if (disposed) return;
      drawnStatic = true;
      draw(elapsed);
    },
    dispose() {
      stop();
      disposed = true;
    },
  };
}
