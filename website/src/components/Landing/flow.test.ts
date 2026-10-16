import {describe, expect, it} from 'vitest';
import {createFlowLoop, layoutFlow, packetsAt, STATIC_TIME_MS, type FlowLayout} from './flow';

const inside = (layout: FlowLayout, col: number, row: number) =>
  col >= 0 && col < layout.cols && row >= 0 && row < layout.rows;

const touches = (box: {col: number; row: number; w: number; h: number}, col: number, row: number) =>
  col >= box.col - 1 && col <= box.col + box.w && row >= box.row && row < box.row + box.h;

describe('layoutFlow', () => {
  it('runs one route from every broker through the core to a handler', () => {
    const layout = layoutFlow(64, 40);
    expect(layout.routes).toHaveLength(layout.brokers.length);
    layout.routes.forEach((route, index) => {
      const first = route[0]!;
      const last = route[route.length - 1]!;
      expect(touches(layout.brokers[index]!, first.col, first.row)).toBe(true);
      expect(layout.handlers.some((handler) => touches(handler, last.col, last.row))).toBe(true);
      expect(route.some((cell) => touches(layout.core, cell.col, cell.row))).toBe(true);
      expect(route.every((cell) => inside(layout, cell.col, cell.row))).toBe(true);
    });
  });

  it('keeps every step of a route one cell from the previous one', () => {
    for (const route of layoutFlow(40, 26).routes) {
      route.slice(1).forEach((cell, index) => {
        const previous = route[index]!;
        expect(Math.abs(cell.col - previous.col) + Math.abs(cell.row - previous.row)).toBe(1);
      });
    }
  });
});

describe('packetsAt', () => {
  it('puts a message on every route in the static frame', () => {
    const layout = layoutFlow(64, 40);
    const heads = packetsAt(layout, STATIC_TIME_MS).filter((packet) => packet.level === 1);
    layout.routes.forEach((route) => {
      expect(
        heads.some((head) => route.some((c) => c.col === head.col && c.row === head.row)),
      ).toBe(true);
    });
  });

  it('moves the messages as time passes', () => {
    const layout = layoutFlow(64, 40);
    expect(packetsAt(layout, 1000)).not.toEqual(packetsAt(layout, 1500));
  });
});

// A scheduler the test drives by hand, standing in for `requestAnimationFrame`.
function fakeScheduler() {
  let next = 1;
  const pending = new Map<number, (now: number) => void>();
  return {
    pending,
    request: (callback: (now: number) => void) => {
      pending.set(next, callback);
      return next++;
    },
    cancel: (handle: number) => {
      pending.delete(handle);
    },
    tick(now: number) {
      const callbacks = [...pending.values()];
      pending.clear();
      callbacks.forEach((callback) => callback(now));
    },
  };
}

describe('createFlowLoop', () => {
  it('draws one static frame under reduced motion and never schedules another', () => {
    const scheduler = fakeScheduler();
    const drawn: number[] = [];
    const loop = createFlowLoop({reducedMotion: true, draw: (t) => drawn.push(t), ...scheduler});
    loop.setVisible(true);
    loop.setVisible(false);
    loop.setVisible(true);
    expect(drawn).toEqual([STATIC_TIME_MS]);
    expect(scheduler.pending.size).toBe(0);
  });

  it('redraws the same frame when the theme changes, moving or not', () => {
    const scheduler = fakeScheduler();
    const drawn: number[] = [];
    const loop = createFlowLoop({reducedMotion: true, draw: (t) => drawn.push(t), ...scheduler});
    loop.redraw();
    expect(drawn).toEqual([STATIC_TIME_MS]);
  });

  it('animates while visible and stops when hidden, without jumping on return', () => {
    const scheduler = fakeScheduler();
    const drawn: number[] = [];
    const loop = createFlowLoop({reducedMotion: false, draw: (t) => drawn.push(t), ...scheduler});
    loop.setVisible(true);
    scheduler.tick(1000);
    scheduler.tick(1040);
    scheduler.tick(1080);
    expect(drawn).toEqual([STATIC_TIME_MS + 40, STATIC_TIME_MS + 80]);

    loop.setVisible(false);
    expect(scheduler.pending.size).toBe(0);

    loop.setVisible(true);
    scheduler.tick(60_000);
    scheduler.tick(60_040);
    expect(drawn.at(-1)).toBe(STATIC_TIME_MS + 120);
  });

  it('stops for good once disposed', () => {
    const scheduler = fakeScheduler();
    const loop = createFlowLoop({reducedMotion: false, draw: () => {}, ...scheduler});
    loop.setVisible(true);
    loop.dispose();
    loop.setVisible(true);
    expect(scheduler.pending.size).toBe(0);
  });
});
