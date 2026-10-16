import React, {type ReactNode, useEffect, useRef} from 'react';
import {createFlowLoop, layoutFlow, packetsAt, type FlowLayout} from './flow';
import styles from './hero.module.css';

/** The side of one cell of the dot matrix, in CSS pixels. */
const CELL = 8;

interface Palette {
  dot: string;
  wire: string;
  node: string;
  core: string;
  packet: string;
}

function readPalette(element: Element): Palette {
  const style = getComputedStyle(element);
  const read = (name: string) => style.getPropertyValue(name).trim();
  return {
    dot: read('--rs-land-dot'),
    wire: read('--rs-land-wire'),
    node: read('--rs-land-node'),
    core: read('--rs-land-core'),
    packet: read('--rs-land-packet'),
  };
}

const within = (box: {col: number; row: number; w: number; h: number}, col: number, row: number) =>
  col >= box.col && col < box.col + box.w && row >= box.row && row < box.row + box.h;

// Paints what does not move (the grid, the wires, the boxes) once per size and palette, so a
// frame is one image copy plus the messages.
function paintStatic(target: HTMLCanvasElement, layout: FlowLayout, palette: Palette, dpr: number) {
  const ctx = target.getContext('2d')!;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, target.width, target.height);
  const wires = new Set(layout.routes.flat().map(({col, row}) => `${col}:${row}`));
  const square = (col: number, row: number, size: number) =>
    ctx.fillRect(col * CELL + (CELL - size) / 2, row * CELL + (CELL - size) / 2, size, size);
  for (let row = 0; row < layout.rows; row++) {
    for (let col = 0; col < layout.cols; col++) {
      if (within(layout.core, col, row)) {
        const edge =
          col === layout.core.col ||
          row === layout.core.row ||
          col === layout.core.col + layout.core.w - 1 ||
          row === layout.core.row + layout.core.h - 1;
        ctx.fillStyle = palette.core;
        ctx.globalAlpha = edge ? 0.95 : 0.4;
        square(col, row, edge ? 3.5 : 2.5);
      } else if ([...layout.brokers, ...layout.handlers].some((box) => within(box, col, row))) {
        ctx.fillStyle = palette.node;
        ctx.globalAlpha = 0.85;
        square(col, row, 4);
      } else if (wires.has(`${col}:${row}`)) {
        ctx.fillStyle = palette.wire;
        ctx.globalAlpha = 1;
        square(col, row, 3);
      } else {
        ctx.fillStyle = palette.dot;
        ctx.globalAlpha = 1;
        square(col, row, 2);
      }
    }
  }
  ctx.globalAlpha = 1;
}

interface Props {
  label: string;
}

/**
 * The hero's dot matrix: messages leave the brokers on the left, pass through the core and
 * reach the handlers on the right. It animates only while on screen, draws one still frame
 * under `prefers-reduced-motion: reduce`, and repaints in the new colors when the theme changes.
 */
export default function FlowCanvas({label}: Props): ReactNode {
  const ref = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = ref.current;
    const ctx = canvas?.getContext('2d');
    if (!canvas || !ctx) return undefined;
    const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    const still = document.createElement('canvas');
    let palette = readPalette(canvas);
    let layout = layoutFlow(1, 1);
    let dpr = 1;

    const draw = (timeMs: number) => {
      ctx.setTransform(1, 0, 0, 1, 0, 0);
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      ctx.drawImage(still, 0, 0);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.fillStyle = palette.packet;
      for (const {col, row, level} of packetsAt(layout, timeMs)) {
        const size = level === 1 ? 6 : 4;
        ctx.globalAlpha = level;
        ctx.fillRect(col * CELL + (CELL - size) / 2, row * CELL + (CELL - size) / 2, size, size);
      }
      ctx.globalAlpha = 1;
      canvas.dataset.frame = String(Math.round(timeMs));
    };

    const resize = () => {
      const {width, height} = canvas.getBoundingClientRect();
      dpr = Math.min(window.devicePixelRatio || 1, 2);
      for (const target of [canvas, still]) {
        target.width = Math.round(width * dpr);
        target.height = Math.round(height * dpr);
      }
      layout = layoutFlow(
        Math.max(24, Math.floor(width / CELL)),
        Math.max(14, Math.floor(height / CELL)),
      );
      paintStatic(still, layout, palette, dpr);
    };

    const loop = createFlowLoop({
      reducedMotion,
      draw,
      request: (callback) => window.requestAnimationFrame(callback),
      cancel: (handle) => window.cancelAnimationFrame(handle),
    });

    const visibility = new IntersectionObserver(([entry]) =>
      loop.setVisible(!!entry?.isIntersecting),
    );
    const size = new ResizeObserver(() => {
      resize();
      loop.redraw();
    });
    const theme = new MutationObserver(() => {
      palette = readPalette(canvas);
      paintStatic(still, layout, palette, dpr);
      loop.redraw();
    });

    resize();
    loop.redraw();
    size.observe(canvas);
    visibility.observe(canvas);
    theme.observe(document.documentElement, {attributes: true, attributeFilter: ['data-theme']});
    return () => {
      loop.dispose();
      visibility.disconnect();
      size.disconnect();
      theme.disconnect();
    };
  }, []);

  return <canvas ref={ref} className={styles.canvas} role="img" aria-label={label} />;
}
