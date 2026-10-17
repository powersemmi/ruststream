import {describe, expect, it} from 'vitest';
import {extent, route, textVersion, type Box} from './flow';

const box = (id: string, x: number, y: number, fields: Partial<Box> = {}): Box => ({
  id,
  x,
  y,
  w: 100,
  h: 40,
  lines: [id],
  ...fields,
});

describe('a route between two boxes', () => {
  it('drops straight down when the target is under the source', () => {
    expect(route(box('a', 0, 0), box('b', 0, 100), {from: 'a', to: 'b'}).d).toBe('M 50 40 V 100');
  });

  it('bends halfway down when the target is below and aside', () => {
    expect(route(box('a', 0, 0), box('b', 200, 100), {from: 'a', to: 'b'}).d).toBe(
      'M 50 40 V 70 H 250 V 100',
    );
  });

  it('runs sideways when the target is beside the source', () => {
    expect(route(box('a', 0, 0), box('b', 200, 0), {from: 'a', to: 'b'}).d).toBe('M 100 20 H 200');
  });

  it('climbs back up when the target is above', () => {
    expect(route(box('a', 0, 100), box('b', 0, 0), {from: 'a', to: 'b'}).d).toBe('M 50 100 V 40');
  });

  it('leaves and enters by the sides a link names', () => {
    const link = {from: 'a', to: 'b', exit: 'right', enter: 'right'} as const;
    expect(route(box('a', 0, 0), box('b', 0, 100), link).d).toBe('M 100 20 H 120 V 120 H 100');
  });

  it('meets a wide box where the link says, along its side', () => {
    const wide = box('b', 0, 100, {w: 400});
    expect(route(box('a', 0, 0), wide, {from: 'a', to: 'b', enterAt: 0.125}).d).toBe(
      'M 50 40 V 100',
    );
  });

  it('places its label beside the first stretch of the line', () => {
    const {label} = route(box('a', 0, 0), box('b', 0, 100), {from: 'a', to: 'b', label: 'x'});
    expect(label).toEqual({x: 56, y: 70, align: 'start'});
  });
});

it('centres a label over a sideways stretch', () => {
  const link = {from: 'a', to: 'b', label: 'x', exit: 'left', enter: 'top'} as const;
  expect(route(box('a', 200, 0), box('b', 0, 100), link).label).toEqual({
    x: 125,
    y: 14,
    align: 'middle',
  });
});

it('names a bending link over its stretch across', () => {
  const link = {from: 'a', to: 'b', label: 'x'};
  expect(route(box('a', 0, 0), box('b', 200, 100), link).label).toEqual({
    x: 150,
    y: 64,
    align: 'middle',
  });
});

describe('the drawing area', () => {
  it('reaches the lowest box with a margin', () => {
    expect(extent([box('a', 0, 0), box('b', 0, 300, {h: 60})])).toBe(368);
  });
});

describe('the text version', () => {
  it('reads every link as source, target and what passes', () => {
    const boxes = [
      box('a', 0, 0, {lines: ['Broker']}),
      box('b', 0, 100, {lines: ['Sub', 'stream']}),
    ];
    expect(
      textVersion(boxes, [
        {from: 'a', to: 'b', label: 'deliveries'},
        {from: 'b', to: 'a'},
      ]),
    ).toEqual(['Broker → Sub stream: deliveries', 'Sub stream → Broker']);
  });
});
