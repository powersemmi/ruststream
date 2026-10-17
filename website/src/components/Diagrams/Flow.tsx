import {useId, type ReactNode} from 'react';
import Link from '@docusaurus/Link';
import {extent, route, textVersion, type Box, type Lane, type Link as Arrow} from './flow';
import styles from './styles.module.css';

/*
 * One architecture diagram: boxes and the links between them, drawn as SVG on a narrow drawing
 * area that a phone shows at full width and a desktop at a readable size, and the same diagram
 * as text under it. A box that has a page of its own links to it.
 */

// The drawing area is phone-first: 360 units read at 390 px without shrinking the text.
const WIDTH = 360;
const LINE = 15;

export type {Lane};

export interface FlowProps {
  title: string;
  boxes: Box[];
  links: Arrow[];
  // Column headers with a guide line down the drawing: who runs each step of a timeline.
  lanes?: Lane[];
  // The words of the summary that opens the text version.
  text: string;
  width?: number;
}

const KIND_CLASS: Record<NonNullable<Box['kind']>, string | undefined> = {
  service: styles.service,
  core: styles.core,
  broker: styles.broker,
  step: styles.step,
  good: styles.good,
  bad: styles.bad,
  group: styles.group,
  note: styles.note,
};

// A frame is drawn in two parts: its outline under the links, its name over them, so a link
// that crosses into the frame passes behind the name instead of striking it through.
function BoxShape({
  box,
  part = 'whole',
}: {
  box: Box;
  part?: 'whole' | 'outline' | 'name';
}): ReactNode {
  const kind = box.kind ?? 'step';
  const group = kind === 'group';
  // A frame names its contents from its top corner; every other box centres its lines.
  const first = group ? box.y + LINE : box.y + box.h / 2 - ((box.lines.length - 1) * LINE) / 2;
  const shape = (
    <g className={`${styles.box} ${KIND_CLASS[kind]} ${box.code ? styles.code : ''}`}>
      {part !== 'name' && (
        <rect x={box.x} y={box.y} width={box.w} height={box.h} rx={group ? 8 : 4} />
      )}
      {part !== 'outline' && (
        <text
          x={group ? box.x + 8 : box.x + box.w / 2}
          y={first}
          textAnchor={group ? 'start' : 'middle'}
          dominantBaseline="middle">
          {box.lines.map((line, index) => (
            <tspan
              key={index}
              x={group ? box.x + 8 : box.x + box.w / 2}
              dy={index === 0 ? 0 : LINE}>
              {line}
            </tspan>
          ))}
        </text>
      )}
    </g>
  );
  return box.href ? (
    <Link to={box.href} className={styles.target} aria-label={box.lines.join(' ')}>
      {shape}
    </Link>
  ) : (
    shape
  );
}

export function Flow({title, boxes, links, lanes, text, width = WIDTH}: FlowProps): ReactNode {
  const id = useId();
  const byId = new Map(boxes.map((box) => [box.id, box]));
  const height = extent(boxes);
  const arrow = `${id}-arrow`;
  return (
    <figure className={styles.figure}>
      <svg
        className={styles.diagram}
        viewBox={`0 0 ${width} ${height}`}
        style={{maxWidth: `${width * 1.6}px`}}
        role="group"
        aria-labelledby={`${id}-title`}>
        <title id={`${id}-title`}>{title}</title>
        <defs>
          <marker
            id={arrow}
            viewBox="0 0 10 10"
            refX="9"
            refY="5"
            markerWidth="7"
            markerHeight="7"
            orient="auto-start-reverse">
            <path d="M 0 1 L 9 5 L 0 9 z" className={styles.head} />
          </marker>
        </defs>
        {lanes?.map((lane) => (
          <g key={lane.label} className={styles.lane}>
            <text x={lane.x} y={10} textAnchor="middle">
              {lane.label}
            </text>
            <line x1={lane.x} y1={18} x2={lane.x} y2={height} />
          </g>
        ))}
        {boxes
          .filter((box) => box.kind === 'group')
          .map((box) => (
            <BoxShape key={box.id} box={box} part="outline" />
          ))}
        {links.map((link) => {
          const {d, label} = route(byId.get(link.from)!, byId.get(link.to)!, link);
          return (
            <g key={`${link.from}-${link.to}`} className={styles.link}>
              <path
                d={d}
                markerEnd={`url(#${arrow})`}
                className={link.dashed ? styles.dashed : ''}
              />
              {/* The text version names every link with its label, so this copy is hidden. */}
              {label && (
                <text
                  aria-hidden="true"
                  x={label.x}
                  y={label.y}
                  textAnchor={label.align}
                  dominantBaseline="middle">
                  {link.label}
                </text>
              )}
            </g>
          );
        })}
        {boxes
          .filter((box) => box.kind === 'group')
          .map((box) => (
            <BoxShape key={box.id} box={box} part="name" />
          ))}
        {boxes
          .filter((box) => box.kind !== 'group')
          .map((box) => (
            <BoxShape key={box.id} box={box} />
          ))}
      </svg>
      <details className={styles.text}>
        <summary>{text}</summary>
        <ol>
          {textVersion(boxes, links, lanes).map((line) => (
            <li key={line}>{line}</li>
          ))}
        </ol>
      </details>
    </figure>
  );
}
