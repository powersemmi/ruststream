import React, {type ReactNode, useState} from 'react';
import Link from '@docusaurus/Link';
import Translate, {translate} from '@docusaurus/Translate';
import useBaseUrl from '@docusaurus/useBaseUrl';
import SectionHead from './SectionHead';
import {type NodeId, PART_PAGES} from './partPages';
import shared from './landing.module.css';
import styles from './architecture.module.css';

interface Part {
  id: NodeId;
  name: string;
  role: string;
}

// Called inside the component so the strings follow the page's language.
function parts(): Part[] {
  return [
    {
      id: 'broker',
      name: translate({id: 'landing.arch.broker', message: 'Broker'}),
      role: translate({
        id: 'landing.arch.broker.role',
        message:
          'A broker crate wraps one client and is held to the contract that the conformance suite checks; the core depends on no broker.',
      }),
    },
    {
      id: 'subscription',
      name: translate({id: 'landing.arch.subscription', message: 'Subscription'}),
      role: translate({
        id: 'landing.arch.subscription.role',
        message:
          'A subscription names what to consume and is resolved once, at startup, against the connected broker.',
      }),
    },
    {
      id: 'codec',
      name: translate({id: 'landing.arch.codec', message: 'Codec'}),
      role: translate({
        id: 'landing.arch.codec.role',
        message:
          'The codec turns the payload into the handler type: JSON, MessagePack or CBOR chosen at compile time, or raw bytes with no codec at all.',
      }),
    },
    {
      id: 'middleware',
      name: translate({id: 'landing.arch.middleware', message: 'Middleware'}),
      role: translate({
        id: 'landing.arch.middleware.role',
        message:
          'Consume and publish layers wrap every delivery: logging, metrics, tracing or your own.',
      }),
    },
    {
      id: 'handler',
      name: translate({id: 'landing.arch.handler', message: 'Handler'}),
      role: translate({
        id: 'landing.arch.handler.role',
        message:
          'An async function under #[subscriber]; what it returns settles the delivery or becomes the reply.',
      }),
    },
    {
      id: 'reply',
      name: translate({id: 'landing.arch.reply', message: 'Reply'}),
      role: translate({
        id: 'landing.arch.reply.role',
        message:
          'A reply type derives Outgoing and declares where it goes; the runtime encodes it and publishes it.',
      }),
    },
    {
      id: 'publisher',
      name: translate({id: 'landing.arch.publisher', message: 'Publisher'}),
      role: translate({
        id: 'landing.arch.publisher.role',
        message:
          'A publish policy paired with the connected broker; a handler also publishes through typed Out slots.',
      }),
    },
    {
      id: 'asyncapi',
      name: 'AsyncAPI',
      role: translate({
        id: 'landing.arch.asyncapi.role',
        message:
          'The same declarations generate an AsyncAPI 3.1 document, so the document cannot drift from the code.',
      }),
    },
    {
      id: 'testapp',
      name: 'TestApp',
      role: translate({
        id: 'landing.arch.testapp.role',
        message:
          'Tests start the production app with the broker in process and drive the real dispatch, with no server.',
      }),
    },
  ];
}

interface Geometry {
  x: number;
  y: number;
  w: number;
  h: number;
}

// The diagram on a 1000 x 420 canvas: the delivery runs left to right along the top row, the
// reply comes back right to left along the bottom one, and the side parts sit underneath.
const GEOMETRY: Record<NodeId, Geometry> = {
  broker: {x: 10, y: 40, w: 180, h: 220},
  subscription: {x: 270, y: 40, w: 180, h: 64},
  codec: {x: 530, y: 40, w: 180, h: 64},
  middleware: {x: 770, y: 8, w: 220, h: 284},
  handler: {x: 790, y: 40, w: 180, h: 220},
  publisher: {x: 270, y: 196, w: 180, h: 64},
  reply: {x: 530, y: 196, w: 180, h: 64},
  testapp: {x: 10, y: 336, w: 180, h: 64},
  asyncapi: {x: 400, y: 336, w: 180, h: 64},
};

const ARROWS = [
  'M190 72H262',
  'M450 72H522',
  'M710 72H782',
  'M790 228H718',
  'M530 228H458',
  'M270 228H198',
];

function Node({
  part,
  active,
  onActivate,
}: {
  part: Part;
  active: boolean;
  onActivate: (id: NodeId) => void;
}): ReactNode {
  const {x, y, w, h} = GEOMETRY[part.id];
  const frame = part.id === 'middleware';
  const href = useBaseUrl(PART_PAGES[part.id]);
  // An SVG link: Enter and a click open the part's page, and hover or focus shows its role.
  return (
    <a
      href={href}
      className={`${styles.node} ${frame ? styles.frame : ''} ${active ? styles.active : ''}`}
      aria-label={part.name}
      aria-describedby="landing-arch-role"
      onMouseEnter={() => onActivate(part.id)}
      onFocus={() => onActivate(part.id)}>
      <rect x={x} y={y} width={w} height={h} rx={frame ? 10 : 6} />
      <text
        x={frame ? x + 14 : x + w / 2}
        y={frame ? y + 20 : y + h / 2}
        textAnchor={frame ? 'start' : 'middle'}
        dominantBaseline="central">
        {part.name}
      </text>
    </a>
  );
}

/**
 * The path of a message through a service, as an SVG on wide screens and as a list on narrow
 * ones. Hovering or focusing a part shows its role, and each part links to its page.
 */
export default function Architecture(): ReactNode {
  const all = parts();
  const [active, setActive] = useState<NodeId>('handler');
  const current = all.find((part) => part.id === active)!;
  return (
    <section className={shared.section} aria-labelledby="landing-arch">
      <SectionHead
        id="landing-arch"
        eyebrow={<Translate id="landing.arch.eyebrow">Architecture</Translate>}
        title={<Translate id="landing.arch.title">The path of a message</Translate>}
        lede={
          <Translate id="landing.arch.lede">
            A delivery goes from the broker through the subscription and the codec to your handler,
            and the reply goes back the same way. Each part is a type the compiler checks.
          </Translate>
        }
      />
      <div className={styles.wide}>
        <svg
          className={styles.diagram}
          viewBox="0 0 1000 420"
          role="group"
          aria-label={translate({
            id: 'landing.arch.diagramLabel',
            message: 'Diagram of the parts a message passes through',
          })}>
          <defs>
            <marker
              id="landing-arch-arrow"
              viewBox="0 0 10 10"
              refX="9"
              refY="5"
              markerWidth="8"
              markerHeight="8"
              orient="auto-start-reverse">
              <path d="M0 0L10 5L0 10z" className={styles.arrowHead} />
            </marker>
          </defs>
          {ARROWS.map((d) => (
            <path key={d} d={d} className={styles.arrow} markerEnd="url(#landing-arch-arrow)" />
          ))}
          <path d="M100 336V268" className={styles.dotted} />
          <path d="M450 336L380 268M530 336L600 268" className={styles.dotted} />
          {/* The middleware frame precedes the handler, so the handler is drawn and hit on top. */}
          {all.map((part) => (
            <Node key={part.id} part={part} active={part.id === active} onActivate={setActive} />
          ))}
        </svg>
        <div id="landing-arch-role" className={styles.role} aria-live="polite">
          <span className={styles.roleName}>{current.name}</span>
          <span>{current.role}</span>
        </div>
      </div>
      <ol className={styles.narrow}>
        {all.map((part) => (
          <li key={part.id}>
            <Link className={styles.roleName} to={PART_PAGES[part.id]}>
              {part.name}
            </Link>
            <span>{part.role}</span>
          </li>
        ))}
      </ol>
    </section>
  );
}
