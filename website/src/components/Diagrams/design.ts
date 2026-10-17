import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * Design: the service on top, the broker-agnostic core under it, and the broker crates that plug
 * into the core's traits from below, each wrapping one client. Settings live in the broker crate.
 */
export function design(words: Words): {boxes: Box[]; links: Link[]} {
  const boxes: Box[] = [
    {id: 'service', x: 0, y: 0, w: 360, h: 80, kind: 'group', lines: lines(words, 'service')},
    {
      id: 'handlers',
      x: 12,
      y: 26,
      w: 160,
      h: 42,
      kind: 'service',
      code: true,
      lines: lines(words, 'handlers'),
      href: '/components/subscribers/',
    },
    {
      id: 'app',
      x: 188,
      y: 26,
      w: 160,
      h: 42,
      kind: 'service',
      code: true,
      lines: lines(words, 'app'),
      href: '/components/application/',
    },
    {id: 'core', x: 0, y: 104, w: 360, h: 212, kind: 'group', lines: lines(words, 'core')},
    {
      id: 'runtime',
      x: 12,
      y: 130,
      w: 336,
      h: 42,
      kind: 'core',
      lines: lines(words, 'runtime'),
      href: '/architecture/async/',
    },
    {
      id: 'codecs',
      x: 12,
      y: 184,
      w: 160,
      h: 58,
      kind: 'core',
      lines: lines(words, 'codecs'),
      href: '/components/subscribers/codecs/',
    },
    {id: 'modules', x: 188, y: 184, w: 160, h: 58, kind: 'core', lines: lines(words, 'modules')},
    {
      id: 'traits',
      x: 12,
      y: 254,
      w: 160,
      h: 50,
      kind: 'core',
      code: true,
      lines: lines(words, 'traits'),
    },
    {
      id: 'capabilities',
      x: 188,
      y: 254,
      w: 160,
      h: 50,
      kind: 'core',
      lines: lines(words, 'capabilities'),
      href: '/components/subscribers/publishing/publishers/',
    },
    {id: 'crates', x: 0, y: 352, w: 360, h: 120, kind: 'group', lines: lines(words, 'crates')},
    {id: 'nats', x: 12, y: 378, w: 160, h: 82, kind: 'broker', lines: lines(words, 'nats')},
    {id: 'kafka', x: 188, y: 378, w: 160, h: 82, kind: 'broker', lines: lines(words, 'kafka')},
    {
      id: 'natsClient',
      x: 12,
      y: 500,
      w: 160,
      h: 32,
      kind: 'broker',
      code: true,
      lines: ['async-nats'],
    },
    {
      id: 'kafkaClient',
      x: 188,
      y: 500,
      w: 160,
      h: 32,
      kind: 'broker',
      code: true,
      lines: ['rdkafka'],
    },
  ];
  const links: Link[] = [
    {from: 'handlers', to: 'runtime', label: word(words, 'mounted')},
    {from: 'nats', to: 'traits', label: word(words, 'implements')},
    {from: 'kafka', to: 'capabilities', label: word(words, 'implements')},
    {from: 'nats', to: 'natsClient', label: word(words, 'wraps')},
    {from: 'kafka', to: 'kafkaClient', label: word(words, 'wraps')},
  ];
  return {boxes, links};
}
