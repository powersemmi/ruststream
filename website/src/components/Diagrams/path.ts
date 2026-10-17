import type {Box, Link} from './flow';
import {chain} from './chain';
import type {Words} from './words';

/** The path of one delivery that is answered, in the order the runtime runs each stage. */
export function path(words: Words): {boxes: Box[]; links: Link[]} {
  return chain(words, [
    {key: 'stream', kind: 'broker', href: '/components/subscribers/subscription-source/'},
    {key: 'loop', kind: 'core', href: '/architecture/async/'},
    {key: 'layers', href: '/components/subscribers/middleware/layer/'},
    {key: 'decode', href: '/components/subscribers/codecs/'},
    {key: 'body', kind: 'service', href: '/components/subscribers/'},
    {key: 'encode', href: '/components/subscribers/publishing/replies/'},
    {key: 'transforms', href: '/components/subscribers/publishing/transforms/'},
    {key: 'publishLayers', href: '/components/subscribers/middleware/publish-layer/'},
    {
      key: 'publisher',
      kind: 'broker',
      href: '/components/subscribers/publishing/publishers/publisher/',
    },
    {key: 'settle', kind: 'core', href: '/architecture/settlement/'},
  ]);
}
