/** The parts of the diagram "The path of a message". */
export type NodeId =
  | 'broker'
  | 'subscription'
  | 'codec'
  | 'middleware'
  | 'handler'
  | 'reply'
  | 'publisher'
  | 'asyncapi'
  | 'testapp';

/** The page that documents each part, site-relative; the base URL adds the version and language. */
export const PART_PAGES: Record<NodeId, string> = {
  broker: '/brokers/',
  subscription: '/components/subscribers/subscription-source/',
  codec: '/components/subscribers/codecs/',
  middleware: '/components/subscribers/middleware/',
  handler: '/components/subscribers/',
  reply: '/components/subscribers/publishing/replies/',
  publisher: '/components/subscribers/publishing/publishers/publisher/',
  asyncapi: '/components/asyncapi/generation/build-spec/',
  testapp: '/components/application/testing/',
};

/** The files under `docs/` that can serve a route: a page of that name, or a category's index. */
export function docFor(route: string): string[] {
  const inner = route.replace(/^\/|\/$/g, '');
  return [`${inner}.md`, `${inner}/index.md`];
}
