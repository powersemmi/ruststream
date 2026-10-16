// The guides moved to docs.rs, where each feature module documents itself, so every old guide
// address, in every language, lands on the module that took the topic over. `latest` there is
// the newest stable release. A page that moves within the site gets an entry here too.
const docsRs = 'https://docs.rs/ruststream/latest/ruststream';

const guides: Record<string, string> = {
  templates: '/broker-authors/templates/',
  subscribers: `${docsRs}/runtime/index.html#subscribers`,
  publishing: `${docsRs}/runtime/index.html#publishing`,
  routing: `${docsRs}/runtime/index.html#routing`,
  codecs: `${docsRs}/codec/index.html`,
  context: `${docsRs}/runtime/index.html#context-and-state`,
  headers: `${docsRs}/runtime/index.html#typed-headers`,
  lifespan: `${docsRs}/runtime/index.html#lifecycle`,
  middleware: `${docsRs}/runtime/index.html#middleware`,
  'failure-policy': `${docsRs}/runtime/index.html#failure-policy`,
  http: `${docsRs}/runtime/index.html#running-beside-another-server`,
  'transactional-outbox': `${docsRs}/runtime/index.html#running-beside-another-server`,
  logging: `${docsRs}/logging/index.html`,
  metrics: `${docsRs}/metrics/index.html`,
  opentelemetry: `${docsRs}/otel/index.html`,
  asyncapi: `${docsRs}/asyncapi/index.html`,
  testing: `${docsRs}/testing/index.html`,
  cli: `${docsRs}/runtime/cli/index.html`,
};

/** Old address to its target; every language of a build gets the same set. */
export const redirects: Record<string, string> = Object.fromEntries(
  Object.entries(guides).map(([guide, to]) => [`guides/${guide}`, to]),
);
