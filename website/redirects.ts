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

// The entry pages moved into the categories of the site: installation, quick start and the
// tutorial open the guides, and the benchmarks explain the architecture's cost.
const pages: Record<string, string> = {
  'getting-started/installation': '/guides/getting-started/installation/',
  'getting-started/quickstart': '/guides/getting-started/quickstart/',
  'getting-started/tutorial': '/guides/getting-started/tutorial/',
  benchmarks: '/architecture/benchmarks/',
};

/**
 * Old address to its target; every language of a build gets the same set. `docsRs` is the
 * crate's rustdoc for the version built, which took over the API reference page.
 */
export function redirects(docsRs: string): Record<string, string> {
  return {
    ...Object.fromEntries(Object.entries(guides).map(([guide, to]) => [`guides/${guide}`, to])),
    ...pages,
    reference: `${docsRs}ruststream/`,
  };
}
