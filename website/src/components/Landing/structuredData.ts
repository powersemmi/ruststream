/**
 * The landing page's structured data: the schema.org type for a code project. Google shows no
 * rich result for it; Bing and the crawlers of AI assistants read it.
 */
export function softwareSourceCode({description, url}: {description: string; url: string}) {
  return {
    '@context': 'https://schema.org',
    '@type': 'SoftwareSourceCode',
    name: 'RustStream',
    description,
    url,
    codeRepository: 'https://github.com/powersemmi/ruststream',
    programmingLanguage: 'Rust',
    runtimePlatform: 'tokio',
    license: 'https://www.apache.org/licenses/LICENSE-2.0',
  };
}

/** `value` as the text of a `<script type="application/ld+json">` element. */
export function jsonLd(value: object): string {
  // JSON may hold `</script>`, which would end the element; `<` is the same string to a
  // JSON parser.
  return JSON.stringify(value).replace(/</g, '\\u003c');
}
