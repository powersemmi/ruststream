/**
 * The address search engines should index for `url`, a fully qualified address of this build.
 *
 * The newest line is built twice, under its number and under `latest`, and both copies name the
 * `latest` one: its address outlives the release, while the numbered copy becomes an old version
 * at the next one. Any other build is its own canonical copy.
 */
export function canonicalFor(
  url: string,
  siteUrl: string,
  baseUrl: string,
  canonicalBaseUrl: string,
): string {
  const buildRoot = `${siteUrl}${baseUrl}`;
  if (baseUrl === canonicalBaseUrl || !url.startsWith(buildRoot)) {
    return url;
  }
  return `${siteUrl}${canonicalBaseUrl}${url.slice(buildRoot.length)}`;
}
