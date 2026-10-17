import type {HtmlTagObject} from '@docusaurus/types';

/** A search console's ownership token, by the environment variable the deploy passes it in. */
const CONSOLES = [
  ['DOCS_GOOGLE_SITE_VERIFICATION', 'google-site-verification'],
  ['DOCS_BING_SITE_VERIFICATION', 'msvalidate.01'],
] as const;

/**
 * The ownership tags of the search consoles, on every page of the build. A console checks the
 * property's home page, and the root page redirects to `latest/` at once, so a crawler that
 * follows the redirect has to find the tag there too.
 */
export function verificationTags(env: NodeJS.ProcessEnv): HtmlTagObject[] {
  return CONSOLES.flatMap(([variable, name]) => {
    const token = env[variable] ?? '';
    if (token === '') {
      return [];
    }
    if (!/^[A-Za-z0-9_-]+$/.test(token)) {
      throw new Error(`${variable} holds letters, digits, '-' and '_' only, got '${token}'`);
    }
    return [{tagName: 'meta', attributes: {name, content: token}}];
  });
}
