/**
 * The languages the local search indexes one language build in. English is always among them,
 * because the API names on every page are English. A translated build adds its own language
 * only: the Chinese word splitter keeps Latin words and ideographs and drops every other script,
 * so a build that listed Chinese beside Russian would index no Russian word.
 *
 * The plugin sets its index builder up for the first language set it meets in a process, so
 * the `build` script builds each language in a process of its own.
 */
export function searchLanguages(locale: string): string[] {
  return locale === 'en' ? ['en'] : ['en', locale];
}
