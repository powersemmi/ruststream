import {readdir, readFile} from 'node:fs/promises';
import path from 'node:path';
import {DEFAULT_PARSE_FRONT_MATTER, parseMarkdownFile} from '@docusaurus/utils';

/** What a search result shows of a documentation page in one language. */
export interface Page {
  /** The file the page is built from, relative to the repository. */
  file: string;
  locale: string;
  /** The page's path within its language, the same in every language. */
  route: string;
  /** The front matter `title`, or the page's `# ` heading, as Docusaurus reads it. */
  title?: string;
  description?: string;
  /**
   * The language has no translation of the page and serves the English one; its text is
   * checked under English.
   */
  fallback?: boolean;
}

// Google cuts a snippet near 160 characters; a description longer than that ends in an ellipsis.
const maxDescription = 160;
// A description written as a sentence ends with its mark; one that does not was cut off, which is
// what Docusaurus does when it takes the first source line of a page as its description.
const sentenceEnd = /[.!?。！？]$/;

/** The problems of `pages`, one line each; an empty list when every page is fine. */
export function checkPages(pages: Page[]): string[] {
  const problems: string[] = [];
  const titles = new Map<string, string>();
  for (const page of pages) {
    if (!page.title) {
      problems.push(`${page.file}: no title`);
    } else {
      const key = `${page.locale}\u0000${page.title}`;
      const first = titles.get(key);
      if (first) {
        problems.push(`${page.file}: the title '${page.title}' is also the title of ${first}`);
      } else {
        titles.set(key, page.file);
      }
    }
    if (page.fallback) {
      continue;
    }
    const description = page.description?.trim();
    if (!description) {
      problems.push(`${page.file}: no description in the front matter`);
    } else if (description.length > maxDescription) {
      problems.push(
        `${page.file}: the description has ${description.length} characters, more than ${maxDescription}`,
      );
    } else if (!sentenceEnd.test(description)) {
      problems.push(
        `${page.file}: the description does not end with a full stop, so it reads as cut off`,
      );
    }
  }
  return problems;
}

async function markdownFiles(root: string): Promise<string[]> {
  const entries = await readdir(root, {recursive: true, withFileTypes: true});
  return entries
    .filter((entry) => entry.isFile() && /\.mdx?$/.test(entry.name))
    .map((entry) => path.relative(root, path.join(entry.parentPath, entry.name)))
    .sort();
}

async function readPage(repoRoot: string, root: string, file: string, locale: string): Promise<Page> {
  const filePath = path.join(root, file);
  const {frontMatter, contentTitle} = await parseMarkdownFile({
    filePath,
    fileContent: await readFile(filePath, 'utf8'),
    parseFrontMatter: DEFAULT_PARSE_FRONT_MATTER,
  });
  const title = typeof frontMatter.title === 'string' ? frontMatter.title : contentTitle;
  const description =
    typeof frontMatter.description === 'string' ? frontMatter.description : undefined;
  return {
    file: path.relative(repoRoot, filePath),
    locale,
    route: file.replace(/\.mdx?$/, ''),
    title,
    description,
  };
}

/**
 * Every documentation page of every language: the English pages under `docs/`, and per language
 * under `website/i18n/` its translations plus the English pages it has not translated.
 */
export async function readPages(repoRoot: string): Promise<Page[]> {
  const docs = path.join(repoRoot, 'docs');
  const english = await Promise.all(
    (await markdownFiles(docs)).map((file) => readPage(repoRoot, docs, file, 'en')),
  );
  const pages = [...english];
  const i18n = path.join(repoRoot, 'website', 'i18n');
  for (const locale of (await readdir(i18n)).sort()) {
    const root = path.join(i18n, locale, 'docusaurus-plugin-content-docs', 'current');
    const translated = await Promise.all(
      (await markdownFiles(root)).map((file) => readPage(repoRoot, root, file, locale)),
    );
    const routes = new Set(translated.map((page) => page.route));
    pages.push(
      ...translated,
      ...english
        .filter((page) => !routes.has(page.route))
        .map((page) => ({...page, locale, fallback: true})),
    );
  }
  return pages;
}
