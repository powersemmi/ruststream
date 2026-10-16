import path from 'node:path';
import {themes as prismThemes} from 'prism-react-renderer';
import type {Config, Plugin} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import {resolveMarkdownLinks} from './markdownLinks';
import remarkSnippets from './plugins/remarkSnippets';
import redirectsPlugin from './plugins/redirects';
import {redirects} from './redirects';

// Every minor line is built on its own and served from `/ruststream/<version>/`, with a second
// build of the newest line under `/ruststream/latest/`. The deploy script sets the base URL per
// build, so one configuration serves every version directory.
const siteRoot = '/ruststream/';
const baseUrl = process.env.DOCS_BASE_URL ?? `${siteRoot}latest/`;

if (!/^\/ruststream\/[^/]+\/$/.test(baseUrl)) {
  throw new Error(`DOCS_BASE_URL must be ${siteRoot}<version>/, got '${baseUrl}'`);
}

const accent = '#ff5722';

// Excerpts on the pages are read from the repository's compiled sources and link back to them.
const repoRoot = path.resolve(__dirname, '..');
const sourceUrl = 'https://github.com/powersemmi/ruststream/blob/main';

// The search plugin offers an "Ask AI" entry when `askAi` is set and renders the module
// `open-ask-ai` as the widget that answers it. Pointing that module at our adapter sends the
// entry to the site's own panel instead of the third-party widget.
function askAiInSearch(): Plugin {
  return {
    name: 'ruststream-ask-ai-in-search',
    configureWebpack() {
      return {
        resolve: {
          alias: {
            'open-ask-ai$': path.resolve(__dirname, 'src/components/AskAi/searchAdapter.tsx'),
            'open-ask-ai/styles.css': path.resolve(__dirname, 'src/components/AskAi/empty.css'),
          },
        },
      };
    },
  };
}

const config: Config = {
  title: 'RustStream',
  tagline: 'Async messaging framework for Rust',
  favicon: 'img/logo.svg',

  future: {
    v4: true,
    faster: true,
  },

  url: 'https://powersemmi.github.io',
  baseUrl,
  trailingSlash: true,

  organizationName: 'powersemmi',
  projectName: 'ruststream',

  onBrokenLinks: 'throw',
  onBrokenAnchors: 'throw',
  onDuplicateRoutes: 'throw',

  customFields: {
    siteRoot,
    accent,
  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en', 'ru', 'zh'],
    localeConfigs: {
      en: {label: 'English', htmlLang: 'en'},
      ru: {label: 'Русский', htmlLang: 'ru', path: 'ru'},
      zh: {label: '中文', htmlLang: 'zh-CN', path: 'zh'},
    },
  },

  markdown: {
    mermaid: true,
    hooks: {
      onBrokenMarkdownLinks: resolveMarkdownLinks(),
    },
  },

  themes: [
    '@docusaurus/theme-mermaid',
    [
      '@easyops-cn/docusaurus-search-local',
      {
        hashed: true,
        language: ['en', 'ru', 'zh'],
        indexBlog: false,
        docsDir: '../docs',
        docsRouteBasePath: '/',
        highlightSearchTermsOnTargetPage: true,
        askAi: {},
      },
    ],
  ],

  plugins: [askAiInSearch, (context) => redirectsPlugin(context, {redirects})],

  presets: [
    [
      'classic',
      {
        docs: {
          path: '../docs',
          routeBasePath: '/',
          sidebarPath: './sidebars.ts',
          remarkPlugins: [[remarkSnippets, {rootDir: repoRoot, repoUrl: sourceUrl}]],
          editUrl: 'https://github.com/powersemmi/ruststream/edit/main/docs/',
          // The translations live under `website/i18n/`; the other folders feed tools, not pages.
          exclude: [
            'ru/**',
            'zh/**',
            'benchmarks/**',
          ],
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    colorMode: {
      defaultMode: 'dark',
      respectPrefersColorScheme: false,
      disableSwitch: false,
    },
    navbar: {
      title: 'RustStream',
      logo: {
        alt: 'RustStream',
        src: 'img/logo.svg',
      },
      items: [
        {type: 'custom-versionDropdown', position: 'left'},
        {type: 'custom-askAi', position: 'right'},
        {type: 'localeDropdown', position: 'right'},
        {
          href: 'https://github.com/powersemmi/ruststream',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'light',
      copyright: 'Apache-2.0',
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['toml', 'bash'],
    },
    mermaid: {
      theme: {light: 'neutral', dark: 'dark'},
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
