import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import {linkToPendingPage, pendingPages} from './pendingPages';

// Every minor line is built on its own and served from `/ruststream/<version>/`, with a second
// build of the newest line under `/ruststream/latest/`. The deploy script sets the base URL per
// build, so one configuration serves every version directory.
const siteRoot = '/ruststream/';
const baseUrl = process.env.DOCS_BASE_URL ?? `${siteRoot}latest/`;

if (!/^\/ruststream\/[^/]+\/$/.test(baseUrl)) {
  throw new Error(`DOCS_BASE_URL must be ${siteRoot}<version>/, got '${baseUrl}'`);
}

const accent = '#ff5722';

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
      onBrokenMarkdownLinks: linkToPendingPage(baseUrl),
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
      },
    ],
  ],

  presets: [
    [
      'classic',
      {
        docs: {
          path: '../docs',
          routeBasePath: '/',
          sidebarPath: './sidebars.ts',
          editUrl: 'https://github.com/powersemmi/ruststream/edit/main/docs/',
          // The translations live under `website/i18n/`; the other folders feed tools, not pages.
          exclude: [
            'ru/**',
            'zh/**',
            'hooks/**',
            'assets/**',
            'benchmarks/**',
            ...pendingPages.map((page) => `${page}.md`),
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
        {type: 'localeDropdown', position: 'right'},
        {
          href: 'https://github.com/powersemmi/ruststream',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
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
