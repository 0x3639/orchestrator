// @ts-check
// Operator documentation for the Zenon TSS bridge orchestrator, hosted at
// 0x3639.com. Themed with the Zenon Network of Momentum design system:
// https://github.com/digitalSloth/zenon-design-system

import {themes as prismThemes} from 'prism-react-renderer';

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Orchestrator',
  tagline: 'Operator guide for the Zenon TSS bridge orchestrator',
  favicon: 'img/znn-logo.svg',

  url: 'https://0x3639.com',
  // Change to '/orchestrator/' if the site is served from a sub-path.
  baseUrl: '/',

  organizationName: '0x3639',
  projectName: 'orchestrator',

  onBrokenLinks: 'throw',
  onBrokenAnchors: 'throw',
  markdown: {
    hooks: {
      onBrokenMarkdownLinks: 'throw',
    },
  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  // The design system's two fonts: Space Grotesk for UI, JetBrains Mono for data.
  stylesheets: [
    {
      href: 'https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&family=Space+Grotesk:wght@300;400;500;600;700&display=swap',
      type: 'text/css',
    },
  ],

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: './sidebars.js',
          routeBasePath: 'docs',
          editUrl: 'https://github.com/0x3639/orchestrator/tree/dev/website/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      colorMode: {
        defaultMode: 'dark',
        respectPrefersColorScheme: true,
      },
      navbar: {
        title: 'Orchestrator',
        logo: {
          alt: 'Zenon',
          src: 'img/znn-logo.svg',
        },
        items: [
          {type: 'docSidebar', sidebarId: 'docs', position: 'left', label: 'Docs'},
          {to: '/docs/operations/signing-stalls', position: 'left', label: 'Runbooks'},
          {href: 'https://github.com/0x3639/orchestrator', label: 'GitHub', position: 'right'},
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              {label: 'Install', to: '/docs/install'},
              {label: 'Configuration', to: '/docs/configuration'},
              {label: 'Health API', to: '/docs/health-api'},
            ],
          },
          {
            title: 'Runbooks',
            items: [
              {label: 'First sync', to: '/docs/operations/first-sync'},
              {label: 'Signing stalls', to: '/docs/operations/signing-stalls'},
              {label: 'Backfill', to: '/docs/operations/backfill'},
            ],
          },
          {
            title: 'Source',
            items: [
              {label: 'Orchestrator (0x3639 fork)', href: 'https://github.com/0x3639/orchestrator'},
              {label: 'Upstream (HyperCore-Team)', href: 'https://github.com/HyperCore-Team/orchestrator'},
              {label: 'Zenon design system', href: 'https://github.com/digitalSloth/zenon-design-system'},
            ],
          },
        ],
        copyright: `Zenon Network of Momentum. Operator documentation maintained by 0x3639.`,
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
        additionalLanguages: ['bash', 'json', 'go'],
      },
    }),
};

export default config;
