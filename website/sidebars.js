// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docs: [
    'intro',
    'architecture',
    'security-model',
    'install',
    'configuration',
    'secrets',
    'networks',
    'health-api',
    'bridge-parameters',
    {
      type: 'category',
      label: 'Operations',
      collapsed: false,
      items: [
        'operations/states',
        'operations/first-sync',
        'operations/signing-stalls',
        'operations/backfill',
        'operations/emergency-reset',
        'operations/troubleshooting',
      ],
    },
    'upgrade-notes',
    'attribution',
  ],
};

export default sidebars;
