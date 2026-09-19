// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docs: [
    'intro',
    'install',
    'configuration',
    'secrets',
    'networks',
    'health-api',
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
  ],
};

export default sidebars;
