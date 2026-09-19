# Orchestrator operator docs

Docusaurus site for the operator documentation, hosted at 0x3639.com. Styled with the
[Zenon design system](https://github.com/digitalSloth/zenon-design-system): Space Grotesk and
JetBrains Mono, semantic colour tokens, dark mode by default, plasma reserved for the main action.

```bash
cd website
npm install
npm run start     # local dev server with live reload
npm run build     # static site in build/
npm run serve     # preview the build
```

Content lives in `docs/`. The sidebar order is in `sidebars.js`. Theme tokens are in
`src/css/custom.css`. Set `baseUrl` in `docusaurus.config.js` if the site is served from a sub-path.
