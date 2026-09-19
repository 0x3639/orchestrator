// Emits llms.txt, llms-full.txt and a raw Markdown copy of every doc into the
// build output, so language models and their crawlers can read the guide as
// plain text. Follows the llms.txt convention: an H1, a blockquote summary,
// then sections of links with one-line descriptions.
//
// Nothing here is committed; the files are generated at build time from the
// same Markdown that renders the HTML pages.

import fs from 'node:fs/promises';
import path from 'node:path';

const DOCS_DIR = 'docs';
const DOCS_ROUTE = 'docs';

function parseFrontMatter(source) {
  const match = source.match(/^---\n([\s\S]*?)\n---\n/);
  if (!match) return {data: {}, body: source};
  const data = {};
  for (const line of match[1].split('\n')) {
    const idx = line.indexOf(':');
    if (idx === -1) continue;
    const key = line.slice(0, idx).trim();
    let value = line.slice(idx + 1).trim();
    if (value.startsWith('"') && value.endsWith('"')) value = JSON.parse(value);
    data[key] = value;
  }
  return {data, body: source.slice(match[0].length)};
}

// Sidebar order: the sidebars.js structure flattened, categories kept.
function sidebarOrder(sidebars) {
  const out = [];
  const walk = (items, section) => {
    for (const item of items) {
      if (typeof item === 'string') out.push({id: item, section});
      else if (item.type === 'category') walk(item.items, item.label);
    }
  };
  walk(sidebars.docs, 'Guide');
  return out;
}

// Turn relative doc links (../health-api.md, backfill.md#runbook) into
// absolute site URLs, and Docusaurus admonitions into plain paragraphs.
function toPlainMarkdown(body, docId, siteUrl) {
  const dir = path.posix.dirname(docId);
  const linked = body.replace(/\]\(([^)]+?\.md)(#[^)]*)?\)/g, (m, target, hash) => {
    const resolved = path.posix.normalize(path.posix.join(dir, target)).replace(/\.md$/, '');
    return `](${siteUrl}/${DOCS_ROUTE}/${resolved}/${hash ?? ''})`;
  });
  return linked
    // MDX imports and React components have no text form; drop them.
    .replace(/^import .*$/gm, '')
    .replace(/^<[A-Z][\w.]*[^>]*\/>$/gm, '')
    .replace(/^:::(\w+)(?:\s+(.*))?$/gm, (m, kind, title) => `**${title || kind[0].toUpperCase() + kind.slice(1)}.**`)
    .replace(/^:::$/gm, '')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

export default function llmsTxtPlugin(context) {
  return {
    name: 'llms-txt',
    async postBuild({outDir, siteConfig}) {
      const siteUrl = siteConfig.url.replace(/\/$/, '') + siteConfig.baseUrl.replace(/\/$/, '');
      const {default: sidebars} = await import(path.resolve(context.siteDir, 'sidebars.js'));
      const order = sidebarOrder(sidebars);

      const pages = [];
      for (const {id, section} of order) {
        const file = path.join(context.siteDir, DOCS_DIR, `${id}.md`);
        const source = await fs.readFile(file, 'utf8');
        const {data, body} = parseFrontMatter(source);
        const markdown = toPlainMarkdown(body, id, siteUrl);
        pages.push({
          id,
          section,
          title: data.title ?? id,
          description: data.description ?? '',
          url: `${siteUrl}/${DOCS_ROUTE}/${id}/`,
          markdown,
        });
      }

      // llms.txt: the index.
      const bySection = new Map();
      for (const p of pages) {
        if (!bySection.has(p.section)) bySection.set(p.section, []);
        bySection.get(p.section).push(p);
      }
      let index = `# ${siteConfig.title}\n\n> ${siteConfig.tagline}. `;
      index += 'The orchestrator is the threshold-signature signer node of the Zenon bridge between the Network of Momentum and EVM networks. ';
      index += 'This guide covers installing, configuring, securing and operating a signer, and diagnosing sync and signing problems.\n\n';
      index += `Every page below is also available as raw Markdown by appending \`.md\` to its path (for example \`${siteUrl}/${DOCS_ROUTE}/intro.md\`), and the whole guide is concatenated at ${siteUrl}/llms-full.txt.\n\n`;
      for (const [section, list] of bySection) {
        index += `## ${section}\n\n`;
        for (const p of list) index += `- [${p.title}](${p.url}): ${p.description}\n`;
        index += '\n';
      }
      index += '## Source\n\n';
      index += `- [Orchestrator repository, 0x3639 build](https://github.com/0x3639/orchestrator): Go source for the signer this guide documents.\n`;
      index += `- [Upstream orchestrator](https://github.com/HyperCore-Team/orchestrator): the HyperCore Team codebase this build derives from.\n`;
      await fs.writeFile(path.join(outDir, 'llms.txt'), index);

      // llms-full.txt: everything.
      let full = `# ${siteConfig.title}: complete guide\n\n> ${siteConfig.tagline}. Generated from the same Markdown that renders ${siteUrl}.\n\n`;
      for (const p of pages) {
        full += `---\n\n# ${p.title}\n\nSource: ${p.url}\n\n${p.markdown}\n\n`;
      }
      await fs.writeFile(path.join(outDir, 'llms-full.txt'), full);

      // Raw Markdown beside each rendered page.
      for (const p of pages) {
        const target = path.join(outDir, DOCS_ROUTE, `${p.id}.md`);
        await fs.mkdir(path.dirname(target), {recursive: true});
        await fs.writeFile(target, `# ${p.title}\n\n${p.description}\n\nCanonical: ${p.url}\n\n${p.markdown}\n`);
      }
    },
  };
}
