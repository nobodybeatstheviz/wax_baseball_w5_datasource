// Start the local Tableau MCP server (.mcp/, see build_workbook.py --setup-mcp) against ONE site,
// reading PAT auth from an untracked env file so no secret ever sits in Claude Code's config.
//
//   node tableau_mcp_launch.mjs .env         -> dev   (10ax / nobodybeatstheviz)
//   node tableau_mcp_launch.mjs .env.trial   -> trial (a second Tableau Cloud site)
//
// Env files use the repo's TABLEAU_* names (see .env.example); this maps them to the server's
// SERVER / SITE_NAME / PAT_NAME / PAT_VALUE. stdout belongs to the MCP protocol: log to stderr only.
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const envPath = resolve(here, process.argv[2] ?? '.env');

const vars = {};
for (const line of readFileSync(envPath, 'utf8').split(/\r?\n/)) {
  const m = line.match(/^\s*([A-Z_]+)\s*=\s*(.*?)\s*$/);
  if (m) vars[m[1]] = m[2];
}
const missing = ['TABLEAU_SERVER', 'TABLEAU_SITE', 'TABLEAU_PAT_NAME', 'TABLEAU_PAT_SECRET']
  .filter((k) => !vars[k] || vars[k].startsWith('paste-'));
if (missing.length) {
  console.error(`tableau_mcp_launch: ${envPath} is missing ${missing.join(', ')}`);
  process.exit(1);
}

Object.assign(process.env, {
  SERVER: vars.TABLEAU_SERVER,
  SITE_NAME: vars.TABLEAU_SITE,
  PAT_NAME: vars.TABLEAU_PAT_NAME,
  PAT_VALUE: vars.TABLEAU_PAT_SECRET,
});

// The server reads process.env at startup, so load it in-process rather than spawning a child.
await import(pathToFileURL(join(here, '.mcp/node_modules/@tableau/mcp-server/build/index.js')).href);
