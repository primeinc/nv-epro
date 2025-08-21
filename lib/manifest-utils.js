
// lib/manifest-utils.js
const fs = require('fs');
const fsp = fs.promises;
const path = require('path');
const crypto = require('crypto');
const { execSync } = require('child_process');

function normalizeRel(p) {
  return p.split(path.sep).join('/');
}

function sha256Stream(filePath) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('sha256');
    const s = fs.createReadStream(filePath);
    s.on('data', c => hash.update(c));
    s.on('end', () => resolve(hash.digest('hex')));
    s.on('error', reject);
  });
}

async function countCsvRecords(filePath) {
  // Fast line count; subtract 1 for header if present
  const s = fs.createReadStream(filePath, { encoding: 'utf8' });
  let lines = 0;
  let leftover = '';
  for await (const chunk of s) {
    let start = 0;
    let idx;
    while ((idx = chunk.indexOf('\n', start)) !== -1) {
      lines++;
      start = idx + 1;
    }
    leftover += chunk.slice(start);
  }
  if (leftover.length) lines++; // last line no newline
  // Heuristic: assume first line is header if file has at least 2 lines
  return Math.max(0, lines - (lines > 1 ? 1 : 0));
}

async function listOutputFiles(dir, status = 'success') {
  let out = [];
  let entries = [];
  try {
    entries = await fsp.readdir(dir, { withFileTypes: true });
  } catch {
    return out;
  }
  for (const ent of entries) {
    if (!ent.isFile()) continue;
    const fp = path.join(dir, ent.name);
    const stat = await fsp.stat(fp);
    const item = {
      name: ent.name,
      sizeBytes: stat.size,
      kind: ent.name.toLowerCase().endsWith('.csv') ? 'csv' : 'blob',
      records: null
    };
    if (item.kind === 'csv') {
      try {
        item.records = await countCsvRecords(fp);
      } catch {
        item.records = null;
      }
    }
    // Mark partial if this is an error run
    if (status === 'error' || status === 'failed') {
      item.partial = true;
    }
    out.push(item);
  }
  return out;
}

function git(val, fallback) {
  try {
    return execSync(val, { stdio: ['ignore', 'pipe', 'ignore'] }).toString().trim() || fallback;
  } catch {
    return fallback;
  }
}

function getGitBranch() { return git('git rev-parse --abbrev-ref HEAD', 'unknown'); }
function getGitSHA()    { return git('git rev-parse HEAD', 'unknown'); }

async function* walkFiles(rootDir) {
  const stack = [rootDir];
  while (stack.length) {
    const dir = stack.pop();
    let entries = [];
    try {
      entries = await fsp.readdir(dir, { withFileTypes: true });
    } catch {
      continue;
    }
    for (const ent of entries) {
      const fp = path.join(dir, ent.name);
      if (ent.isDirectory()) {
        stack.push(fp);
      } else if (ent.isFile()) {
        yield fp;
      }
    }
  }
}

async function generateChecksums(runtime) {
  const lines = [];
  for await (const fp of walkFiles(runtime.rawDir)) {
    const base = path.basename(fp).toLowerCase();
    if (base === 'checksums.sha256') continue; // don't hash ourselves
    const hash = await sha256Stream(fp);
    const rel = normalizeRel(path.relative(runtime.rawDir, fp));
    lines.push(`${hash}  ${rel}`);
  }
  lines.sort(); // stable
  await fsp.writeFile(runtime.checksumsPath, lines.join('\n') + '\n', 'utf8');
  return lines;
}

async function generateManifest(runtime, cfg) {
  const {
    dataset,
    label,
    startDate,
    endDate,
    filter,
    startTime,
    endTime,
    command,
    status = 'success',
    error = null
  } = cfg;

  const outputs = await listOutputFiles(runtime.OUTPUT_DIR, status);
  const totalRecords = outputs.reduce((a, f) => a + (typeof f.records === 'number' ? f.records : 0), 0);

  const manifest = {
    schemaVersion: '1.0',
    run: {
      id: runtime.runId,
      dataset,
      label: label ?? null,
      startTime: startTime ? new Date(startTime).toISOString() : null,
      endTime: endTime ? new Date(endTime).toISOString() : null,
      durationMs: startTime && endTime ? (new Date(endTime) - new Date(startTime)) : null,
      status,
      error
    },
    input: {
      parameters: {
        ...(startDate && { startDate }),
        ...(endDate && { endDate }),
        ...(filter && { filter })
      },
      argv: process.argv.slice(2),
      command: command || `node ${path.basename(process.argv[1])} ${process.argv.slice(2).join(' ')}`
    },
    environment: {
      platform: process.platform,
      nodeVersion: process.version,
      playwrightVersion: (() => {
        try {
          return require(path.join(process.cwd(), 'node_modules', 'playwright', 'package.json')).version;
        } catch {
          return 'unknown';
        }
      })(),
      dataRoot: runtime.DATA_ROOT,
      git: {
        branch: getGitBranch(),
        sha: getGitSHA(),
        shortSha: runtime.runId.split('_').pop()
      }
    },
    output: {
      outputDir: normalizeRel(path.relative(runtime.rawDir, runtime.OUTPUT_DIR)) || '.',
      downloadDir: normalizeRel(path.relative(runtime.rawDir, runtime.DOWNLOAD_DIR)) || '.',
      files: outputs
    },
    source: {
      system: 'Nevada eProcurement',
      url: 'https://nevadaepro.com',
      accessedAt: startTime ? new Date(startTime).toISOString() : null
    }
  };

  await fsp.writeFile(runtime.manifestPath, JSON.stringify(manifest, null, 2), 'utf8');
  return manifest;
}

async function finalizeRun(runtime, cfg) {
  const manifest = await generateManifest(runtime, cfg);
  await generateChecksums(runtime);
  return manifest;
}

module.exports = {
  finalizeRun,
  generateManifest,
  generateChecksums,
  countCsvRecords,
  getGitBranch,
  getGitSHA
};