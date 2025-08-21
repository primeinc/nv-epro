const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

function utcStamp() {
  const d = new Date();
  const yyyy = d.getUTCFullYear();
  const MM = String(d.getUTCMonth() + 1).padStart(2, '0');
  const DD = String(d.getUTCDate()).padStart(2, '0');
  const hh = String(d.getUTCHours()).padStart(2, '0');
  const mm = String(d.getUTCMinutes()).padStart(2, '0');
  const ss = String(d.getUTCSeconds()).padStart(2, '0');
  return `${yyyy}${MM}${DD}T${hh}${mm}${ss}Z`;
}

function gitSha() {
  try {
    const sha = execSync('git rev-parse --short HEAD', { stdio: ['ignore', 'pipe', 'ignore'] }).toString().trim();
    return sha || 'nogit';
  } catch (e) {
    return 'nogit';
  }
}

function getRunContext(dataset, label) {
  const DATA_ROOT = process.env.DATA_ROOT || path.join(process.cwd(), 'data');
  const runId = `run_${utcStamp()}_${gitSha()}`;

  const now = new Date();
  const yyyy = String(now.getUTCFullYear());
  const MM = String(now.getUTCMonth() + 1).padStart(2, '0');
  const DD = String(now.getUTCDate()).padStart(2, '0');

  const rawDir = path.join(DATA_ROOT, 'nevada-epro', dataset, 'raw', yyyy, MM, DD, runId);
  const filesDir = path.join(rawDir, 'files');
  const downloadsDir = path.join(rawDir, 'downloads');

  fs.mkdirSync(filesDir, { recursive: true });
  fs.mkdirSync(downloadsDir, { recursive: true });

  return {
    runId,
    DATA_ROOT,
    OUTPUT_DIR: filesDir,
    DOWNLOAD_DIR: downloadsDir,
    manifestPath: path.join(rawDir, 'manifest.json'),
    checksumsPath: path.join(rawDir, 'checksums.sha256'),
    rawDir
  };
}

module.exports = { getRunContext };