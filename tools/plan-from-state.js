
// tools/plan-from-state.js
// Plan tasks by scanning existing manifests in DATA_ROOT and finding gaps.
// Outputs JSON { tasks: [...] } to stdout.
//
// Policies encoded here:
// - Snapshot datasets (contracts, bids, vendors):
//   * Ensure at least one snapshot per calendar day for the last SNAPSHOT_DAYS (default 14).
//   * If a day is missing, schedule a snapshot task for today (we don't backdate since it's a live feed).
//   * Always schedule today's snapshot.
// - POs (windowed):
//   * Ensure coverage for each day from last N DAYS (default 14). If missing, schedule a window for that day.
//   * For backfill, respect BACKFILL_START (default 2018-01-31) until today; only schedule windows that are missing.
//   * If run with --backfill, emit monthly windows for historical gaps.
//
// Usage examples:
//   node tools/plan-from-state.js > plan.json
//   node tools/plan-from-state.js --backfill > plan.json
//
const fs = require('fs');
const fsp = fs.promises;
const path = require('path');

const DATA_ROOT = process.env.DATA_ROOT || path.join(process.cwd(), 'data');
const SNAPSHOT_DATASETS = ['contracts','bids','vendors'];
const WINDOW_DATASET = 'purchase_orders';

const SNAPSHOT_DAYS = parseInt(process.env.PLAN_SNAPSHOT_DAYS || '14', 10);
const WINDOW_DAYS = parseInt(process.env.PLAN_WINDOW_DAYS || '14', 10);
const BACKFILL_START = process.env.PO_BACKFILL_START || '2018-01-01';  // ISO

function ymd(d) { return d.toISOString().slice(0,10); }
function toMMDDYYYY(d) {
  const m = String(d.getUTCMonth()+1).padStart(2,'0');
  const dd = String(d.getUTCDate()).padStart(2,'0');
  const y = d.getUTCFullYear();
  return `${m}/${dd}/${y}`;
}

async function* walk(dir) {
  const stack = [dir];
  while (stack.length) {
    const cur = stack.pop();
    let entries;
    try { entries = await fsp.readdir(cur, { withFileTypes: true }); }
    catch { continue; }
    for (const e of entries) {
      const p = path.join(cur, e.name);
      if (e.isDirectory()) stack.push(p);
      else if (e.isFile()) yield p;
    }
  }
}

async function readJsonSafe(p) {
  try {
    const s = await fsp.readFile(p, 'utf8');
    return JSON.parse(s);
  } catch { return null; }
}

async function manifestsForDataset(dataset) {
  const root = path.join(DATA_ROOT, 'nevada-epro', dataset, 'raw');
  const out = [];
  for await (const p of walk(root)) {
    if (p.endsWith('manifest.json')) {
      const m = await readJsonSafe(p);
      if (!m) continue;
      out.push({ path: p, manifest: m });
    }
  }
  return out;
}

function daySetFromManifests(mfs) {
  const set = new Set();
  for (const { manifest } of mfs) {
    const ts = manifest?.run?.endTime || manifest?.run?.startTime;
    if (!ts) continue;
    const day = ts.slice(0,10);
    set.add(day);
  }
  return set;
}

function hasNonEmptyCsv(m) {
  const files = m?.output?.files || [];
  return files.some(f => f.kind === 'csv' && (f.records || 0) > 0);
}

async function planSnapshots() {
  const tasks = [];
  const today = new Date();
  // Always schedule today's snapshot for each dataset
  for (const ds of SNAPSHOT_DATASETS) tasks.push({ kind:'snapshot', dataset: ds, label: 'all' });
  // Ensure coverage for last SNAPSHOT_DAYS
  for (const ds of SNAPSHOT_DATASETS) {
    const mfs = await manifestsForDataset(ds);
    const daysCovered = daySetFromManifests(mfs);
    // We only schedule a snapshot for today; historic gaps are not recoverable for ephemeral feeds.
    // But we can warn by adding 'note' tasks.
    for (let i=1; i<=SNAPSHOT_DAYS; i++) {
      const d = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate()-i));
      const key = ymd(d);
      if (!daysCovered.has(key)) {
        tasks.push({ kind: 'note', dataset: ds, message: `Missing snapshot for ${key} (ephemeral; cannot recover).` });
      }
    }
  }
  return tasks;
}

async function planPOWindows(opts) {
  const tasks = [];
  const today = new Date(Date.UTC(new Date().getUTCFullYear(), new Date().getUTCMonth(), new Date().getUTCDate()));
  const mfs = await manifestsForDataset(WINDOW_DATASET);
  // Extract per-day coverage from manifests: consider a day covered if manifest output has non-empty CSV
  const covered = new Set();
  for (const { manifest } of mfs) {
    const label = manifest?.run?.label || null;
    // If we labeled by YYYY-MM or YYYY-MM-DD, try to detect day coverage by input parameters
    const start = manifest?.input?.parameters?.startDate || null;
    const end = manifest?.input?.parameters?.endDate || null;
    if (start && end) {
      // Convert MM/DD/YYYY to ISO
      const [sm, sd, sy] = start.split('/').map(n=>parseInt(n,10));
      const [em, ed, ey] = end.split('/').map(n=>parseInt(n,10));
      const s = new Date(Date.UTC(sy, sm-1, sd));
      const e = new Date(Date.UTC(ey, em-1, ed));
      // cover each day [s, e] inclusive
      for (let d = new Date(s); d <= e; d = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()+1))) {
        covered.add(ymd(d));
      }
    } else if (label && /^\d{4}-\d{2}-\d{2}$/.test(label)) {
      covered.add(label);
    }
  }

  // For auto mode: if no PO data exists, include full historical backfill + current month
  // Otherwise just do current month updates
  if (!opts.backfill) {
    if (covered.size === 0) {
      // No PO data exists - use bulk download ranges from config
      const bulkRangesPath = path.join(process.cwd(), 'config', 'po-download-ranges.json');
      if (fs.existsSync(bulkRangesPath)) {
        const config = JSON.parse(fs.readFileSync(bulkRangesPath, 'utf8'));
        for (const range of config.ranges) {
          tasks.push({
            kind: 'window',
            dataset: 'purchase_orders',
            start: range.start_date,
            end: range.end_date,
            label: range.id
          });
        }
      } else {
        // Fallback to yearly windows if no bulk config exists
        const start = new Date(BACKFILL_START + 'T00:00:00Z');
        let year = start.getUTCFullYear();
        const currentYear = today.getUTCFullYear();
        
        while (year <= currentYear) {
          const yearStart = new Date(Date.UTC(year, 0, 1));
          let yearEnd;
          
          if (year === currentYear) {
            // For current year, end at today
            yearEnd = today;
          } else {
            // For past years, end at Dec 31
            yearEnd = new Date(Date.UTC(year, 11, 31));
          }
          
          // Start from beginning of year, no restrictions
          const actualStart = yearStart;
          
          tasks.push({
            kind: 'window',
            dataset: 'purchase_orders',
            start: toMMDDYYYY(actualStart),
            end: toMMDDYYYY(yearEnd),
            label: `year_${year}`
          });
          
          year++;
        }
      }
    } else {
      // PO data exists - use configured update strategy
      const bulkRangesPath = path.join(process.cwd(), 'config', 'po-download-ranges.json');
      if (fs.existsSync(bulkRangesPath)) {
        const config = JSON.parse(fs.readFileSync(bulkRangesPath, 'utf8'));
        if (config.ranges && config.ranges.length > 0) {
          const strategy = config.update_strategy || 'last';
          
          if (strategy === 'all') {
            // Download all ranges
            for (const range of config.ranges) {
              tasks.push({
                kind: 'window',
                dataset: 'purchase_orders',
                start: range.start_date,
                end: range.end_date,
                label: range.id
              });
            }
          } else if (strategy === 'incremental') {
            // Download only ranges not done today (default behavior, will be filtered by orchestrator)
            for (const range of config.ranges) {
              tasks.push({
                kind: 'window',
                dataset: 'purchase_orders',
                start: range.start_date,
                end: range.end_date,
                label: range.id
              });
            }
          } else {
            // Default to 'last' - just the most recent range
            const lastRange = config.ranges[config.ranges.length - 1];
            tasks.push({
              kind: 'window',
              dataset: 'purchase_orders',
              start: lastRange.start_date,
              end: lastRange.end_date,
              label: lastRange.id
            });
          }
        }
      } else {
        // No config file - fail
        throw new Error('Missing config/po-download-ranges.json - cannot determine PO download ranges');
      }
    }
  } else {
    // Backfill from BACKFILL_START to today, monthly, emit only months with uncovered days
    const start = new Date(BACKFILL_START + 'T00:00:00Z');
    let cursor = new Date(Date.UTC(start.getUTCFullYear(), start.getUTCMonth(), 1));
    while (cursor <= today) {
      const monthStart = new Date(Date.UTC(cursor.getUTCFullYear(), cursor.getUTCMonth(), 1));
      const monthEnd = new Date(Date.UTC(cursor.getUTCFullYear(), cursor.getUTCMonth()+1, 1)); // exclusive
      let missing = false;
      for (let d = new Date(monthStart); d < monthEnd && d <= today; d = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()+1))) {
        if (!covered.has(ymd(d))) { missing = true; break; }
      }
      if (missing) {
        const lastOfMonth = new Date(Date.UTC(cursor.getUTCFullYear(), cursor.getUTCMonth()+1, 0));
        tasks.push({
          kind: 'window',
          dataset: 'purchase_orders',
          start: toMMDDYYYY(monthStart),
          end: toMMDDYYYY(lastOfMonth),
          label: ymd(monthStart).slice(0,7)
        });
      }
      cursor = new Date(Date.UTC(cursor.getUTCFullYear(), cursor.getUTCMonth()+1, 1));
    }
  }

  return tasks;
}

async function main() {
  const backfill = process.argv.includes('--backfill');
  const tasks = [
    ...(await planSnapshots()),
    ...(await planPOWindows({ backfill }))
  ];
  process.stdout.write(JSON.stringify({ tasks }, null, 2));
}

if (require.main === module) {
  main().catch(err => { console.error(err); process.exit(1); });
}
