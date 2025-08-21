
// orchestrate.js (state-aware variant)
// Adds `run auto` which invokes tools/plan-from-state.js to compute tasks from DATA_ROOT.
const { spawn, spawnSync } = require('child_process');
const path = require('path');

const DATA_ROOT = process.env.DATA_ROOT || path.join(process.cwd(), 'data');
const CONCURRENCY = parseInt(process.env.ORCH_CONCURRENCY || '2', 10);
const RETRIES = parseInt(process.env.ORCH_RETRIES || '2', 10);

const DS = {
  contracts: { script: 'scrape-contract-cli.js', mode: 'snapshot' },
  bids: { script: 'scrape-bid-cli.js', mode: 'snapshot' },
  vendors: { script: 'scrape-vendor-cli.js', mode: 'snapshot' },
  purchase_orders: { script: 'scrape-po-cli.js', mode: 'windowed' }
};

function ymd(d) { return d.toISOString().slice(0,10); }
function toMMDDYYYY(d) {
  const m = String(d.getUTCMonth()+1).padStart(2,'0');
  const dd = String(d.getUTCDate()).padStart(2,'0');
  const y = d.getUTCFullYear();
  return `${m}/${dd}/${y}`;
}

function monthWindows(startIso, endIso) {
  const windows = [];
  const start = new Date(startIso+"T00:00:00Z");
  const end = new Date(endIso+"T00:00:00Z");
  let curr = new Date(Date.UTC(start.getUTCFullYear(), start.getUTCMonth(), 1));
  if (start.getUTCDate() !== 1) {
    const last = new Date(Date.UTC(start.getUTCFullYear(), start.getUTCMonth()+1, 0));
    windows.push([start, last < end ? last : end]);
    curr = new Date(Date.UTC(start.getUTCFullYear(), start.getUTCMonth()+1, 1));
  }
  while (curr <= end) {
    const mStart = new Date(Date.UTC(curr.getUTCFullYear(), curr.getUTCMonth(), 1));
    const mEnd = new Date(Date.UTC(curr.getUTCFullYear(), curr.getUTCMonth()+1, 0));
    windows.push([mStart, mEnd < end ? mEnd : end]);
    curr = new Date(Date.UTC(curr.getUTCFullYear(), curr.getUTCMonth()+1, 1));
  }
  return windows;
}

function runTask(t, attempt=0) {
  return new Promise((resolve) => {
    const ds = DS[t.dataset];
    if (!ds) return resolve({ task:t, ok:false, code:1, error:`Unknown dataset ${t.dataset}` });
    const script = path.join(process.cwd(), ds.script);
    const env = { ...process.env, DATA_ROOT };
    const args = [];
    if (t.kind === 'snapshot') {
      // For contracts/bids/vendors with label 'all', pass NO arguments
      if (t.label === 'all') {
        // No arguments needed - scrapers default to "all" mode
      } else if (t.label) {
        args.push(t.label);
      }
    } else if (t.kind === 'window') {
      // POs need date arguments in format: month day year
      const [sm, sd, sy] = t.start.split('/').map(s => parseInt(s));
      const [em, ed, ey] = t.end.split('/').map(s => parseInt(s));
      
      // Convert to month name for PO scraper
      const monthNames = ['', 'jan', 'feb', 'mar', 'apr', 'may', 'jun', 
                          'jul', 'aug', 'sep', 'oct', 'nov', 'dec'];
      const monthName = monthNames[sm];
      
      // Check if this is a full month window (starts on 1st, ends on last day)
      const startDate = new Date(sy, sm - 1, sd);
      const endDate = new Date(ey, em - 1, ed);
      const lastDayOfMonth = new Date(sy, sm, 0).getDate();
      
      if (sd === 1 && ed === lastDayOfMonth && sm === em && sy === ey) {
        // Full month window - pass month and year only
        args.push(monthName, String(sy));
      } else if (sm === em && sd === ed && sy === ey) {
        // Single day window
        args.push(monthName, String(sd), String(sy));
      } else {
        // Custom range - not supported by CLI, would need to pass raw dates
        console.error(`Warning: Non-standard window ${t.start} to ${t.end} - using month ${monthName} ${sy}`);
        args.push(monthName, String(sy));
      }
    }
    const child = spawn('node', [script, ...args], { stdio:'inherit', env });
    child.on('close', (code) => {
      if (code === 0) return resolve({ task:t, ok:true, code });
      if (attempt < RETRIES) {
        setTimeout(() => {
          runTask(t, attempt+1).then(resolve);
        }, 1000 * Math.pow(2, attempt));
      } else {
        resolve({ task:t, ok:false, code });
      }
    });
  });
}

async function runTasks(tasks) {
  const queue = tasks.slice();
  const active = new Set();
  const results = [];
  function spawnNext() {
    if (!queue.length) return;
    if (active.size >= CONCURRENCY) return;
    const t = queue.shift();
    const p = runTask(t).then(res => {
      active.delete(p); results.push(res); spawnNext();
    });
    active.add(p);
    spawnNext();
  }
  for (let i=0;i<CONCURRENCY;i++) spawnNext();
  await Promise.all([...active]);
  return results;
}

function usage() {
  console.log(`
Usage:
  node orchestrate.js plan auto       # Show plan without executing
  node orchestrate.js run auto        # Execute the auto plan
  node orchestrate.js run daily
  node orchestrate.js run nightly
  node orchestrate.js run backfill-auto  # Run bounded backfill (MAX_BACKFILL_WINDOWS=6)
  node orchestrate.js run backfill-pos --start 2018-01-31 --end 2025-08-21

Env:
  DATA_ROOT=./data (default)
  ORCH_CONCURRENCY=2
  ORCH_RETRIES=2
  MAX_BACKFILL_WINDOWS=6 (for backfill-auto)
`);
}

async function main() {
  const [cmd, subcmd, ...rest] = process.argv.slice(2);
  const args = {};
  for (let i=0;i<rest.length;i++) {
    const v = rest[i];
    if (v === '--start') args.start = rest[++i];
    else if (v === '--end') args.end = rest[++i];
  }

  let tasks = [];
  
  // Handle 'plan' command to show tasks without executing
  if (cmd === 'plan') {
    if (subcmd === 'auto') {
      const proc = spawnSync('node', [path.join('tools','plan-from-state.js')], {
        env: { ...process.env, DATA_ROOT },
        encoding: 'utf8'
      });
      if (proc.status !== 0) {
        console.error(proc.stdout || '');
        console.error(proc.stderr || '');
        process.exit(2);
      }
      console.log(proc.stdout);
      return;
    }
    // Add other plan modes here if needed
    return usage();
  }
  
  if (cmd !== 'run') return usage();
  if (subcmd === 'auto') {
    const proc = spawnSync('node', [path.join('tools','plan-from-state.js')], {
      env: { ...process.env, DATA_ROOT },
      encoding: 'utf8'
    });
    if (proc.status !== 0) {
      console.error(proc.stdout || '');
      console.error(proc.stderr || '');
      process.exit(2);
    }
    const payload = JSON.parse(proc.stdout || '{"tasks":[]}');
    tasks = payload.tasks || [];
  } else if (subcmd === 'daily') {
    const today = new Date();
    const yesterday = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate()-1));
    tasks.push({ kind:'snapshot', dataset:'contracts', label:'all' });
    tasks.push({ kind:'snapshot', dataset:'bids', label:'all' });
    tasks.push({ kind:'snapshot', dataset:'vendors', label:'all' });
    tasks.push({ kind:'window', dataset:'purchase_orders', start: toMMDDYYYY(yesterday), end: toMMDDYYYY(today), label: ymd(yesterday) });
  } else if (subcmd === 'nightly') {
    const today = new Date();
    const end = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate()));
    const start = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate()-14));
    tasks.push({ kind:'snapshot', dataset:'contracts', label:'all' });
    tasks.push({ kind:'snapshot', dataset:'bids', label:'all' });
    tasks.push({ kind:'snapshot', dataset:'vendors', label:'all' });
    for (let d = new Date(start); d <= end; d = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()+1))) {
      const next = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()+1));
      tasks.push({ kind:'window', dataset:'purchase_orders', start: toMMDDYYYY(d), end: toMMDDYYYY(next), label: ymd(d) });
    }
  } else if (subcmd === 'backfill-auto') {
    // State-aware bounded backfill that reads manifests and only processes missing months
    const maxWindows = parseInt(process.env.MAX_BACKFILL_WINDOWS || '6', 10);
    const proc = spawnSync('node', [path.join('tools','plan-from-state.js'), '--backfill'], {
      env: { ...process.env, DATA_ROOT },
      encoding: 'utf8'
    });
    if (proc.status !== 0) {
      console.error(proc.stdout || '');
      console.error(proc.stderr || '');
      process.exit(2);
    }
    const plan = JSON.parse(proc.stdout || '{"tasks":[]}');
    // Filter to only PO windows and limit to maxWindows
    const poWindows = (plan.tasks || [])
      .filter(t => t.dataset === 'purchase_orders' && t.kind === 'window')
      .slice(0, maxWindows);
    
    if (!poWindows.length) {
      console.log('✅ Backfill complete: no uncovered months remaining.');
      return;
    }
    
    console.log(`📊 Found ${plan.tasks.filter(t => t.dataset === 'purchase_orders' && t.kind === 'window').length} uncovered PO windows`);
    console.log(`🎯 Processing batch of ${poWindows.length} windows (max: ${maxWindows})`);
    tasks = poWindows;
  } else if (subcmd === 'backfill-pos') {
    const startIso = args.start || '2018-01-31';
    const endIso = args.end || new Date().toISOString().slice(0,10);
    const windows = monthWindows(startIso, endIso);
    for (const [s,e] of windows) {
      tasks.push({ kind:'window', dataset:'purchase_orders', start: toMMDDYYYY(s), end: toMMDDYYYY(e), label: ymd(s).slice(0,7) });
    }
  } else {
    return usage();
  }

  console.log(`Running ${tasks.length} tasks with concurrency=${CONCURRENCY}, retries=${RETRIES}`);
  const results = await runTasks(tasks);
  const ok = results.filter(r => r.ok).length;
  const bad = results.length - ok;
  console.log(`\nSummary: ${ok}/${results.length} succeeded, ${bad} failed`);
  if (bad) process.exitCode = 2;
}

if (require.main === module) {
  main().catch(err => { console.error(err); process.exit(1); });
}
