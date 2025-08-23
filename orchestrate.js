
// orchestrate.js (state-aware variant)
// Adds `run auto` which invokes tools/plan-from-state.js to compute tasks from DATA_ROOT.
const { spawn, spawnSync } = require('child_process');
const path = require('path');
const fs = require('fs');

const DATA_ROOT = process.env.DATA_ROOT || path.join(process.cwd(), 'data');
const CONCURRENCY = parseInt(process.env.ORCH_CONCURRENCY || '2', 10);
const RETRIES = parseInt(process.env.ORCH_RETRIES || '2', 10);

const DS = {
  contracts: { script: 'scripts/scrape-contract-cli.js', mode: 'snapshot' },
  bids: { script: 'scripts/scrape-bid-cli.js', mode: 'snapshot' },
  vendors: { script: 'scripts/scrape-vendor-cli.js', mode: 'snapshot' },
  purchase_orders: { script: 'scripts/scrape-po-cli.js', mode: 'windowed' }
};

// Track active lock files for cleanup on process exit
const activeLockFiles = new Set();

// Register process-wide cleanup handlers once
process.on('exit', () => {
  for (const lockFile of activeLockFiles) {
    removeLockFile(lockFile);
  }
});

process.on('SIGINT', () => {
  for (const lockFile of activeLockFiles) {
    removeLockFile(lockFile);
  }
  process.exit(130);
});

process.on('SIGTERM', () => {
  for (const lockFile of activeLockFiles) {
    removeLockFile(lockFile);
  }
  process.exit(143);
});

function ymd(d) { return d.toISOString().slice(0,10); }
function toMMDDYYYY(d) {
  const m = String(d.getUTCMonth()+1).padStart(2,'0');
  const dd = String(d.getUTCDate()).padStart(2,'0');
  const y = d.getUTCFullYear();
  return `${m}/${dd}/${y}`;
}

// Check if a task has already been successfully completed today
async function hasSuccessfulRunToday(dataset, label) {
  const today = new Date().toISOString().slice(0, 10);
  const basePath = path.join(DATA_ROOT, 'nevada-epro', dataset, 'raw');
  
  // Parse today's date to get year/month/day structure
  const [year, month, day] = today.split('-');
  const dayPath = path.join(basePath, year, month, day);
  
  // Check if directory exists
  if (!fs.existsSync(dayPath)) return false;
  
  // Scan all run directories for today
  const runs = fs.readdirSync(dayPath).filter(d => d.startsWith('run_'));
  
  for (const runDir of runs) {
    const manifestPath = path.join(dayPath, runDir, 'manifest.json');
    if (!fs.existsSync(manifestPath)) continue;
    
    try {
      const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
      // Check if this manifest matches our task and was successful
      if (manifest.run?.dataset === dataset && 
          manifest.run?.label === label && 
          manifest.run?.status === 'success') {
        return true;
      }
    } catch (e) {
      console.error(`Warning: Failed to parse manifest ${manifestPath}:`, e.message);
      // Invalid manifest, skip
    }
  }
  
  return false;
}

// Create a lock file to prevent concurrent execution of the same task
function createLockFile(dataset, label) {
  const lockDir = path.join(DATA_ROOT, '.locks');
  if (!fs.existsSync(lockDir)) {
    fs.mkdirSync(lockDir, { recursive: true });
  }
  
  const lockFile = path.join(lockDir, `${dataset}_${label}.lock`);
  
  // Check for existing lock
  if (fs.existsSync(lockFile)) {
    const stats = fs.statSync(lockFile);
    const ageMs = Date.now() - stats.mtimeMs;
    const oneHour = 60 * 60 * 1000;
    
    if (ageMs < oneHour) {
      // Lock is fresh, another process is running
      return false;
    }
    // Lock is stale, remove it
    fs.unlinkSync(lockFile);
  }
  
  // Create new lock with PID and timestamp
  fs.writeFileSync(lockFile, JSON.stringify({
    pid: process.pid,
    started: new Date().toISOString()
  }));
  
  activeLockFiles.add(lockFile);
  return lockFile;
}

function removeLockFile(lockFile) {
  if (lockFile && fs.existsSync(lockFile)) {
    fs.unlinkSync(lockFile);
    activeLockFiles.delete(lockFile);
  }
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
  return new Promise(async (resolve) => {
    const ds = DS[t.dataset];
    if (!ds) return resolve({ task:t, ok:false, code:1, error:`Unknown dataset ${t.dataset}` });
    
    // Skip 'note' tasks - they're just informational
    if (t.kind === 'note') {
      return resolve({ task:t, ok:true, code:0, skipped:true });
    }
    
    // Check for idempotence - has this task already succeeded today?
    const label = t.label || 'unknown';
    if (await hasSuccessfulRunToday(t.dataset, label)) {
      console.log(`⏭️  Skipping ${t.dataset}/${label} - already completed successfully today`);
      return resolve({ task:t, ok:true, code:0, skipped:true });
    }
    
    // Try to acquire lock
    const lockFile = createLockFile(t.dataset, label);
    if (!lockFile) {
      console.log(`🔒 Cannot acquire lock for ${t.dataset}/${label} - another process is running`);
      return resolve({ task:t, ok:false, code:1, error:'Lock acquisition failed' });
    }
    
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
    // Capture output to extract run ID
    let output = '';
    let runId = null;
    let manifestPath = null;
    
    const child = spawn('node', [script, ...args], { env });
    
    // Capture stdout
    child.stdout.on('data', (data) => {
      const text = data.toString();
      process.stdout.write(text); // Still show output
      output += text;
      
      // Extract run ID from output
      const runIdMatch = text.match(/Run ID: (run_[^\s]+)/);
      if (runIdMatch) {
        runId = runIdMatch[1];
      }
      
      // Extract manifest path
      const manifestMatch = text.match(/Manifest: (.+manifest\.json)/);
      if (manifestMatch) {
        manifestPath = manifestMatch[1].trim();
      }
    });
    
    // Pass through stderr
    child.stderr.on('data', (data) => {
      process.stderr.write(data);
    });
    
    child.on('close', (code) => {
      removeLockFile(lockFile);  // Clean up lock file
      if (code === 0) {
        return resolve({ task:t, ok:true, code, runId, manifestPath });
      }
      if (attempt < RETRIES) {
        setTimeout(() => {
          runTask(t, attempt+1).then(resolve);
        }, 1000 * Math.pow(2, attempt));
      } else {
        resolve({ task:t, ok:false, code, runId, manifestPath });
      }
    });
    
    // Lock cleanup is handled by global process handlers
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
  node orchestrate.js plan auto    # Show plan without executing
  node orchestrate.js run auto     # Execute auto plan (snapshots + PO current month + full backfill if no data)

Env:
  DATA_ROOT=./data (default)
  ORCH_CONCURRENCY=2
  ORCH_RETRIES=2
  MAX_BACKFILL_WINDOWS=6 (for backfill-auto)
`);
}

async function runUrlValidation() {
  console.log('\n🔍 Running URL validation...');
  const { spawn } = require('child_process');
  
  return new Promise((resolve) => {
    const child = spawn('node', [path.join('tools', 'validate-urls.js')], {
      stdio: 'inherit',
      env: { ...process.env, DATA_ROOT, URL_VALIDATION_SAMPLES: '3' }
    });
    
    child.on('close', (code) => {
      if (code !== 0) {
        console.error('⚠️  URL validation detected issues - check url-validation-report.json');
      }
      resolve(code);
    });
  });
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
  
  // Import orchestration manifest generator
  const { generateOrchestrationId, createOrchestrationManifest } = require('./lib/orchestration-manifest');
  
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
  } else {
    return usage();
  }

  // Generate orchestration ID and track timing
  const orchId = generateOrchestrationId();
  const startTime = new Date();
  console.log(`🎯 Orchestration ID: ${orchId}`);
  console.log(`Running ${tasks.length} tasks with concurrency=${CONCURRENCY}, retries=${RETRIES}`);
  
  const results = await runTasks(tasks);
  const ok = results.filter(r => r.ok).length;
  const bad = results.length - ok;
  console.log(`\nSummary: ${ok}/${results.length} succeeded, ${bad} failed`);
  
  // Run URL validation after tasks complete (but only if we had scraping tasks)
  let validationReport = null;
  const scrapingTasks = results.filter(r => !r.skipped && r.task.kind !== 'note');
  if (scrapingTasks.length > 0) {
    const validationCode = await runUrlValidation();
    if (validationCode !== 0) {
      console.error('⚠️  URL builder may need updates - patterns might have changed');
      // Don't fail the whole run, just warn
    }
    
    // Load validation report
    try {
      const reportPath = path.join(DATA_ROOT, 'url-validation-report.json');
      validationReport = JSON.parse(fs.readFileSync(reportPath, 'utf8'));
    } catch (e) {
      console.error('⚠️  Could not load validation report:', e.message);
    }
  }
  
  const endTime = new Date();
  
  // Create orchestration manifest
  try {
    const { manifestPath } = await createOrchestrationManifest({
      orchId,
      command: `node orchestrate.js ${process.argv.slice(2).join(' ')}`,
      tasks,
      results,
      validationReport,
      dataRoot: DATA_ROOT,
      startTime,
      endTime
    });
    
    console.log(`\n📋 Orchestration manifest: ${path.relative(process.cwd(), manifestPath)}`);
  } catch (e) {
    console.error('⚠️  Could not create orchestration manifest:', e.message);
  }
  
  if (bad) process.exitCode = 2;
}

if (require.main === module) {
  main().catch(err => { console.error(err); process.exit(1); });
}
