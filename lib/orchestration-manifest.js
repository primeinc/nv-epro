/**
 * Orchestration Manifest Generator
 * 
 * Creates immutable orchestration manifests that tie together:
 * - All tasks executed
 * - Their run IDs and results
 * - URL validation results
 * - Forensic audit trail
 * 
 * Follows the principle: "The source of truth is the manifest"
 */

const fs = require('fs').promises;
const path = require('path');
const crypto = require('crypto');

function generateOrchestrationId() {
  const now = new Date();
  const ts = now.toISOString().replace(/[:-]/g, '').replace('T', 'T').replace('Z', '');
  const ms = String(now.getMilliseconds()).padStart(3, '0');
  const rand = crypto.randomBytes(3).toString('hex');
  return `orch_${ts}.${ms}Z_${rand}`;
}

async function createOrchestrationManifest(config) {
  const {
    orchId,
    command,
    tasks,
    results,
    validationReport,
    dataRoot,
    startTime,
    endTime
  } = config;

  const manifest = {
    schemaVersion: '1.0',
    orchestration: {
      id: orchId,
      command,
      startTime: startTime.toISOString(),
      endTime: endTime.toISOString(),
      durationMs: endTime - startTime,
      tasksTotal: tasks.length,
      tasksSucceeded: results.filter(r => r && r.ok).length,
      tasksFailed: results.filter(r => r && !r.ok).length,
      tasksSkipped: results.filter(r => r && r.skipped).length
    },
    tasks: tasks.map((task, idx) => {
      const result = results[idx];
      return {
        index: idx,
        dataset: task.dataset,
        kind: task.kind,
        label: task.label || null,
        startDate: task.start || null,
        endDate: task.end || null,
        result: result ? {
          ok: result.ok,
          exitCode: result.code,
          skipped: result.skipped || false,
          error: result.error || null,
          runId: result.runId || null,
          manifestPath: result.manifestPath ? path.resolve(result.manifestPath) : null
        } : {
          ok: false,
          exitCode: null,
          skipped: false,
          error: 'Task interrupted - no result available',
          runId: null,
          manifestPath: null
        }
      };
    }),
    validation: validationReport ? {
      timestamp: validationReport.timestamp,
      summary: validationReport.summary,
      datasets: Object.keys(validationReport.datasets || {}).map(ds => ({
        dataset: ds,
        tested: validationReport.datasets[ds].tested,
        passed: validationReport.datasets[ds].passed,
        failed: validationReport.datasets[ds].failed,
        samples: (validationReport.datasets[ds].samples || []).length
      }))
    } : null,
    environment: {
      dataRoot,
      platform: process.platform,
      nodeVersion: process.version,
      cwd: process.cwd()
    }
  };

  // Create orchestration directory
  const orchDir = path.join(dataRoot, 'orchestrations', 
    startTime.getFullYear().toString(),
    String(startTime.getMonth() + 1).padStart(2, '0'),
    String(startTime.getDate()).padStart(2, '0'),
    orchId
  );
  
  await fs.mkdir(orchDir, { recursive: true });
  
  // Write manifest
  const manifestPath = path.join(orchDir, 'manifest.json');
  await fs.writeFile(manifestPath, JSON.stringify(manifest, null, 2));
  
  // Copy validation report if it exists
  if (validationReport) {
    const validationPath = path.join(orchDir, 'url-validation.json');
    await fs.writeFile(validationPath, JSON.stringify(validationReport, null, 2));
  }
  
  return { manifest, manifestPath, orchDir };
}

/**
 * Extract run ID from scraper output
 */
function extractRunId(taskResult) {
  // Try to find run ID from the task's child process output
  // This would need to be captured from the scraper output
  // For now, return null - would need to modify runTask to capture this
  return null;
}

/**
 * Find manifest path for a completed task
 */
async function findManifestPath(task, dataRoot) {
  if (!task.dataset) return null;
  
  const today = new Date().toISOString().slice(0, 10);
  const [year, month, day] = today.split('-');
  const basePath = path.join(dataRoot, 'nevada-epro', task.dataset, 'raw', year, month, day);
  
  try {
    const runs = await fs.readdir(basePath);
    const runDirs = runs.filter(d => d.startsWith('run_')).sort().reverse();
    
    for (const runDir of runDirs) {
      const manifestPath = path.join(basePath, runDir, 'manifest.json');
      try {
        const manifest = JSON.parse(await fs.readFile(manifestPath, 'utf8'));
        if (manifest.run?.dataset === task.dataset && 
            manifest.run?.label === (task.label || 'all')) {
          return manifestPath;
        }
      } catch (e) {
        console.error(`Warning: Failed to parse manifest ${manifestPath}:`, e.message);
        // Invalid manifest, continue
      }
    }
  } catch (e) {
    // Directory doesn't exist
  }
  
  return null;
}

module.exports = {
  generateOrchestrationId,
  createOrchestrationManifest,
  extractRunId,
  findManifestPath
};