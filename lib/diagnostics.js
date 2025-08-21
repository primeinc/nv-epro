const fs = require('fs');
const fsp = fs.promises;
const path = require('path');

/**
 * Append NDJSON line to a file
 */
async function appendNdjson(filepath, obj) {
  try {
    await fsp.appendFile(filepath, JSON.stringify(obj) + '\n', 'utf8');
  } catch (e) {
    // Silent fail - don't let logging break the scraper
  }
}

/**
 * Safely create directory
 */
async function safeMkdir(dirPath) {
  try {
    await fsp.mkdir(dirPath, { recursive: true });
  } catch (e) {
    // Silent fail
  }
}

/**
 * Capture all diagnostics on failure
 */
async function captureDiagnostics(runtime, context, page, error, stage = 'unknown') {
  const diagDir = path.join(runtime.rawDir, 'diagnostics');
  await safeMkdir(diagDir);
  
  const results = {
    screenshot: false,
    html: false,
    trace: false,
    har: false,
    storage: false,
    errorFile: false,
    failMarker: false
  };
  
  // Capture screenshot
  if (page) {
    try {
      await page.screenshot({ 
        path: path.join(diagDir, 'screenshot.png'), 
        fullPage: true 
      });
      results.screenshot = true;
    } catch (e) {}
    
    // Capture HTML
    try {
      const html = await page.content();
      await fsp.writeFile(path.join(diagDir, 'page.html'), html, 'utf8');
      results.html = true;
    } catch (e) {}
  }
  
  // Stop and save trace
  if (context) {
    try {
      await context.tracing.stop({ 
        path: path.join(diagDir, 'trace.playwright.zip') 
      });
      results.trace = true;
    } catch (e) {}
    
    // Save storage state
    try {
      await context.storageState({ 
        path: path.join(diagDir, 'storage-state.json') 
      });
      results.storage = true;
    } catch (e) {}
  }
  
  // Write error summary
  try {
    const errorSummary = `ERROR SUMMARY
=============
Time: ${new Date().toISOString()}
Stage: ${stage}
URL: ${page ? await page.url().catch(() => 'unknown') : 'no page'}

Error Name: ${error.name || 'Error'}
Message: ${error.message || 'Unknown error'}

Stack Trace:
${error.stack || 'No stack trace available'}

Captured Diagnostics:
- Screenshot: ${results.screenshot ? 'Yes' : 'No'}
- HTML: ${results.html ? 'Yes' : 'No'}
- Trace: ${results.trace ? 'Yes' : 'No'}
- Storage: ${results.storage ? 'Yes' : 'No'}
`;
    await fsp.writeFile(path.join(diagDir, 'error.txt'), errorSummary, 'utf8');
    results.errorFile = true;
  } catch (e) {}
  
  // Write failure marker
  try {
    await fsp.writeFile(path.join(runtime.rawDir, '_FAILED'), '', 'utf8');
    results.failMarker = true;
  } catch (e) {}
  
  return results;
}

/**
 * Set up console and network logging
 */
function setupLogging(page, runtime) {
  const diagDir = path.join(runtime.rawDir, 'diagnostics');
  
  // Ensure diagnostics directory exists
  safeMkdir(diagDir);
  
  // Log console messages
  page.on('console', async msg => {
    await appendNdjson(path.join(diagDir, 'console.ndjson'), {
      ts: new Date().toISOString(),
      type: msg.type(),
      text: msg.text(),
      location: msg.location()
    });
  });
  
  // Log network requests
  page.on('requestfinished', async req => {
    try {
      const response = await req.response();
      await appendNdjson(path.join(diagDir, 'requests.ndjson'), {
        ts: new Date().toISOString(),
        url: req.url(),
        method: req.method(),
        status: response ? response.status() : null,
        statusText: response ? response.statusText() : null,
        timing: req.timing()
      });
    } catch (e) {
      // Silent fail - don't break on logging
    }
  });
  
  // Log page errors
  page.on('pageerror', async error => {
    await appendNdjson(path.join(diagDir, 'console.ndjson'), {
      ts: new Date().toISOString(),
      type: 'error',
      text: error.toString(),
      stack: error.stack
    });
  });
}

/**
 * Parse error for structured output
 */
function parseError(error, stage = 'unknown', page = null, selector = null) {
  return {
    name: error.name || 'Error',
    message: String(error.message || error),
    stack: String(error.stack || ''),
    stage: stage,
    url: null, // Will be filled async
    selector: selector,
    timestamp: new Date().toISOString()
  };
}

module.exports = {
  appendNdjson,
  safeMkdir,
  captureDiagnostics,
  setupLogging,
  parseError
};