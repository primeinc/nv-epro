#!/usr/bin/env node

/**
 * OpenLineage Event Emitter
 * 
 * Emits OpenLineage-compliant events for data pipeline observability.
 * Follows OpenLineage 1.0 specification with Great Expectations facets.
 * 
 * @see https://openlineage.io/docs/spec/object-model
 * @see https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json
 */

const crypto = require('crypto');
const fs = require('fs').promises;
const path = require('path');

/**
 * OpenLineage namespaces and producers
 */
const OPENLINEAGE_NAMESPACE = process.env.OPENLINEAGE_NAMESPACE || 'bronze://nevada-epro';
const OPENLINEAGE_PRODUCER = process.env.OPENLINEAGE_PRODUCER || 'nv-epro://bronze-validation/0.1.0';
const OPENLINEAGE_SCHEMA_URL = 'https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs';

/**
 * Generate UUID v4 for run IDs
 */
function generateUUID() {
  return crypto.randomUUID();
}

/**
 * Create base OpenLineage event structure
 */
function createBaseEvent(eventType, job, run) {
  return {
    eventType,
    eventTime: new Date().toISOString(),
    producer: OPENLINEAGE_PRODUCER,
    schemaURL: `${OPENLINEAGE_SCHEMA_URL}/${eventType}Event`,
    job: {
      namespace: OPENLINEAGE_NAMESPACE,
      name: job.name,
      facets: job.facets || {}
    },
    run: {
      runId: run.runId,
      facets: run.facets || {}
    },
    inputs: [],
    outputs: []
  };
}

/**
 * Create dataset reference
 */
function createDataset(name, namespace = OPENLINEAGE_NAMESPACE, facets = {}) {
  return {
    namespace,
    name,
    facets: {
      schema: facets.schema || null,
      dataSource: facets.dataSource || null,
      lifecycleStateChange: facets.lifecycleStateChange || null,
      version: facets.version || null,
      ...facets
    }
  };
}

/**
 * Create Great Expectations validation facet
 * Follows GE ExpectationSuiteValidationResult format
 */
function createDataQualityFacet(validationResults) {
  return {
    '_producer': OPENLINEAGE_PRODUCER,
    '_schemaURL': `${OPENLINEAGE_SCHEMA_URL}/facets/DataQualityMetricsInputDatasetFacet`,
    columnMetrics: validationResults.columnMetrics || {},
    rowCount: validationResults.rowCount || null,
    bytes: validationResults.bytes || null,
    // Great Expectations compatible fields
    assertions: validationResults.assertions || [],
    success: validationResults.success !== false,
    statistics: validationResults.statistics || {}
  };
}

/**
 * Create SQL job facet for transformations
 */
function createSqlJobFacet(query) {
  return {
    '_producer': OPENLINEAGE_PRODUCER,
    '_schemaURL': `${OPENLINEAGE_SCHEMA_URL}/facets/SQLJobFacet`,
    query
  };
}

/**
 * Create Bronze ingestion START event
 */
function createBronzeIngestionStartEvent(options) {
  const { dataset, sourcePath, runId = generateUUID() } = options;
  
  const event = createBaseEvent('START', 
    {
      name: `bronze.ingest.${dataset}`,
      facets: {
        documentation: {
          '_producer': OPENLINEAGE_PRODUCER,
          '_schemaURL': `${OPENLINEAGE_SCHEMA_URL}/facets/DocumentationJobFacet`,
          description: `Ingest ${dataset} CSV to Bronze layer with content-addressed storage`
        }
      }
    },
    {
      runId,
      facets: {
        nominalTime: {
          '_producer': OPENLINEAGE_PRODUCER,
          '_schemaURL': `${OPENLINEAGE_SCHEMA_URL}/facets/NominalTimeRunFacet`,
          nominalStartTime: new Date().toISOString()
        }
      }
    }
  );
  
  // Add input dataset (source CSV)
  event.inputs.push(createDataset(
    sourcePath,
    'file://raw',
    {
      dataSource: {
        '_producer': OPENLINEAGE_PRODUCER,
        '_schemaURL': `${OPENLINEAGE_SCHEMA_URL}/facets/DatasourceDatasetFacet`,
        name: 'filesystem',
        uri: `file://${path.resolve(sourcePath)}`
      }
    }
  ));
  
  return event;
}

/**
 * Create Bronze ingestion COMPLETE event
 */
function createBronzeIngestionCompleteEvent(options) {
  const { 
    dataset, 
    sourcePath, 
    outputPath,
    runId,
    rowCount,
    byteSize,
    fileHash,
    duration,
    error = null
  } = options;
  
  const eventType = error ? 'FAIL' : 'COMPLETE';
  
  const event = createBaseEvent(eventType,
    {
      name: `bronze.ingest.${dataset}`,
      facets: {}
    },
    {
      runId,
      facets: {
        nominalTime: {
          '_producer': OPENLINEAGE_PRODUCER,
          '_schemaURL': `${OPENLINEAGE_SCHEMA_URL}/facets/NominalTimeRunFacet`,
          nominalEndTime: new Date().toISOString()
        }
      }
    }
  );
  
  // Add processing metrics
  if (!error) {
    event.run.facets.processing = {
      '_producer': OPENLINEAGE_PRODUCER,
      '_schemaURL': `${OPENLINEAGE_SCHEMA_URL}/facets/ProcessingEngineRunFacet`,
      version: '0.10.0',
      name: 'duckdb',
      processingTime: duration
    };
    
    // Add output dataset (Bronze parquet)
    event.outputs.push(createDataset(
      outputPath,
      'bronze://nevada-epro',
      {
        dataSource: {
          '_producer': OPENLINEAGE_PRODUCER,
          '_schemaURL': `${OPENLINEAGE_SCHEMA_URL}/facets/DatasourceDatasetFacet`,
          name: 'bronze-layer',
          uri: `file://${path.resolve(outputPath)}`
        },
        storage: {
          '_producer': OPENLINEAGE_PRODUCER,
          '_schemaURL': `${OPENLINEAGE_SCHEMA_URL}/facets/StorageDatasetFacet`,
          storageLayer: 'bronze',
          fileFormat: 'parquet',
          contentHash: fileHash
        },
        dataQualityMetrics: createDataQualityFacet({
          rowCount,
          bytes: byteSize,
          success: true
        })
      }
    ));
  } else {
    // Add error facet
    event.run.facets.errorMessage = {
      '_producer': OPENLINEAGE_PRODUCER,
      '_schemaURL': `${OPENLINEAGE_SCHEMA_URL}/facets/ErrorMessageRunFacet`,
      message: error.message,
      programmingLanguage: 'javascript',
      stackTrace: error.stack
    };
  }
  
  return event;
}

/**
 * Create Bronze validation event with Great Expectations results
 */
function createBronzeValidationEvent(options) {
  const {
    dataset,
    bronzePath,
    validationResults,
    runId = generateUUID()
  } = options;
  
  const event = createBaseEvent('COMPLETE',
    {
      name: `bronze.validate.${dataset}`,
      facets: {
        documentation: {
          '_producer': OPENLINEAGE_PRODUCER,
          '_schemaURL': `${OPENLINEAGE_SCHEMA_URL}/facets/DocumentationJobFacet`,
          description: `Validate Bronze layer data quality for ${dataset}`
        }
      }
    },
    {
      runId,
      facets: {}
    }
  );
  
  // Convert validation results to Great Expectations format
  const geResults = {
    success: validationResults.status === 'healthy',
    statistics: {
      evaluated_expectations: validationResults.expectations?.length || 0,
      successful_expectations: validationResults.expectations?.filter(e => e.success).length || 0,
      unsuccessful_expectations: validationResults.expectations?.filter(e => !e.success).length || 0
    },
    results: validationResults.expectations || [],
    meta: {
      run_id: runId,
      batch_kwargs: {
        path: bronzePath,
        datasource: 'bronze_layer'
      },
      expectation_suite_name: `bronze.${dataset}.expectations`,
      great_expectations_version: '0.15.0'  // GE compatible version
    }
  };
  
  // Add validation facet
  event.run.facets.dataQuality = {
    '_producer': OPENLINEAGE_PRODUCER,
    '_schemaURL': `${OPENLINEAGE_SCHEMA_URL}/facets/DataQualityAssertionsRunFacet`,
    assertions: geResults.results.map(exp => ({
      assertion: exp.expectation_type,
      success: exp.success,
      column: exp.kwargs?.column || null
    }))
  };
  
  // Add input dataset with quality metrics
  event.inputs.push(createDataset(
    bronzePath,
    'bronze://nevada-epro',
    {
      dataQualityMetrics: createDataQualityFacet({
        rowCount: validationResults.metrics?.volume?.row_count,
        bytes: validationResults.metrics?.volume?.bytes_max,
        success: geResults.success,
        columnMetrics: validationResults.distribution || {},
        assertions: geResults.results
      }),
      greatExpectations: {
        '_producer': OPENLINEAGE_PRODUCER,
        '_schemaURL': `${OPENLINEAGE_SCHEMA_URL}/facets/GreatExpectationsAssertionsDatasetFacet`,
        assertions: geResults.results
      }
    }
  ));
  
  return { event, validationResult: geResults };
}

/**
 * Emit event to configured backend
 * Writes NDJSON to logs/lineage/*.ndjson for streaming consumption
 */
async function emitEvent(event, options = {}) {
  const { 
    console: logToConsole = false,
    file: saveToFile = true,
    backend: sendToBackend = false
  } = options;
  
  // Validate event structure
  if (!event.eventType || !event.job || !event.run) {
    throw new Error('Invalid OpenLineage event structure');
  }
  
  // Write NDJSON to logs for streaming
  if (saveToFile) {
    const logsDir = path.join('logs', 'lineage');
    await fs.mkdir(logsDir, { recursive: true });
    
    // NDJSON format - one event per line
    const date = new Date().toISOString().split('T')[0];
    const ndjsonPath = path.join(logsDir, `${date}.ndjson`);
    await fs.appendFile(ndjsonPath, JSON.stringify(event) + '\n');
    
    // Also save individual event for debugging
    const eventsDir = path.join(logsDir, 'events');
    await fs.mkdir(eventsDir, { recursive: true });
    const filename = `${event.job.name.replace(/\./g, '_')}_${event.run.runId}_${event.eventType}.json`;
    const filepath = path.join(eventsDir, filename);
    await fs.writeFile(filepath, JSON.stringify(event, null, 2));
  }
  
  // Log to console if requested (NDJSON format)
  if (logToConsole) {
    console.log(JSON.stringify(event));
  }
  
  // Send to backend if configured
  if (sendToBackend && process.env.OPENLINEAGE_URL) {
    const fetch = require('node-fetch');
    const response = await fetch(`${process.env.OPENLINEAGE_URL}/api/v1/lineage`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${process.env.OPENLINEAGE_API_KEY || ''}`
      },
      body: JSON.stringify(event)
    });
    
    if (!response.ok) {
      throw new Error(`Failed to emit event: ${response.statusText}`);
    }
  }
  
  return event;
}

/**
 * Create Great Expectations expectation results
 */
function createExpectation(type, kwargs, result) {
  return {
    expectation_config: {
      expectation_type: type,
      kwargs
    },
    result: {
      observed_value: result.observed_value,
      element_count: result.element_count || null,
      missing_count: result.missing_count || null,
      missing_percent: result.missing_percent || null,
      unexpected_count: result.unexpected_count || null,
      unexpected_percent: result.unexpected_percent || null
    },
    success: result.success,
    meta: {},
    exception_info: result.exception_info || null
  };
}

/**
 * Generate deterministic run ID from inputs
 */
function generateRunId(eventTime, datasetPath, gitSha = 'unknown') {
  const hash = crypto.createHash('sha256');
  hash.update(eventTime);
  hash.update(datasetPath);
  hash.update(gitSha);
  return `bronze-${hash.digest('hex').substring(0, 16)}`;
}

/**
 * Write NDJSON log for OpenTelemetry compatibility
 */
async function writeOTelLog(severity, body, attributes = {}) {
  const log = {
    timestamp: new Date().toISOString(),
    severity,
    body,
    attributes,
    traceId: attributes.runId || null,
    spanId: crypto.randomBytes(8).toString('hex')
  };
  
  const logsDir = path.join('logs', 'otel');
  await fs.mkdir(logsDir, { recursive: true });
  
  const date = new Date().toISOString().split('T')[0];
  const logPath = path.join(logsDir, `${date}.ndjson`);
  await fs.appendFile(logPath, JSON.stringify(log) + '\n');
  
  return log;
}

module.exports = {
  generateUUID,
  generateRunId,
  createBaseEvent,
  createDataset,
  createDataQualityFacet,
  createSqlJobFacet,
  createBronzeIngestionStartEvent,
  createBronzeIngestionCompleteEvent,
  createBronzeValidationEvent,
  createExpectation,
  emitEvent,
  writeOTelLog,
  OPENLINEAGE_NAMESPACE,
  OPENLINEAGE_PRODUCER
};