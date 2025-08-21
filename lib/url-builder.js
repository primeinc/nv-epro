#!/usr/bin/env node

/**
 * Nevada ePro URL Builder and Validator
 * 
 * Reconstructs deeplink URLs from record IDs in CSV data
 * Validates URL patterns and provides forensic audit trail capability
 */

const BASE_URL = 'https://nevadaepro.com';

/**
 * URL Templates for each dataset type
 * These patterns were discovered through browser investigation
 */
const URL_PATTERNS = {
  // Purchase Orders use poSummary.sdo endpoint
  purchase_order: {
    template: '/bso/external/purchaseorder/poSummary.sdo?docId={ID}&releaseNbr=0&external=true&parentUrl=close',
    idField: 'PO #',
    urlField: 'po_url',
    examples: ['72DOW-NV18-6', '44DOC-NV18-5', '72DOW-NV18-2']
  },
  
  // Contracts use the same endpoint as POs
  contract: {
    template: '/bso/external/purchaseorder/poSummary.sdo?docId={ID}&releaseNbr=0&external=true&parentUrl=close',
    idField: 'Contract #',
    urlField: 'contract_url',
    examples: ['80DOT-MC3960', '08DOA-MC5021']
  },
  
  // Bids have two possible URLs
  bid: {
    detail: {
      template: '/bso/external/bidDetail.sdo?docId={ID}&external=true&parentUrl=close',
      idField: 'Bid Solicitation #',
      urlField: 'bid_detail_url',
      examples: ['30DOE-S3449', '69CRC-S3446', '65DPS-S3437']
    },
    holderList: {
      template: '/bso/external/bidAckList.sdo?bidId={ID}',
      idField: 'Bid Solicitation #',
      urlField: 'bid_holder_list_url',
      examples: ['65DPS-S3437', '30DOE-S3418'],
      conditional: true // Only some bids have holder lists
    }
  },
  
  // Vendors use vendorProfileOrgInfo.sda endpoint
  vendor: {
    template: '/bso/external/vendor/vendorProfileOrgInfo.sda?external=true&vendorId={ID}',
    idField: 'Vendor ID',
    urlField: 'vendor_url',
    examples: ['VEN30280', 'VEN28064', 'VEN5639']
  }
};

/**
 * Build URL from record ID
 * @param {string} recordId - The ID from the CSV (e.g., "72DOW-NV18-6")
 * @param {string} recordType - Type of record ('purchase_order', 'contract', 'bid', 'vendor')
 * @param {string} urlType - For bids: 'detail' or 'holderList'
 * @returns {string} The full URL
 */
function buildUrl(recordId, recordType, urlType = null) {
  if (!recordId) {
    throw new Error('Record ID is required');
  }
  
  let pattern;
  if (recordType === 'bid' && urlType) {
    pattern = URL_PATTERNS.bid[urlType];
  } else {
    pattern = URL_PATTERNS[recordType];
  }
  
  if (!pattern) {
    throw new Error(`Unknown record type: ${recordType}${urlType ? '/' + urlType : ''}`);
  }
  
  const url = pattern.template.replace('{ID}', encodeURIComponent(recordId));
  return BASE_URL + url;
}

/**
 * Validate URL pattern matches expected format
 * @param {string} url - URL to validate
 * @param {string} recordType - Expected record type
 * @returns {object} Validation result with extracted ID
 */
function validateUrl(url, recordType) {
  const result = {
    valid: false,
    recordId: null,
    recordType: null,
    errors: []
  };
  
  // Check base URL
  if (!url.startsWith(BASE_URL)) {
    result.errors.push(`URL must start with ${BASE_URL}`);
    return result;
  }
  
  // Extract path
  const path = url.substring(BASE_URL.length);
  
  // Check against patterns
  if (recordType === 'purchase_order' || recordType === 'contract') {
    const match = path.match(/\/bso\/external\/purchaseorder\/poSummary\.sdo\?docId=([^&]+)&releaseNbr=0&external=true&parentUrl=close/);
    if (match) {
      result.valid = true;
      result.recordId = decodeURIComponent(match[1]);
      result.recordType = recordType;
    } else {
      result.errors.push('URL does not match PO/Contract pattern');
    }
  } else if (recordType === 'bid') {
    // Check bid detail pattern
    let match = path.match(/\/bso\/external\/bidDetail\.sdo\?docId=([^&]+)&external=true&parentUrl=close/);
    if (match) {
      result.valid = true;
      result.recordId = decodeURIComponent(match[1]);
      result.recordType = 'bid_detail';
      return result;
    }
    
    // Check bid holder list pattern
    match = path.match(/\/bso\/external\/bidAckList\.sdo\?bidId=([^&]+)/);
    if (match) {
      result.valid = true;
      result.recordId = decodeURIComponent(match[1]);
      result.recordType = 'bid_holder_list';
      return result;
    }
    
    result.errors.push('URL does not match any Bid pattern');
  } else if (recordType === 'vendor') {
    const match = path.match(/\/bso\/external\/vendor\/vendorProfileOrgInfo\.sda\?external=true&vendorId=([^&]+)/);
    if (match) {
      result.valid = true;
      result.recordId = decodeURIComponent(match[1]);
      result.recordType = 'vendor';
    } else {
      result.errors.push('URL does not match Vendor pattern');
    }
  }
  
  return result;
}

/**
 * Validate ID format for consistency
 * @param {string} recordId - The record ID to validate
 * @param {string} recordType - The type of record
 * @returns {object} Validation result
 */
function validateIdFormat(recordId, recordType) {
  const result = {
    valid: false,
    warnings: [],
    format: null
  };
  
  if (!recordId) {
    result.warnings.push('Empty record ID');
    return result;
  }
  
  // Known ID patterns from investigation
  const patterns = {
    purchase_order: {
      // Examples: 72DOW-NV18-6, 44DOC-NV18-5
      regex: /^\d{2}[A-Z]+-NV\d{2}-\d+$/,
      format: '##XXX-NV##-#',
      description: '2-digit prefix + dept code + -NV + year + sequence'
    },
    contract: {
      // Examples: 80DOT-MC3960, 08DOA-MC5021
      regex: /^\d{2}[A-Z]+-MC\d+$/,
      format: '##XXX-MC####',
      description: '2-digit prefix + dept code + -MC + number'
    },
    bid: {
      // Examples: 30DOE-S3449, 69CRC-S3446
      regex: /^(\d{2}[A-Z]+-S\d+|[A-Z]+-S\d+)$/,
      format: '##XXX-S#### or XXX-S####',
      description: 'dept code + -S + number'
    },
    vendor: {
      // Examples: VEN30280, VEN28064
      regex: /^VEN\d+$/,
      format: 'VEN#####',
      description: 'VEN + numeric ID'
    }
  };
  
  const pattern = patterns[recordType];
  if (!pattern) {
    result.warnings.push(`Unknown record type: ${recordType}`);
    return result;
  }
  
  if (pattern.regex.test(recordId)) {
    result.valid = true;
    result.format = pattern.format;
  } else {
    result.warnings.push(`ID does not match expected format: ${pattern.format}`);
    result.warnings.push(`Expected: ${pattern.description}`);
    result.format = pattern.format;
  }
  
  return result;
}

/**
 * Process a CSV row and add URL columns
 * @param {object} row - CSV row object
 * @param {string} recordType - Type of record
 * @returns {object} Row with URL columns added
 */
function addUrlsToRow(row, recordType) {
  const enhanced = { ...row };
  
  if (recordType === 'purchase_order') {
    const poId = row[URL_PATTERNS.purchase_order.idField];
    if (poId) {
      enhanced[URL_PATTERNS.purchase_order.urlField] = buildUrl(poId, 'purchase_order');
    }
  } else if (recordType === 'contract') {
    const contractId = row[URL_PATTERNS.contract.idField];
    if (contractId) {
      enhanced[URL_PATTERNS.contract.urlField] = buildUrl(contractId, 'contract');
    }
  } else if (recordType === 'bid') {
    const bidId = row[URL_PATTERNS.bid.detail.idField];
    if (bidId) {
      enhanced[URL_PATTERNS.bid.detail.urlField] = buildUrl(bidId, 'bid', 'detail');
      
      // Only add holder list URL if "View List" is present in the row
      if (row['Bid Holder List'] && row['Bid Holder List'].includes('View List')) {
        enhanced[URL_PATTERNS.bid.holderList.urlField] = buildUrl(bidId, 'bid', 'holderList');
      }
    }
  } else if (recordType === 'vendor') {
    const vendorId = row[URL_PATTERNS.vendor.idField];
    if (vendorId) {
      enhanced[URL_PATTERNS.vendor.urlField] = buildUrl(vendorId, 'vendor');
    }
  }
  
  return enhanced;
}

/**
 * Generate test URLs for validation
 * @returns {object} Test URLs for each record type
 */
function generateTestUrls() {
  const tests = {};
  
  // Purchase Orders
  tests.purchase_orders = URL_PATTERNS.purchase_order.examples.map(id => ({
    id,
    url: buildUrl(id, 'purchase_order'),
    expected: `${BASE_URL}/bso/external/purchaseorder/poSummary.sdo?docId=${encodeURIComponent(id)}&releaseNbr=0&external=true&parentUrl=close`
  }));
  
  // Contracts
  tests.contracts = URL_PATTERNS.contract.examples.map(id => ({
    id,
    url: buildUrl(id, 'contract'),
    expected: `${BASE_URL}/bso/external/purchaseorder/poSummary.sdo?docId=${encodeURIComponent(id)}&releaseNbr=0&external=true&parentUrl=close`
  }));
  
  // Bids (both URL types)
  tests.bids = URL_PATTERNS.bid.detail.examples.map(id => ({
    id,
    detailUrl: buildUrl(id, 'bid', 'detail'),
    expectedDetail: `${BASE_URL}/bso/external/bidDetail.sdo?docId=${encodeURIComponent(id)}&external=true&parentUrl=close`
  }));
  
  tests.bidHolderLists = URL_PATTERNS.bid.holderList.examples.map(id => ({
    id,
    holderUrl: buildUrl(id, 'bid', 'holderList'),
    expectedHolder: `${BASE_URL}/bso/external/bidAckList.sdo?bidId=${encodeURIComponent(id)}`
  }));
  
  // Vendors
  tests.vendors = URL_PATTERNS.vendor.examples.map(id => ({
    id,
    url: buildUrl(id, 'vendor'),
    expected: `${BASE_URL}/bso/external/vendor/vendorProfileOrgInfo.sda?external=true&vendorId=${encodeURIComponent(id)}`
  }));
  
  return tests;
}

// Export functions for use in other modules
module.exports = {
  buildUrl,
  validateUrl,
  validateIdFormat,
  addUrlsToRow,
  generateTestUrls,
  URL_PATTERNS,
  BASE_URL
};

// CLI interface for testing
if (require.main === module) {
  const args = process.argv.slice(2);
  
  if (args.length === 0 || args[0] === '--help') {
    console.log(`
Nevada ePro URL Builder and Validator

Usage:
  node url-builder.js build <type> <id>           Build URL from ID
  node url-builder.js validate <type> <url>       Validate URL format
  node url-builder.js test                        Run validation tests
  node url-builder.js examples                    Show example URLs

Types:
  purchase_order, contract, bid, vendor

Examples:
  node url-builder.js build purchase_order "72DOW-NV18-6"
  node url-builder.js build bid "30DOE-S3449" detail
  node url-builder.js validate vendor "https://nevadaepro.com/bso/external/vendor/vendorProfileOrgInfo.sda?external=true&vendorId=VEN30280"
`);
    process.exit(0);
  }
  
  const command = args[0];
  
  if (command === 'build') {
    const type = args[1];
    const id = args[2];
    const subtype = args[3];
    
    try {
      const url = buildUrl(id, type, subtype);
      console.log(`\nBuilt URL for ${type} "${id}":`);
      console.log(url);
      
      // Also validate the ID format
      const validation = validateIdFormat(id, type);
      if (!validation.valid) {
        console.log('\n⚠️  ID Format Warning:');
        validation.warnings.forEach(w => console.log(`  - ${w}`));
      }
    } catch (error) {
      console.error(`Error: ${error.message}`);
      process.exit(1);
    }
  } else if (command === 'validate') {
    const type = args[1];
    const url = args[2];
    
    const result = validateUrl(url, type);
    if (result.valid) {
      console.log(`\n✅ Valid ${type} URL`);
      console.log(`  Record ID: ${result.recordId}`);
      console.log(`  Type: ${result.recordType}`);
    } else {
      console.log(`\n❌ Invalid URL`);
      result.errors.forEach(e => console.log(`  - ${e}`));
    }
  } else if (command === 'test') {
    console.log('Running URL builder validation tests...\n');
    const tests = generateTestUrls();
    
    let passed = 0;
    let failed = 0;
    
    Object.entries(tests).forEach(([category, items]) => {
      console.log(`\n${category}:`);
      items.forEach(test => {
        const url = test.url || test.detailUrl || test.holderUrl;
        const expected = test.expected || test.expectedDetail || test.expectedHolder;
        
        if (url === expected) {
          console.log(`  ✅ ${test.id}`);
          passed++;
        } else {
          console.log(`  ❌ ${test.id}`);
          console.log(`     Got:      ${url}`);
          console.log(`     Expected: ${expected}`);
          failed++;
        }
      });
    });
    
    console.log(`\n\nTests: ${passed} passed, ${failed} failed`);
  } else if (command === 'examples') {
    const tests = generateTestUrls();
    console.log('Example URLs for each record type:\n');
    
    Object.entries(tests).forEach(([category, items]) => {
      console.log(`\n${category}:`);
      items.forEach(test => {
        console.log(`  ${test.id}:`);
        if (test.url) console.log(`    ${test.url}`);
        if (test.detailUrl) console.log(`    Detail: ${test.detailUrl}`);
        if (test.holderUrl) console.log(`    Holder: ${test.holderUrl}`);
      });
    });
  }
}