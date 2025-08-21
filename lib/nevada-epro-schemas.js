#!/usr/bin/env node

/**
 * Nevada ePro Schema Definitions
 * 
 * Maps CSV columns to optimized data types for each dataset
 * This is the reference for converting raw CSV to typed Parquet
 */

const SCHEMAS = {
  bids: {
    columns: {
      'Bid Solicitation #': {
        output_name: 'bid_id',
        type: 'VARCHAR',
        nullable: false,
        primary_key: true
      },
      'Organization Name': {
        output_name: 'organization',
        type: 'VARCHAR',
        nullable: false
      },
      'Contract #': {
        output_name: 'contract_id',
        type: 'VARCHAR',
        nullable: true,
        transform: "NULLIF(\"Contract #\", '')"
      },
      'Buyer': {
        output_name: 'buyer',
        type: 'VARCHAR',
        nullable: false
      },
      'Description': {
        output_name: 'description',
        type: 'VARCHAR',
        nullable: false
      },
      'Bid Opening Date': {
        output_name: 'bid_opening_date',
        type: 'TIMESTAMP',
        nullable: true,
        transform: "strptime(\"Bid Opening Date\", '%m/%d/%Y %H:%M:%S')"
      },
      'Bid Holder List': {
        output_name: 'has_holder_list',
        type: 'BOOLEAN',
        nullable: false,
        transform: "(\"Bid Holder List\" = '/bso/external/bidAckList.sdo')"
      },
      'Awarded Vendor(s)': {
        output_name: 'awarded_vendors',
        type: 'VARCHAR',
        nullable: true,
        transform: "NULLIF(\"Awarded Vendor(s)\", '')"
      },
      'Status': {
        output_name: 'status',
        type: 'VARCHAR',
        nullable: false
        // Could be: 'Sent', 'Awarded', 'Cancelled', etc.
      },
      'Alternate Id': {
        output_name: 'alternate_id',
        type: 'VARCHAR',
        nullable: true,
        transform: "NULLIF(\"Alternate Id\", '')"
      }
    },
    partition_by: 'bid_opening_date'
  },

  purchase_orders: {
    columns: {
      'PO #': {
        output_name: 'po_id',
        type: 'VARCHAR',
        nullable: false,
        primary_key: true
      },
      'Revision #': {
        output_name: 'revision_number',
        type: 'INTEGER',
        nullable: true,
        transform: "TRY_CAST(\"Revision #\" AS INTEGER)"
      },
      'Date Sent': {
        output_name: 'date_sent',
        type: 'DATE',
        nullable: true,
        transform: "strptime(\"Date Sent\", '%m/%d/%Y')::DATE"
      },
      'Days to Delivery': {
        output_name: 'days_to_delivery',
        type: 'INTEGER',
        nullable: true,
        transform: "TRY_CAST(\"Days to Delivery\" AS INTEGER)"
      },
      'Status': {
        output_name: 'status',
        type: 'VARCHAR',
        nullable: false
      },
      'Total': {
        output_name: 'total_amount',
        type: 'DECIMAL(18,2)',
        nullable: true,
        transform: "TRY_CAST(REPLACE(REPLACE(\"Total\", '$', ''), ',', '') AS DECIMAL(18,2))"
      },
      'Bid #': {
        output_name: 'bid_id',
        type: 'VARCHAR',
        nullable: true,
        transform: "NULLIF(\"Bid #\", '')"
      },
      'Contract #': {
        output_name: 'contract_id',
        type: 'VARCHAR',
        nullable: true,
        transform: "NULLIF(\"Contract #\", '')"
      },
      'Buyer Name': {
        output_name: 'buyer_name',
        type: 'VARCHAR',
        nullable: true
      },
      'Supplier': {
        output_name: 'supplier',
        type: 'VARCHAR',
        nullable: false
      },
      'Description': {
        output_name: 'description',
        type: 'VARCHAR',
        nullable: true
      }
    },
    partition_by: 'date_sent'
  },

  contracts: {
    columns: {
      'Contract #': {
        output_name: 'contract_id',
        type: 'VARCHAR',
        nullable: false,
        primary_key: true
      },
      'Description': {
        output_name: 'description',
        type: 'VARCHAR',
        nullable: false
      },
      'Department': {
        output_name: 'department',
        type: 'VARCHAR',
        nullable: false
      },
      'Buyer': {
        output_name: 'buyer',
        type: 'VARCHAR',
        nullable: true
      },
      'Supplier': {
        output_name: 'supplier',
        type: 'VARCHAR',
        nullable: false
      },
      'Start Date': {
        output_name: 'start_date',
        type: 'DATE',
        nullable: true,
        transform: "strptime(\"Start Date\", '%m/%d/%Y')::DATE"
      },
      'End Date': {
        output_name: 'end_date',
        type: 'DATE',
        nullable: true,
        transform: "strptime(\"End Date\", '%m/%d/%Y')::DATE"
      },
      'Status': {
        output_name: 'status',
        type: 'VARCHAR',
        nullable: false
      },
      'Contract Amount': {
        output_name: 'contract_amount',
        type: 'DECIMAL(18,2)',
        nullable: true,
        transform: "TRY_CAST(REPLACE(REPLACE(\"Contract Amount\", '$', ''), ',', '') AS DECIMAL(18,2))"
      },
      'Bid #': {
        output_name: 'bid_id',
        type: 'VARCHAR',
        nullable: true,
        transform: "NULLIF(\"Bid #\", '')"
      }
    },
    partition_by: 'start_date'
  },

  vendors: {
    columns: {
      'Vendor ID': {
        output_name: 'vendor_id',
        type: 'VARCHAR',
        nullable: false,
        primary_key: true
      },
      'Vendor Name': {
        output_name: 'vendor_name',
        type: 'VARCHAR',
        nullable: false
      },
      'Status': {
        output_name: 'status',
        type: 'VARCHAR',
        nullable: false
        // Usually: 'Active', 'Inactive', 'Suspended'
      },
      'City': {
        output_name: 'city',
        type: 'VARCHAR',
        nullable: true
      },
      'State': {
        output_name: 'state',
        type: 'VARCHAR(2)',
        nullable: true
      },
      'Zip': {
        output_name: 'zip',
        type: 'VARCHAR(10)',
        nullable: true
      },
      'Country': {
        output_name: 'country',
        type: 'VARCHAR',
        nullable: true
      },
      'Business Type': {
        output_name: 'business_type',
        type: 'VARCHAR',
        nullable: true
      },
      'Commodity Codes': {
        output_name: 'commodity_codes',
        type: 'VARCHAR',
        nullable: true
        // Usually comma-separated list
      }
    },
    partition_by: null  // No date field to partition by
  }
};

/**
 * Generate SELECT statement for a dataset
 */
function generateSelectSQL(dataset) {
  const schema = SCHEMAS[dataset];
  if (!schema) {
    throw new Error(`Unknown dataset: ${dataset}`);
  }
  
  const selectClauses = [];
  
  for (const [csvColumn, config] of Object.entries(schema.columns)) {
    if (config.transform) {
      selectClauses.push(`${config.transform}::${config.type} AS ${config.output_name}`);
    } else {
      selectClauses.push(`"${csvColumn}"::${config.type} AS ${config.output_name}`);
    }
  }
  
  return `SELECT\n  ${selectClauses.join(',\n  ')}`;
}

/**
 * Get partition column for a dataset
 */
function getPartitionColumn(dataset) {
  const schema = SCHEMAS[dataset];
  return schema ? schema.partition_by : null;
}

/**
 * Get primary key for a dataset
 */
function getPrimaryKey(dataset) {
  const schema = SCHEMAS[dataset];
  if (!schema) return null;
  
  for (const [csvColumn, config] of Object.entries(schema.columns)) {
    if (config.primary_key) {
      return config.output_name;
    }
  }
  return null;
}

module.exports = {
  SCHEMAS,
  generateSelectSQL,
  getPartitionColumn,
  getPrimaryKey
};