# Iceberg JDBC Catalog Analysis - Todo List

## Overview
The `JdbcUtil.java` file contains utility methods for the Iceberg JDBC catalog that manage metadata in two main database tables:
1. `iceberg_tables` - Stores table and view metadata
2. `iceberg_namespace_properties` - Stores namespace properties

## Database Schema

### Main Catalog Table: `iceberg_tables`
- **Columns**: `catalog_name`, `table_namespace`, `table_name`, `metadata_location`, `previous_metadata_location`, `iceberg_type` (V1 only)
- **Primary Key**: `(catalog_name, table_namespace, table_name)`
- **Record Types**: `TABLE`, `VIEW`, or `NULL` (for V0 compatibility)

### Namespace Properties Table: `iceberg_namespace_properties`
- **Columns**: `catalog_name`, `namespace`, `property_key`, `property_value`
- **Primary Key**: `(catalog_name, namespace, property_key)`

## Todo List - Catalog Actions

### 1. Schema Version Management
- [ ] **Analyze V0_CREATE_CATALOG_SQL**: Creates initial `iceberg_tables` table without `iceberg_type` column
  - **Files touched**: Creates `iceberg_tables` table in database
  - **Records affected**: None (table creation)

- [ ] **Analyze V1_UPDATE_CATALOG_SQL**: Adds `iceberg_type` column to existing `iceberg_tables` table
  - **Files touched**: Alters `iceberg_tables` table structure
  - **Records affected**: None (schema migration)

### 2. Table Operations

#### Table Creation
- [ ] **Analyze doCommitCreateTable()**: Creates new table entry in catalog
  - **SQL**: V1_DO_COMMIT_CREATE_SQL or V0_DO_COMMIT_CREATE_SQL
  - **Records queried**: None
  - **Records updated**: Inserts new record in `iceberg_tables`
  - **Files touched**: `iceberg_tables` table

#### Table Updates
- [ ] **Analyze updateTable()**: Updates existing table metadata location
  - **SQL**: V1_DO_COMMIT_TABLE_SQL or V0_DO_COMMIT_SQL
  - **Records queried**: Finds table by `(catalog_name, table_namespace, table_name, old_metadata_location)`
  - **Records updated**: Updates `metadata_location` and `previous_metadata_location` for matching record
  - **Files touched**: `iceberg_tables` table

#### Table Loading
- [ ] **Analyze loadTable()**: Retrieves table metadata from catalog
  - **SQL**: V1_GET_TABLE_SQL or V0_GET_TABLE_SQL
  - **Records queried**: Single record from `iceberg_tables` by `(catalog_name, table_namespace, table_name)`
  - **Records updated**: None
  - **Files touched**: `iceberg_tables` table

#### Table Existence Check
- [ ] **Analyze tableExists()**: Checks if table exists in catalog
  - **SQL**: V1_GET_TABLE_SQL or V0_GET_TABLE_SQL
  - **Records queried**: Single record from `iceberg_tables` by `(catalog_name, table_namespace, table_name)`
  - **Records updated**: None
  - **Files touched**: `iceberg_tables` table

#### Table Listing
- [ ] **Analyze V1_LIST_TABLE_SQL/V0_LIST_TABLE_SQL**: Lists all tables in namespace
  - **Records queried**: All records from `iceberg_tables` where `catalog_name = ?` and `table_namespace = ?`
  - **Records updated**: None
  - **Files touched**: `iceberg_tables` table

#### Table Renaming
- [ ] **Analyze V1_RENAME_TABLE_SQL/V0_RENAME_TABLE_SQL**: Renames table in catalog
  - **Records queried**: Finds table by `(catalog_name, old_table_namespace, old_table_name)`
  - **Records updated**: Updates `table_namespace` and `table_name` for matching record
  - **Files touched**: `iceberg_tables` table

#### Table Deletion
- [ ] **Analyze V1_DROP_TABLE_SQL/V0_DROP_TABLE_SQL**: Removes table from catalog
  - **Records queried**: Finds table by `(catalog_name, table_namespace, table_name)`
  - **Records updated**: Deletes matching record from `iceberg_tables`
  - **Files touched**: `iceberg_tables` table

### 3. View Operations

#### View Creation
- [ ] **Analyze doCommitCreateView()**: Creates new view entry in catalog
  - **SQL**: V1_DO_COMMIT_CREATE_SQL (with `iceberg_type = 'VIEW'`)
  - **Records queried**: None
  - **Records updated**: Inserts new record in `iceberg_tables`
  - **Files touched**: `iceberg_tables` table

#### View Updates
- [ ] **Analyze updateView()**: Updates existing view metadata location
  - **SQL**: V1_DO_COMMIT_VIEW_SQL
  - **Records queried**: Finds view by `(catalog_name, table_namespace, table_name, old_metadata_location, iceberg_type = 'VIEW')`
  - **Records updated**: Updates `metadata_location` and `previous_metadata_location` for matching record
  - **Files touched**: `iceberg_tables` table

#### View Loading
- [ ] **Analyze loadView()**: Retrieves view metadata from catalog
  - **SQL**: GET_VIEW_SQL
  - **Records queried**: Single record from `iceberg_tables` by `(catalog_name, table_namespace, table_name, iceberg_type = 'VIEW')`
  - **Records updated**: None
  - **Files touched**: `iceberg_tables` table

#### View Existence Check
- [ ] **Analyze viewExists()**: Checks if view exists in catalog
  - **SQL**: GET_VIEW_SQL
  - **Records queried**: Single record from `iceberg_tables` by `(catalog_name, table_namespace, table_name, iceberg_type = 'VIEW')`
  - **Records updated**: None
  - **Files touched**: `iceberg_tables` table

#### View Listing
- [ ] **Analyze LIST_VIEW_SQL**: Lists all views in namespace
  - **Records queried**: All records from `iceberg_tables` where `catalog_name = ?`, `table_namespace = ?`, and `iceberg_type = 'VIEW'`
  - **Records updated**: None
  - **Files touched**: `iceberg_tables` table

#### View Renaming
- [ ] **Analyze RENAME_VIEW_SQL**: Renames view in catalog
  - **Records queried**: Finds view by `(catalog_name, old_table_namespace, old_table_name, iceberg_type = 'VIEW')`
  - **Records updated**: Updates `table_namespace` and `table_name` for matching record
  - **Files touched**: `iceberg_tables` table

#### View Deletion
- [ ] **Analyze DROP_VIEW_SQL**: Removes view from catalog
  - **Records queried**: Finds view by `(catalog_name, table_namespace, table_name, iceberg_type = 'VIEW')`
  - **Records updated**: Deletes matching record from `iceberg_tables`
  - **Files touched**: `iceberg_tables` table

### 4. Namespace Operations

#### Namespace Existence Check
- [ ] **Analyze namespaceExists()**: Checks if namespace exists by looking in both tables
  - **SQL**: GET_NAMESPACE_SQL and GET_NAMESPACE_PROPERTIES_SQL
  - **Records queried**:
    - Records from `iceberg_tables` where `catalog_name = ?` and `table_namespace = ?` or `table_namespace LIKE ?`
    - Records from `iceberg_namespace_properties` where `catalog_name = ?` and `namespace = ?` or `namespace LIKE ?`
  - **Records updated**: None
  - **Files touched**: `iceberg_tables` table, `iceberg_namespace_properties` table

#### Namespace Listing
- [ ] **Analyze LIST_NAMESPACES_SQL**: Lists namespaces with pattern matching
  - **Records queried**: Distinct `table_namespace` values from `iceberg_tables` where `catalog_name = ?` and `table_namespace LIKE ?`
  - **Records updated**: None
  - **Files touched**: `iceberg_tables` table

- [ ] **Analyze LIST_ALL_NAMESPACES_SQL**: Lists all namespaces
  - **Records queried**: Distinct `table_namespace` values from `iceberg_tables` where `catalog_name = ?`
  - **Records updated**: None
  - **Files touched**: `iceberg_tables` table

### 5. Namespace Properties Operations

#### Namespace Properties Table Creation
- [ ] **Analyze CREATE_NAMESPACE_PROPERTIES_TABLE_SQL**: Creates namespace properties table
  - **Files touched**: Creates `iceberg_namespace_properties` table in database
  - **Records affected**: None (table creation)

#### Namespace Properties Retrieval
- [ ] **Analyze GET_ALL_NAMESPACE_PROPERTIES_SQL**: Gets all properties for a namespace
  - **Records queried**: All records from `iceberg_namespace_properties` where `catalog_name = ?` and `namespace = ?`
  - **Records updated**: None
  - **Files touched**: `iceberg_namespace_properties` table

#### Namespace Properties Insertion
- [ ] **Analyze insertPropertiesStatement()**: Generates SQL for inserting multiple properties
  - **SQL**: INSERT_NAMESPACE_PROPERTIES_SQL with multiple value tuples
  - **Records queried**: None
  - **Records updated**: Inserts multiple records in `iceberg_namespace_properties`
  - **Files touched**: `iceberg_namespace_properties` table

#### Namespace Properties Updates
- [ ] **Analyze updatePropertiesStatement()**: Generates SQL for updating multiple properties
  - **Records queried**: Finds properties by `(catalog_name, namespace, property_key)`
  - **Records updated**: Updates `property_value` for matching records in `iceberg_namespace_properties`
  - **Files touched**: `iceberg_namespace_properties` table

#### Namespace Properties Deletion
- [ ] **Analyze deletePropertiesStatement()**: Generates SQL for deleting specific properties
  - **Records queried**: Finds properties by `(catalog_name, namespace, property_key IN (...))`
  - **Records updated**: Deletes matching records from `iceberg_namespace_properties`
  - **Files touched**: `iceberg_namespace_properties` table

- [ ] **Analyze DELETE_ALL_NAMESPACE_PROPERTIES_SQL**: Deletes all properties for a namespace
  - **Records queried**: Finds all properties by `(catalog_name, namespace)`
  - **Records updated**: Deletes all matching records from `iceberg_namespace_properties`
  - **Files touched**: `iceberg_namespace_properties` table

#### Namespace Properties Listing
- [ ] **Analyze LIST_PROPERTY_NAMESPACES_SQL**: Lists namespaces with properties using pattern matching
  - **Records queried**: Distinct `namespace` values from `iceberg_namespace_properties` where `catalog_name = ?` and `namespace LIKE ?`
  - **Records updated**: None
  - **Files touched**: `iceberg_namespace_properties` table

- [ ] **Analyze LIST_ALL_PROPERTY_NAMESPACES_SQL**: Lists all namespaces with properties
  - **Records queried**: Distinct `namespace` values from `iceberg_namespace_properties` where `catalog_name = ?`
  - **Records updated**: None
  - **Files touched**: `iceberg_namespace_properties` table

### 6. Utility Operations

#### String Conversion Utilities
- [ ] **Analyze stringToNamespace()**: Converts dot-separated string to Namespace object
  - **Files touched**: None (in-memory operation)
  - **Records affected**: None

- [ ] **Analyze namespaceToString()**: Converts Namespace object to dot-separated string
  - **Files touched**: None (in-memory operation)
  - **Records affected**: None

- [ ] **Analyze stringToTableIdentifier()**: Creates TableIdentifier from namespace and table name strings
  - **Files touched**: None (in-memory operation)
  - **Records affected**: None

#### Property Filtering
- [ ] **Analyze filterAndRemovePrefix()**: Filters properties by prefix and removes prefix from keys
  - **Files touched**: None (in-memory operation)
  - **Records affected**: None

#### Generic Existence Check
- [ ] **Analyze exists()**: Generic method to check if any records match given SQL and parameters
  - **Records queried**: Depends on provided SQL query
  - **Records updated**: None
  - **Files touched**: Depends on provided SQL query

## Summary of Catalog Files Touched

The Iceberg JDBC catalog primarily touches two database tables:

1. **`iceberg_tables`** - Main catalog table storing:
   - Table and view metadata locations
   - Table/view identifiers (catalog, namespace, name)
   - Record types (TABLE/VIEW)
   - Previous metadata locations for versioning

2. **`iceberg_namespace_properties`** - Properties table storing:
   - Namespace-level properties
   - Property key-value pairs

The catalog does NOT directly touch Iceberg metadata files (like .metadata.json files) - those are managed by the Iceberg engine. The catalog only stores the file paths to those metadata files.