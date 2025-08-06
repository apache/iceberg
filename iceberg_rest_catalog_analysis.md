# Iceberg REST Catalog Analysis - Todo List

## Overview
The `RESTCatalogServer.java` provides a REST API layer over any Iceberg catalog implementation. It uses `RESTCatalogServlet` to handle HTTP requests and `RESTCatalogAdapter` to translate REST calls into catalog operations. The actual catalog operations are delegated to `CatalogHandlers` which call the underlying catalog implementation (e.g., JDBC catalog).

## Architecture
- **RESTCatalogServer**: HTTP server setup with Jetty
- **RESTCatalogServlet**: HTTP request handling and routing
- **RESTCatalogAdapter**: REST API to catalog operations translation
- **RESTServerCatalogAdapter**: Extends adapter to inject storage credentials into responses
- **CatalogHandlers**: Business logic for catalog operations
- **Backend Catalog**: Actual catalog implementation (e.g., JdbcCatalog)

## Database Tables Managed
The REST catalog delegates to the backend catalog, which typically manages:
1. **`iceberg_tables`** - Table and view metadata
2. **`iceberg_namespace_properties`** - Namespace properties

## Credential Management

### When Credentials Are Used
The REST catalog **DOES** need storage credentials because it returns table metadata that contains file locations. Clients need these credentials to access the actual table data files. Credentials are injected into `LoadTableResponse` objects when the `include-credentials` configuration is enabled.

### Storage Credentials Supported
- **AWS S3**: `s3.access-key-id`, `s3.secret-access-key`, `s3.session-token`
- **Google Cloud Storage**: `gcs.oauth2-token`
- **Azure Data Lake Storage**: SAS tokens and connection strings

### Endpoints That Return Credentials
Credentials are injected into responses for endpoints that return `LoadTableResponse`:

1. **CREATE_TABLE** - When creating new tables
2. **LOAD_TABLE** - When loading existing table metadata
3. **UPDATE_TABLE** - When updating table metadata
4. **REGISTER_TABLE** - When registering existing tables
5. **Stage Table Create** - When staging table creation

## Detailed Credential Usage Analysis

### **Where Credentials Are Actually Used**

#### **1. FileIO Creation with Credentials**
When the REST catalog loads a table, it creates a `FileIO` instance with credentials:

```java
// In RESTSessionCatalog.loadTable()
RESTTableOperations ops = new RESTTableOperations(
    tableClient,
    paths.table(finalIdentifier),
    Map::of,
    tableFileIO(context, tableConf, response.credentials()), // Credentials passed here
    tableMetadata,
    endpoints);
```

#### **2. FileIO Configuration**
The `tableFileIO()` method creates a FileIO with storage credentials:

```java
private FileIO tableFileIO(
    SessionContext context, Map<String, String> config, List<Credential> storageCredentials) {
  // ...
  return newFileIO(context, fullConf, storageCredentials);
}
```

#### **3. FileIO Loading with Credentials**
The `newFileIO()` method loads the FileIO implementation with credentials:

```java
private FileIO newFileIO(
    SessionContext context, Map<String, String> properties, List<Credential> storageCredentials) {
  String ioImpl = properties.getOrDefault(CatalogProperties.FILE_IO_IMPL, DEFAULT_FILE_IO_IMPL);
  return CatalogUtil.loadFileIO(
      ioImpl,
      properties,
      conf,
      storageCredentials.stream()
          .map(c -> StorageCredential.create(c.prefix(), c.config()))
          .collect(Collectors.toList()));
}
```

### **What Functions Use Credentials**

#### **1. Table Operations That Read/Write Files**
The `RESTTableOperations` class uses the FileIO (with credentials) for:

- **Reading metadata files**: `TableMetadataParser.read(io(), metadataLocation)`
- **Writing metadata files**: `TableMetadataParser.overwrite(metadata, newMetadataLocation)`
- **Reading data files**: When clients access table data
- **Writing data files**: When clients write to tables

#### **2. BaseMetastoreTableOperations**
The base class handles metadata file operations:

```java
protected String writeNewMetadata(TableMetadata metadata, int newVersion) {
  String newTableMetadataFilePath = newTableMetadataFilePath(metadata, newVersion);
  OutputFile newMetadataLocation = io().newOutputFile(newTableMetadataFilePath);

  // Write the new metadata file
  TableMetadataParser.overwrite(metadata, newMetadataLocation);

  return newMetadataLocation.location();
}

protected void refreshFromMetadataLocation(String newLocation) {
  // Read metadata file
  TableMetadata newMetadata = TableMetadataParser.read(io(), newLocation);
  // ...
}
```

### **Does the Catalog Read or Write Metadata Files?**

#### **YES - The Catalog BOTH Reads AND Writes Metadata Files**

#### **Reading Metadata Files:**
1. **Table Loading**: When `loadTable()` is called, the catalog reads the metadata file from the location stored in the database
2. **Table Refresh**: When `refresh()` is called, the catalog reads the latest metadata file
3. **Metadata Validation**: During commits, the catalog reads the current metadata to validate changes

#### **Writing Metadata Files:**
1. **Table Creation**: When creating a new table, the catalog writes the initial metadata file
2. **Table Updates**: When updating table schema, properties, or adding snapshots, the catalog writes new metadata files
3. **Transaction Commits**: When committing transactions, the catalog writes new metadata files

#### **The Complete Flow:**

1. **Catalog stores metadata location** in database (`iceberg_tables.metadata_location`)
2. **Catalog reads metadata file** from that location when loading tables
3. **Catalog writes new metadata file** when making changes
4. **Catalog updates database** with new metadata location
5. **Clients use credentials** to access data files referenced in metadata

### **Key Functions That Use Credentials:**

1. **`io().newInputFile(path)`** - Reading files (metadata, data, manifests)
2. **`io().newOutputFile(path)`** - Writing files (metadata, data, manifests)
3. **`io().deleteFile(path)`** - Deleting files (cleanup operations)
4. **`TableMetadataParser.read(io(), location)`** - Reading metadata files
5. **`TableMetadataParser.overwrite(metadata, outputFile)`** - Writing metadata files

### **Why Credentials Are Essential:**

- **Metadata files contain file locations** for data files, manifests, and other table components
- **Clients need credentials** to access these referenced files
- **The catalog itself needs credentials** to read/write metadata files during operations
- **Without credentials**, neither the catalog nor clients can access the actual table data

## Todo List - REST Endpoints

### 1. Authentication Endpoints

#### OAuth Token Management
- [ ] **Analyze TOKENS endpoint**: `POST /v1/oauth/tokens`
  - **Handler**: `handleOAuthRequest()`
  - **Records queried**: None (authentication only)
  - **Records updated**: None (authentication only)
  - **Files touched**: None (in-memory operation)
  - **Credentials**: None
  - **Purpose**: OAuth token exchange for authentication

### 2. Configuration Endpoints

#### Catalog Configuration
- [ ] **Analyze CONFIG endpoint**: `GET /v1/config`
  - **Handler**: Returns supported endpoints list
  - **Records queried**: None
  - **Records updated**: None
  - **Files touched**: None (in-memory operation)
  - **Credentials**: None
  - **Purpose**: Returns list of supported REST endpoints

### 3. Namespace Operations

#### List Namespaces
- [ ] **Analyze LIST_NAMESPACES endpoint**: `GET /v1/{prefix}/namespaces`
  - **Handler**: `CatalogHandlers.listNamespaces()`
  - **Backend calls**: `catalog.listNamespaces(parent)`
  - **Records queried**:
    - From `iceberg_tables`: Distinct `table_namespace` values where `catalog_name = ?`
    - From `iceberg_namespace_properties`: Distinct `namespace` values where `catalog_name = ?`
  - **Records updated**: None
  - **Files touched**: `iceberg_tables` table, `iceberg_namespace_properties` table
  - **Credentials**: None
  - **Purpose**: Lists all namespaces in the catalog

#### Create Namespace
- [ ] **Analyze CREATE_NAMESPACE endpoint**: `POST /v1/{prefix}/namespaces`
  - **Handler**: `CatalogHandlers.createNamespace()`
  - **Backend calls**: `catalog.createNamespace(namespace, properties)`
  - **Records queried**: None
  - **Records updated**:
    - Inserts records in `iceberg_namespace_properties` for each property
  - **Files touched**: `iceberg_namespace_properties` table
  - **Credentials**: None
  - **Purpose**: Creates a new namespace with properties

#### Namespace Existence Check
- [ ] **Analyze NAMESPACE_EXISTS endpoint**: `HEAD /v1/{prefix}/namespaces/{namespace}`
  - **Handler**: `CatalogHandlers.namespaceExists()`
  - **Backend calls**: `catalog.namespaceExists(namespace)`
  - **Records queried**:
    - From `iceberg_tables`: Records where `catalog_name = ?` and `table_namespace = ?` or `table_namespace LIKE ?`
    - From `iceberg_namespace_properties`: Records where `catalog_name = ?` and `namespace = ?` or `namespace LIKE ?`
  - **Records updated**: None
  - **Files touched**: `iceberg_tables` table, `iceberg_namespace_properties` table
  - **Credentials**: None
  - **Purpose**: Checks if namespace exists

#### Load Namespace
- [ ] **Analyze LOAD_NAMESPACE endpoint**: `GET /v1/{prefix}/namespaces/{namespace}`
  - **Handler**: `CatalogHandlers.loadNamespace()`
  - **Backend calls**: `catalog.loadNamespaceMetadata(namespace)`
  - **Records queried**: All records from `iceberg_namespace_properties` where `catalog_name = ?` and `namespace = ?`
  - **Records updated**: None
  - **Files touched**: `iceberg_namespace_properties` table
  - **Credentials**: None
  - **Purpose**: Loads namespace metadata and properties

#### Drop Namespace
- [ ] **Analyze DROP_NAMESPACE endpoint**: `DELETE /v1/{prefix}/namespaces/{namespace}`
  - **Handler**: `CatalogHandlers.dropNamespace()`
  - **Backend calls**: `catalog.dropNamespace(namespace)`
  - **Records queried**:
    - Checks for tables/views in namespace from `iceberg_tables`
    - Checks for namespace properties from `iceberg_namespace_properties`
  - **Records updated**:
    - Deletes all records from `iceberg_namespace_properties` where `catalog_name = ?` and `namespace = ?`
  - **Files touched**: `iceberg_namespace_properties` table
  - **Credentials**: None
  - **Purpose**: Deletes namespace and its properties

#### Update Namespace Properties
- [ ] **Analyze UPDATE_NAMESPACE endpoint**: `POST /v1/{prefix}/namespaces/{namespace}/properties`
  - **Handler**: `CatalogHandlers.updateNamespaceProperties()`
  - **Backend calls**: `catalog.setProperties(namespace, updates)` and `catalog.removeProperties(namespace, removals)`
  - **Records queried**: All records from `iceberg_namespace_properties` where `catalog_name = ?` and `namespace = ?`
  - **Records updated**:
    - Updates `property_value` for existing properties in `iceberg_namespace_properties`
    - Inserts new property records in `iceberg_namespace_properties`
    - Deletes property records from `iceberg_namespace_properties` for removals
  - **Files touched**: `iceberg_namespace_properties` table
  - **Credentials**: None
  - **Purpose**: Updates namespace properties (add/update/remove)

### 4. Table Operations

#### List Tables
- [ ] **Analyze LIST_TABLES endpoint**: `GET /v1/{prefix}/namespaces/{namespace}/tables`
  - **Handler**: `CatalogHandlers.listTables()`
  - **Backend calls**: `catalog.listTables(namespace)`
  - **Records queried**: All records from `iceberg_tables` where `catalog_name = ?` and `table_namespace = ?`
  - **Records updated**: None
  - **Files touched**: `iceberg_tables` table
  - **Credentials**: None
  - **Purpose**: Lists all tables in a namespace

#### Create Table
- [ ] **Analyze CREATE_TABLE endpoint**: `POST /v1/{prefix}/namespaces/{namespace}/tables`
  - **Handler**: `CatalogHandlers.createTable()`
  - **Backend calls**: `catalog.buildTable().create()`
  - **Records queried**: None
  - **Records updated**:
    - Inserts new record in `iceberg_tables` with table metadata
  - **Files touched**: `iceberg_tables` table
  - **Credentials**: **INJECTED** into `LoadTableResponse.config()` if `include-credentials=true`
  - **Purpose**: Creates a new table in the catalog

#### Stage Table Create
- [ ] **Analyze CREATE_TABLE (staged) endpoint**: `POST /v1/{prefix}/namespaces/{namespace}/tables` with stageCreate=true
  - **Handler**: `CatalogHandlers.stageTableCreate()`
  - **Backend calls**: `catalog.buildTable().createTransaction().table().location()`
  - **Records queried**: Checks if table exists in `iceberg_tables`
  - **Records updated**: None (staging only)
  - **Files touched**: `iceberg_tables` table (read-only)
  - **Credentials**: **INJECTED** into `LoadTableResponse.config()` if `include-credentials=true`
  - **Purpose**: Stages table creation without committing to catalog

#### Table Existence Check
- [ ] **Analyze TABLE_EXISTS endpoint**: `HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}`
  - **Handler**: `CatalogHandlers.tableExists()`
  - **Backend calls**: `catalog.tableExists(ident)`
  - **Records queried**: Single record from `iceberg_tables` where `catalog_name = ?`, `table_namespace = ?`, and `table_name = ?`
  - **Records updated**: None
  - **Files touched**: `iceberg_tables` table
  - **Credentials**: None
  - **Purpose**: Checks if table exists

#### Load Table
- [ ] **Analyze LOAD_TABLE endpoint**: `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}`
  - **Handler**: `CatalogHandlers.loadTable()`
  - **Backend calls**: `catalog.loadTable(ident)`
  - **Records queried**: Single record from `iceberg_tables` where `catalog_name = ?`, `table_namespace = ?`, and `table_name = ?`
  - **Records updated**: None
  - **Files touched**: `iceberg_tables` table
  - **Credentials**: **INJECTED** into `LoadTableResponse.config()` if `include-credentials=true`
  - **Purpose**: Loads table metadata from catalog

#### Register Table
- [ ] **Analyze REGISTER_TABLE endpoint**: `POST /v1/{prefix}/namespaces/{namespace}/register`
  - **Handler**: `CatalogHandlers.registerTable()`
  - **Backend calls**: `catalog.registerTable(identifier, metadataLocation)`
  - **Records queried**: None
  - **Records updated**:
    - Inserts new record in `iceberg_tables` with provided metadata location
  - **Files touched**: `iceberg_tables` table
  - **Credentials**: **INJECTED** into `LoadTableResponse.config()` if `include-credentials=true`
  - **Purpose**: Registers existing table with catalog using provided metadata location

#### Update Table
- [ ] **Analyze UPDATE_TABLE endpoint**: `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}`
  - **Handler**: `CatalogHandlers.updateTable()`
  - **Backend calls**: `catalog.loadTable()` and `TableOperations.commit()`
  - **Records queried**:
    - Single record from `iceberg_tables` for current metadata
  - **Records updated**:
    - Updates `metadata_location` and `previous_metadata_location` in `iceberg_tables`
  - **Files touched**: `iceberg_tables` table
  - **Credentials**: **INJECTED** into `LoadTableResponse.config()` if `include-credentials=true`
  - **Purpose**: Updates table metadata (schema, properties, etc.)

#### Drop Table
- [ ] **Analyze DROP_TABLE endpoint**: `DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}`
  - **Handler**: `CatalogHandlers.dropTable()`
  - **Backend calls**: `catalog.dropTable(ident, false)`
  - **Records queried**: Single record from `iceberg_tables` where `catalog_name = ?`, `table_namespace = ?`, and `table_name = ?`
  - **Records updated**:
    - Deletes record from `iceberg_tables`
  - **Files touched**: `iceberg_tables` table
  - **Credentials**: None
  - **Purpose**: Removes table from catalog

#### Purge Table
- [ ] **Analyze DROP_TABLE (purge) endpoint**: `DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}?purgeRequested=true`
  - **Handler**: `CatalogHandlers.purgeTable()`
  - **Backend calls**: `catalog.dropTable(ident, true)`
  - **Records queried**: Single record from `iceberg_tables` where `catalog_name = ?`, `table_namespace = ?`, and `table_name = ?`
  - **Records updated**:
    - Deletes record from `iceberg_tables`
  - **Files touched**: `iceberg_tables` table
  - **Credentials**: None
  - **Purpose**: Removes table from catalog and deletes underlying files

#### Rename Table
- [ ] **Analyze RENAME_TABLE endpoint**: `POST /v1/{prefix}/tables/rename`
  - **Handler**: `CatalogHandlers.renameTable()`
  - **Backend calls**: `catalog.renameTable(source, destination)`
  - **Records queried**:
    - Finds source table in `iceberg_tables`
  - **Records updated**:
    - Updates `table_namespace` and `table_name` in `iceberg_tables` for source record
  - **Files touched**: `iceberg_tables` table
  - **Credentials**: None
  - **Purpose**: Renames table in catalog

#### Report Metrics
- [ ] **Analyze REPORT_METRICS endpoint**: `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics`
  - **Handler**: Validates request only
  - **Records queried**: None
  - **Records updated**: None
  - **Files touched**: None
  - **Credentials**: None
  - **Purpose**: Reports table metrics (no-op in current implementation)

### 5. Transaction Operations

#### Commit Transaction
- [ ] **Analyze COMMIT_TRANSACTION endpoint**: `POST /v1/{prefix}/transactions/commit`
  - **Handler**: `commitTransaction()`
  - **Backend calls**: `catalog.loadTable()` and `TableOperations.commit()` for each table
  - **Records queried**:
    - Multiple records from `iceberg_tables` for each table in transaction
  - **Records updated**:
    - Updates `metadata_location` and `previous_metadata_location` for each table in `iceberg_tables`
  - **Files touched**: `iceberg_tables` table
  - **Credentials**: None (transaction response doesn't include table metadata)
  - **Purpose**: Commits multiple table updates atomically

### 6. View Operations

#### List Views
- [ ] **Analyze LIST_VIEWS endpoint**: `GET /v1/{prefix}/namespaces/{namespace}/views`
  - **Handler**: `CatalogHandlers.listViews()`
  - **Backend calls**: `catalog.listViews(namespace)`
  - **Records queried**: All records from `iceberg_tables` where `catalog_name = ?`, `table_namespace = ?`, and `iceberg_type = 'VIEW'`
  - **Records updated**: None
  - **Files touched**: `iceberg_tables` table
  - **Credentials**: None
  - **Purpose**: Lists all views in a namespace

#### Create View
- [ ] **Analyze CREATE_VIEW endpoint**: `POST /v1/{prefix}/namespaces/{namespace}/views`
  - **Handler**: `CatalogHandlers.createView()`
  - **Backend calls**: `catalog.buildView().create()`
  - **Records queried**: None
  - **Records updated**:
    - Inserts new record in `iceberg_tables` with `iceberg_type = 'VIEW'`
  - **Files touched**: `iceberg_tables` table
  - **Credentials**: **INJECTED** into `LoadViewResponse.config()` if `include-credentials=true`
  - **Purpose**: Creates a new view in the catalog

#### View Existence Check
- [ ] **Analyze VIEW_EXISTS endpoint**: `HEAD /v1/{prefix}/namespaces/{namespace}/views/{view}`
  - **Handler**: `CatalogHandlers.viewExists()`
  - **Backend calls**: `catalog.viewExists(ident)`
  - **Records queried**: Single record from `iceberg_tables` where `catalog_name = ?`, `table_namespace = ?`, `table_name = ?`, and `iceberg_type = 'VIEW'`
  - **Records updated**: None
  - **Files touched**: `iceberg_tables` table
  - **Credentials**: None
  - **Purpose**: Checks if view exists

#### Load View
- [ ] **Analyze LOAD_VIEW endpoint**: `GET /v1/{prefix}/namespaces/{namespace}/views/{view}`
  - **Handler**: `CatalogHandlers.loadView()`
  - **Backend calls**: `catalog.loadView(ident)`
  - **Records queried**: Single record from `iceberg_tables` where `catalog_name = ?`, `table_namespace = ?`, `table_name = ?`, and `iceberg_type = 'VIEW'`
  - **Records updated**: None
  - **Files touched**: `iceberg_tables` table
  - **Credentials**: **INJECTED** into `LoadViewResponse.config()` if `include-credentials=true`
  - **Purpose**: Loads view metadata from catalog

#### Update View
- [ ] **Analyze UPDATE_VIEW endpoint**: `POST /v1/{prefix}/namespaces/{namespace}/views/{view}`
  - **Handler**: `CatalogHandlers.updateView()`
  - **Backend calls**: `catalog.loadView()` and `ViewOperations.commit()`
  - **Records queried**:
    - Single record from `iceberg_tables` for current view metadata
  - **Records updated**:
    - Updates `metadata_location` and `previous_metadata_location` in `iceberg_tables`
  - **Files touched**: `iceberg_tables` table
  - **Credentials**: **INJECTED** into `LoadViewResponse.config()` if `include-credentials=true`
  - **Purpose**: Updates view metadata

#### Rename View
- [ ] **Analyze RENAME_VIEW endpoint**: `POST /v1/{prefix}/views/rename`
  - **Handler**: `CatalogHandlers.renameView()`
  - **Backend calls**: `catalog.renameView(source, destination)`
  - **Records queried**:
    - Finds source view in `iceberg_tables`
  - **Records updated**:
    - Updates `table_namespace` and `table_name` in `iceberg_tables` for source record
  - **Files touched**: `iceberg_tables` table
  - **Credentials**: None
  - **Purpose**: Renames view in catalog

#### Drop View
- [ ] **Analyze DROP_VIEW endpoint**: `DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}`
  - **Handler**: `CatalogHandlers.dropView()`
  - **Backend calls**: `catalog.dropView(ident)`
  - **Records queried**: Single record from `iceberg_tables` where `catalog_name = ?`, `table_namespace = ?`, `table_name = ?`, and `iceberg_type = 'VIEW'`
  - **Records updated**:
    - Deletes record from `iceberg_tables`
  - **Files touched**: `iceberg_tables` table
  - **Credentials**: None
  - **Purpose**: Removes view from catalog

## Summary of Catalog Files Touched

The REST catalog server delegates all operations to the backend catalog implementation. When using JDBC catalog as backend:

### **Database Tables:**
1. **`iceberg_tables`** - All table/view operations
2. **`iceberg_namespace_properties`** - All namespace operations

### **Key Insights:**

#### **Credential Management:**
- **Storage Credentials**: The REST catalog DOES need storage credentials because it returns table metadata containing file locations
- **Credential Injection**: Credentials are injected into `LoadTableResponse` and `LoadViewResponse` when `include-credentials=true`
- **Client Access**: Clients need these credentials to access the actual table data files referenced in metadata
- **Supported Clouds**: AWS S3, Google Cloud Storage, Azure Data Lake Storage

#### **File Access Pattern:**
- **Metadata Management**: The catalog manages metadata pointers in database tables
- **File References**: Table metadata contains file locations that clients need to access
- **Credential Distribution**: The REST server distributes credentials to clients so they can access files
- **No Direct File Access**: The REST server itself doesn't directly read/write table data files

#### **Operation Patterns:**
- **Read Operations**: Query catalog tables to retrieve metadata
- **Write Operations**: Insert/update/delete records in catalog tables
- **Validation**: Check existence before operations
- **Atomicity**: Use database transactions for multi-table operations
- **Credential Injection**: Add storage credentials to responses for file access endpoints

The REST catalog provides a **standardized HTTP API** over any Iceberg catalog implementation, making it easy to integrate with different storage backends while maintaining consistent metadata management and credential distribution.