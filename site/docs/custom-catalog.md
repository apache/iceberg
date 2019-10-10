<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

# Custom Catalog Implementation

It's possible to read an iceberg table either from an hdfs path or from a hive table. It's also possible to use a custom metastore in place of hive. The steps to do that are as follows.

- [Custom TableOperations](#custom-table-operations-implementation)
- [Custom Catalog](#custom-table-implementation)
- [Custom IcebergSource](#custom-icebergsource)

### Custom table operations implementation
Extend `BaseMetastoreTableOperations` to provide implementation on how to read and write metadata

Example:
```java
class CustomTableOperations extends BaseMetastoreTableOperations {
  private String dbName;
  private String tableName;
  private Configuration conf;
  private FileIO fileIO;

  protected CustomTableOperations(Configuration conf, String dbName, String tableName) {
    this.conf = conf;
    this.dbName = dbName;
    this.tableName = tableName;
  }

  // The doRefresh method should provide implementation on how to get the metadata location
  @Override
  public void doRefresh() {

    // Example custom service which returns the metadata location given a dbName and tableName
    String metadataLocation = CustomService.getMetadataForTable(conf, dbName, tableName);

    // When updating from a metadata file location, call the helper method
    refreshFromMetadataLocation(metadataLocation);

  }

  // The doCommit method should provide implementation on how to update with metadata location atomically
  @Override
  public void doCommit(TableMetadata base, TableMetadata metadata) {
    String oldMetadataLocation = base.location();

    // Write new metadata using helper method
    String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);

    // Example custom service which updates the metadata location for the given db and table atomically
    CustomService.updateMetadataLocation(dbName, tableName, oldMetadataLocation, newMetadataLocation);

  }

  // The io method provides a FileIO which is used to read and write the table metadata files
  @Override
  public FileIO io() {
    if (fileIO == null) {
      fileIO = new HadoopFileIO(conf);
    }
    return fileIO;
  }
}
```

### Custom table implementation
Extend `BaseMetastoreCatalog` to provide default warehouse locations and instantiate `CustomTableOperations`

Example:
```java
public class CustomCatalog extends BaseMetastoreCatalog {

  private Configuration configuration;

  public CustomCatalog(Configuration configuration) {
    this.configuration = configuration;
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    String dbName = tableIdentifier.namespace().level(0);
    String tableName = tableIdentifier.name();
    // instantiate the CustomTableOperations
    return new CustomTableOperations(configuration, dbName, tableName);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {

    // Can choose to use any other configuration name
    String tableLocation = configuration.get("custom.iceberg.warehouse.location");

    // Can be an s3 or hdfs path
    if (tableLocation == null) {
      throw new RuntimeException("custom.iceberg.warehouse.location configuration not set!");
    }

    return String.format(
            "%s/%s.db/%s", tableLocation,
            tableIdentifier.namespace().levels()[0],
            tableIdentifier.name());
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    // Example service to delete table
    CustomService.deleteTable(identifier.namepsace().level(0), identifier.name());
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    Preconditions.checkArgument(from.namespace().level(0).equals(to.namespace().level(0)),
            "Cannot move table between databases");
    // Example service to rename table
    CustomService.renameTable(from.namepsace().level(0), from.name(), to.name());
  }
}
```
### Custom IcebergSource
Extend `IcebergSource` and provide implementation to read from `CustomCatalog`

Example:
```java
public class CustomIcebergSource extends IcebergSource {

  @Override
  protected Table findTable(DataSourceOptions options, Configuration conf) {
    Optional<String> path = options.get("path");
    Preconditions.checkArgument(path.isPresent(), "Cannot open table: path is not set");

    // Read table from CustomCatalog
    CustomCatalog catalog = new CustomCatalog(conf);
    TableIdentifier tableIdentifier = TableIdentifier.parse(path.get());
    return catalog.loadTable(tableIdentifier);
  }
}
```

Register the `CustomIcebergSource` by updating  `META-INF/services/org.apache.spark.sql.sources.DataSourceRegister` with its fully qualified name
