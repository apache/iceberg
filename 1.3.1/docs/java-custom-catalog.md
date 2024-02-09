---
title: "Java Custom Catalog"
url: custom-catalog
aliases:
    - "java/custom-catalog"
menu:
    main:
        parent: "API"
        identifier: java_custom_catalog
        weight: 300
---
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

# Custom Catalog

It's possible to read an iceberg table either from an hdfs path or from a hive table. It's also possible to use a custom metastore in place of hive. The steps to do that are as follows.

- [Custom TableOperations](#custom-table-operations-implementation)
- [Custom Catalog](#custom-catalog-implementation)
- [Custom FileIO](#custom-file-io-implementation)
- [Custom LocationProvider](#custom-location-provider-implementation)
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

A `TableOperations` instance is usually obtained by calling `Catalog.newTableOps(TableIdentifier)`.
See the next section about implementing and loading a custom catalog.

### Custom catalog implementation
Extend `BaseMetastoreCatalog` to provide default warehouse locations and instantiate `CustomTableOperations`

Example:
```java
public class CustomCatalog extends BaseMetastoreCatalog {

  private Configuration configuration;

  // must have a no-arg constructor to be dynamically loaded
  // initialize(String name, Map<String, String> properties) will be called to complete initialization
  public CustomCatalog() {
  }

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

  // implement this method to read catalog name and properties during initialization
  public void initialize(String name, Map<String, String> properties) {
  }
}
```

Catalog implementations can be dynamically loaded in most compute engines.
For Spark and Flink, you can specify the `catalog-impl` catalog property to load it.
Read the [Configuration](../configuration/#catalog-properties) section for more details.
For MapReduce, implement `org.apache.iceberg.mr.CatalogLoader` and set Hadoop property `iceberg.mr.catalog.loader.class` to load it.
If your catalog must read Hadoop configuration to access certain environment properties, make your catalog implement `org.apache.hadoop.conf.Configurable`.

### Custom file IO implementation

Extend `FileIO` and provide implementation to read and write data files

Example:
```java
public class CustomFileIO implements FileIO {

  // must have a no-arg constructor to be dynamically loaded
  // initialize(Map<String, String> properties) will be called to complete initialization
  public CustomFileIO() {
  }

  @Override
  public InputFile newInputFile(String s) {
    // you also need to implement the InputFile interface for a custom input file
    return new CustomInputFile(s);
  }

  @Override
  public OutputFile newOutputFile(String s) {
    // you also need to implement the OutputFile interface for a custom output file
    return new CustomOutputFile(s);
  }

  @Override
  public void deleteFile(String path) {
    Path toDelete = new Path(path);
    FileSystem fs = Util.getFs(toDelete);
    try {
        fs.delete(toDelete, false /* not recursive */);
    } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to delete file: %s", path);
    }
  }

  // implement this method to read catalog properties during initialization
  public void initialize(Map<String, String> properties) {
  }
}
```

If you are already implementing your own catalog, you can implement `TableOperations.io()` to use your custom `FileIO`.
In addition, custom `FileIO` implementations can also be dynamically loaded in `HadoopCatalog` and `HiveCatalog` by specifying the `io-impl` catalog property.
Read the [Configuration](../configuration/#catalog-properties) section for more details.
If your `FileIO` must read Hadoop configuration to access certain environment properties, make your `FileIO` implement `org.apache.hadoop.conf.Configurable`.

### Custom location provider implementation

Extend `LocationProvider` and provide implementation to determine the file path to write data

Example:
```java
public class CustomLocationProvider implements LocationProvider {

  private String tableLocation;

  // must have a 2-arg constructor like this, or a no-arg constructor
  public CustomLocationProvider(String tableLocation, Map<String, String> properties) {
    this.tableLocation = tableLocation;
  }

  @Override
  public String newDataLocation(String filename) {
    // can use any custom method to generate a file path given a file name
    return String.format("%s/%s/%s", tableLocation, UUID.randomUUID().toString(), filename);
  }

  @Override
  public String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename) {
    // can use any custom method to generate a file path given a partition info and file name
    return newDataLocation(filename);
  }
}
```

If you are already implementing your own catalog, you can override `TableOperations.locationProvider()` to use your custom default `LocationProvider`.
To use a different custom location provider for a specific table, specify the implementation when creating the table using table property `write.location-provider.impl`

Example:
```sql
CREATE TABLE hive.default.my_table (
  id bigint,
  data string,
  category string)
USING iceberg
OPTIONS (
  'write.location-provider.impl'='com.my.CustomLocationProvider'
)
PARTITIONED BY (category);
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
