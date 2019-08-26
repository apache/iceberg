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

    protected CustomTableOperations(Configuration conf, String dbName, String tableName) {
        super(conf);
        this.conf = conf;
        this.dbName = dbName;
        this.tableName = tableName;
    }

    // The refresh method should provide implementation on how to get the metadata location
    @Override
    public TableMetadata refresh() {

        // Example custom service which returns the metadata location given a dbName and tableName
        val metadataLocation = CustomService.getMetadataForTable(conf, dbName, tableName)

        // Use existing method to refresh metadata  
        refreshFromMetadataLocation(metadataLocation);

        // Use existing method to return the table metadata
        return current();
    }

    // The commit method should provide implementation on how to persist the metadata location
    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
        // if the metadata is already out of date, reject it
        if (base != current()) {
            throw new CommitFailedException("Cannot commit: stale table metadata for %s.%s", dbName, tableName);
        }

        // if the metadata is not changed, return early
        if (base == metadata) {
            return;
        }

        // Write new metadata
        String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);

        // Example custom service which updates the metadata location for the given db and table
        CustomService.updateMetadataLocation(dbName, tableName, newMetadataLocation);

        // Use existing method to request a refresh
        requestRefresh();
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
        String tableLocation = configuration.get("custom.iceberg.table.location");

        // Can be an s3 or hdfs path
        if (tableLocation == null) {
            throw new RuntimeException("custom.iceberg.table.location configuration not set!");
        }

        return String.format(
                "%s/%s.db/%s", tableLocation,
                tableIdentifier.namespace().levels()[0],
                tableIdentifier.name());
    }

    @Override
    public boolean dropTable(TableIdentifier identifier) {
        throw new RuntimeException("Not Supported");
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {
        throw new RuntimeException("Not Supported");
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