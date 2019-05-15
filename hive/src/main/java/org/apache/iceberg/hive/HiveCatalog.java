package org.apache.iceberg.hive;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.thrift.TException;

import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;

public class HiveCatalog extends BaseMetastoreCatalog {

  private final HiveClientPool clients;
  private final Configuration conf;

  public HiveCatalog(Configuration conf) {
    super(conf);
    this.conf = conf;
    this.clients = new HiveClientPool(2, conf);
  }

  @Override
  public void dropTable(TableIdentifier tableIdentifier) {
    validateTableIdentifier(tableIdentifier);
    HiveMetaStoreClient hiveMetaStoreClient = this.clients.newClient();
    try {
      hiveMetaStoreClient.dropTable(tableIdentifier.namespace().levels()[0], tableIdentifier.name());
    } catch (TException e) {
      throw new RuntimeException("Failed to drop " + tableIdentifier.toString(), e);
    }
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    validateTableIdentifier(from);
    validateTableIdentifier(to);

    HiveMetaStoreClient hiveMetaStoreClient = this.clients.newClient();
    String location = ((BaseTable) getTable(from)).operations().current().file().location();
    String newDBName = to.namespace().levels()[0];
    String oldDBName = from.namespace().levels()[0];
    String newTableName = to.name();
    String oldTableName = from.name();

    String newLocation = location.replaceFirst(oldDBName, newDBName)
        .replaceFirst(oldTableName, newTableName);
    try {
      Table table = hiveMetaStoreClient.getTable(oldDBName, oldTableName);

      // hive metastore renames the table's directory as part of renaming the table.
      // To ensure that the newly renamed table's METADATA_LOCATION_PROP is pointing
      // at the correct location we are updating it in the same alter call.
      table.getParameters().put(METADATA_LOCATION_PROP, newLocation);

      table.setDbName(to.namespace().levels()[0]);
      table.setTableName(to.name());
      hiveMetaStoreClient.alter_table(oldDBName, oldTableName, table);
    } catch (TException e) {
      throw new RuntimeException("Failed to rename " + from.toString() + " to " + to.toString(), e);
    }
  }

  @Override
  public TableOperations newTableOps(
      Configuration configuration, TableIdentifier tableIdentifier) {
    String dbName = tableIdentifier.namespace().levels()[0];
    String tableName = tableIdentifier.name();
    return new HiveTableOperations(configuration, clients, dbName, tableName);
  }

  @Override
  public void close() throws IOException {
    clients.close();
  }
}
