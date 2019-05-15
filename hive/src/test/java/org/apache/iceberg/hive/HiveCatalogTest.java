package org.apache.iceberg.hive;

import com.google.common.util.concurrent.MoreExecutors;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Tasks;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;

public class HiveCatalogTest extends HiveCatalogBaseTest {
  private HiveCatalog hiveCatalog = new HiveCatalog(hiveConf);

  @Test
  public void testCreate() throws TException {
    // Table should be created in hive metastore
    varifyTable(TABLE_IDENTIFIER);
  }

  @Test
  public void testRename() throws TException {
    varifyTable(TABLE_IDENTIFIER);

    String renamedTableName = "rename_table_name";
    TableIdentifier renameTableIdentifier = new TableIdentifier(TABLE_IDENTIFIER.namespace(), renamedTableName);
    hiveCatalog.renameTable(this.TABLE_IDENTIFIER, renameTableIdentifier);

    try {
      hiveCatalog.getTable(TABLE_IDENTIFIER);
      Assert.fail("Should have thrown NoSuchTableException");
    } catch (NoSuchTableException expected) {

    }
    varifyTable(renameTableIdentifier);
    hiveCatalog.dropTable(renameTableIdentifier);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDrop() throws TException {
    // verify the table actually exists
    metastoreClient.getTable(DB_NAME, TABLE_NAME);

    hiveCatalog.dropTable(TABLE_IDENTIFIER);

    metastoreClient.getTable(DB_NAME, TABLE_NAME);
  }

  private void varifyTable(TableIdentifier tableIdentifier) throws TException {
    // Table should be renamed in hive metastore
    String tableName = tableIdentifier.name();
    final org.apache.hadoop.hive.metastore.api.Table table = metastoreClient.getTable(DB_NAME, tableName);

    // check parameters are in expected state
    final Map<String, String> parameters = table.getParameters();
    Assert.assertNotNull(parameters);
    Assert.assertTrue(ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(parameters.get(TABLE_TYPE_PROP)));
    Assert.assertTrue(ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(table.getTableType()));

    // Ensure the table is pointing to empty location
    Assert.assertEquals(getTableLocation(tableName), table.getSd().getLocation());

    // Ensure it is stored as unpartitioned table in hive.
    Assert.assertEquals(0, table.getPartitionKeysSize());

    // Only 1 snapshotFile Should exist and no manifests should exist
    Assert.assertEquals(1, metadataVersionFiles(tableName).size());
    Assert.assertEquals(0, manifestFiles(tableName).size());

    final Table icebergTable = hiveCatalog.getTable(tableIdentifier);
    // Iceberg schema should match the loaded table
    Assert.assertEquals(schema.asStruct(), icebergTable.schema().asStruct());
  }

  @Test
  public void testExistingTableUpdate() throws TException {
    Table icebergTable = catalog.getTable(TABLE_IDENTIFIER);
    // add a column
    icebergTable.updateSchema().addColumn("data", Types.LongType.get()).commit();

    icebergTable = catalog.getTable(TABLE_IDENTIFIER);;

    // Only 2 snapshotFile Should exist and no manifests should exist
    Assert.assertEquals(2, metadataVersionFiles(TABLE_NAME).size());
    Assert.assertEquals(0, manifestFiles(TABLE_NAME).size());
    Assert.assertEquals(altered.asStruct(), icebergTable.schema().asStruct());

    final org.apache.hadoop.hive.metastore.api.Table table = metastoreClient.getTable(DB_NAME, TABLE_NAME);
    final List<String> hiveColumns = table.getSd().getCols().stream().map(f -> f.getName()).collect(Collectors.toList());
    final List<String> icebergColumns = altered.columns().stream().map(f -> f.name()).collect(Collectors.toList());
    Assert.assertEquals(icebergColumns, hiveColumns);
  }

  @Test(expected = CommitFailedException.class)
  public void testFailure() throws TException {
    Table icebergTable = catalog.getTable(TABLE_IDENTIFIER);;
    org.apache.hadoop.hive.metastore.api.Table table = metastoreClient.getTable(DB_NAME, TABLE_NAME);
    String dummyLocation = "dummylocation";
    table.getParameters().put(METADATA_LOCATION_PROP, dummyLocation);
    metastoreClient.alter_table(DB_NAME, TABLE_NAME, table);
    icebergTable.updateSchema()
        .addColumn("data", Types.LongType.get())
        .commit();
  }

  @Test
  public void testConcurrentFastAppends() {
    Table icebergTable = catalog.getTable(TABLE_IDENTIFIER);;
    Table anotherIcebergTable = catalog.getTable(TABLE_IDENTIFIER);;

    String fileName = UUID.randomUUID().toString();
    DataFile file = DataFiles.builder(icebergTable.spec())
        .withPath(FileFormat.PARQUET.addExtension(fileName))
        .withRecordCount(2)
        .withFileSizeInBytes(0)
        .build();

    ExecutorService executorService = MoreExecutors.getExitingExecutorService(
        (ThreadPoolExecutor) Executors.newFixedThreadPool(2));

    AtomicInteger barrier = new AtomicInteger(0);
    Tasks.foreach(icebergTable, anotherIcebergTable)
        .stopOnFailure().throwFailureWhenFinished()
        .executeWith(executorService)
        .run(table -> {
          for (int numCommittedFiles = 0; numCommittedFiles < 10; numCommittedFiles++) {
            while (barrier.get() < numCommittedFiles * 2) {
              try {
                Thread.sleep(10);
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            }

            table.newFastAppend().appendFile(file).commit();
            barrier.incrementAndGet();
          }
        });

    icebergTable.refresh();
    Assert.assertEquals(20, icebergTable.currentSnapshot().manifests().size());
  }
}
