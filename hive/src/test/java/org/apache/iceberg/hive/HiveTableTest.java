/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.hive;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Tasks;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;

public class HiveTableTest extends HiveTableBaseTest {
  @Test
  public void testCreate() throws TException {
    // Table should be created in hive metastore
    // Table should be renamed in hive metastore
    String tableName = TABLE_IDENTIFIER.name();
    org.apache.hadoop.hive.metastore.api.Table table =
        metastoreClient.getTable(TABLE_IDENTIFIER.namespace().level(0), tableName);

    // check parameters are in expected state
    Map<String, String> parameters = table.getParameters();
    Assert.assertNotNull(parameters);
    Assert.assertTrue(ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(parameters.get(TABLE_TYPE_PROP)));
    Assert.assertTrue("EXTERNAL_TABLE".equalsIgnoreCase(table.getTableType()));

    // Ensure the table is pointing to empty location
    Assert.assertEquals(getTableLocation(tableName), table.getSd().getLocation());

    // Ensure it is stored as unpartitioned table in hive.
    Assert.assertEquals(0, table.getPartitionKeysSize());

    // Only 1 snapshotFile Should exist and no manifests should exist
    Assert.assertEquals(1, metadataVersionFiles(tableName).size());
    Assert.assertEquals(0, manifestFiles(tableName).size());

    final Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
    // Iceberg schema should match the loaded table
    Assert.assertEquals(schema.asStruct(), icebergTable.schema().asStruct());
  }

  @Test
  public void testRename() {
    String renamedTableName = "rename_table_name";
    TableIdentifier renameTableIdentifier = TableIdentifier.of(TABLE_IDENTIFIER.namespace(), renamedTableName);
    Table original = catalog.loadTable(TABLE_IDENTIFIER);

    catalog.renameTable(TABLE_IDENTIFIER, renameTableIdentifier);
    Assert.assertFalse(catalog.tableExists(TABLE_IDENTIFIER));
    Assert.assertTrue(catalog.tableExists(renameTableIdentifier));

    Table renamed = catalog.loadTable(renameTableIdentifier);

    Assert.assertEquals(original.schema().asStruct(), renamed.schema().asStruct());
    Assert.assertEquals(original.spec(), renamed.spec());
    Assert.assertEquals(original.location(), renamed.location());
    Assert.assertEquals(original.currentSnapshot(), renamed.currentSnapshot());

    Assert.assertTrue(catalog.dropTable(renameTableIdentifier));
  }

  @Test
  public void testDrop() {
    Assert.assertTrue("Table should exist", catalog.tableExists(TABLE_IDENTIFIER));
    Assert.assertTrue("Drop should return true and drop the table", catalog.dropTable(TABLE_IDENTIFIER));
    Assert.assertFalse("Table should not exist", catalog.tableExists(TABLE_IDENTIFIER));
  }

  @Test
  public void testDropLeavesTableData() throws IOException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);

    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(AvroSchemaUtil.convert(schema, "test"));
    List<GenericData.Record> records = Lists.newArrayList(
        recordBuilder.set("id", 1L).build(),
        recordBuilder.set("id", 2L).build(),
        recordBuilder.set("id", 3L).build()
    );

    String fileLocation = table.location().replace("file:", "") + "/data/file.avro";
    try (FileAppender<GenericData.Record> writer = Avro.write(Files.localOutput(fileLocation))
        .schema(schema)
        .named("test")
        .build()) {
      for (GenericData.Record rec : records) {
        writer.add(rec);
      }
    }

    DataFile file = DataFiles.builder(table.spec())
        .withRecordCount(3)
        .withPath(fileLocation)
        .withFileSizeInBytes(Files.localInput(fileLocation).getLength())
        .build();

    table.newAppend().appendFile(file).commit();

    String manifestListLocation = table.currentSnapshot().manifestListLocation().replace("file:", "");

    Assert.assertTrue("Drop should return true and drop the table", catalog.dropTable(TABLE_IDENTIFIER));
    Assert.assertFalse("Table should not exist", catalog.tableExists(TABLE_IDENTIFIER));

    Assert.assertTrue("Table data files should exist",
        new File(fileLocation).exists());
    Assert.assertTrue("Table metadata files should exist",
        new File(manifestListLocation).exists());
  }

  @Test
  public void testExistingTableUpdate() throws TException {
    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
    // add a column
    icebergTable.updateSchema().addColumn("data", Types.LongType.get()).commit();

    icebergTable = catalog.loadTable(TABLE_IDENTIFIER);

    // Only 2 snapshotFile Should exist and no manifests should exist
    Assert.assertEquals(2, metadataVersionFiles(TABLE_NAME).size());
    Assert.assertEquals(0, manifestFiles(TABLE_NAME).size());
    Assert.assertEquals(altered.asStruct(), icebergTable.schema().asStruct());

    final org.apache.hadoop.hive.metastore.api.Table table = metastoreClient.getTable(DB_NAME, TABLE_NAME);
    final List<String> hiveColumns = table.getSd().getCols().stream()
        .map(FieldSchema::getName)
        .collect(Collectors.toList());
    final List<String> icebergColumns = altered.columns().stream()
        .map(Types.NestedField::name)
        .collect(Collectors.toList());
    Assert.assertEquals(icebergColumns, hiveColumns);
  }

  @Test(expected = CommitFailedException.class)
  public void testFailure() throws TException {
    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
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
    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
    Table anotherIcebergTable = catalog.loadTable(TABLE_IDENTIFIER);

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

  @Test
  public void testRegisterTable() throws TException {
    org.apache.hadoop.hive.metastore.api.Table originalTable = metastoreClient.getTable(DB_NAME, TABLE_NAME);

    Map<String, String> originalParams = originalTable.getParameters();
    Assert.assertNotNull(originalParams);
    Assert.assertTrue(ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(originalParams.get(TABLE_TYPE_PROP)));
    Assert.assertTrue("EXTERNAL_TABLE".equalsIgnoreCase(originalTable.getTableType()));

    catalog.dropTable(TABLE_IDENTIFIER);
    Assert.assertFalse(catalog.tableExists(TABLE_IDENTIFIER));

    List<String> metadataVersionFiles = metadataVersionFiles(TABLE_NAME);
    Assert.assertEquals(1, metadataVersionFiles.size());

    catalog.registerTable(TABLE_IDENTIFIER, "file:" + metadataVersionFiles.get(0));

    org.apache.hadoop.hive.metastore.api.Table newTable = metastoreClient.getTable(DB_NAME, TABLE_NAME);

    Map<String, String> newTableParameters = newTable.getParameters();
    Assert.assertNull(newTableParameters.get(PREVIOUS_METADATA_LOCATION_PROP));
    Assert.assertEquals(originalParams.get(TABLE_TYPE_PROP), newTableParameters.get(TABLE_TYPE_PROP));
    Assert.assertEquals(originalParams.get(METADATA_LOCATION_PROP), newTableParameters.get(METADATA_LOCATION_PROP));
    Assert.assertEquals(originalTable.getSd(), newTable.getSd());
  }

  @Test
  public void testCloneTable() throws IOException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);

    List<String> metadataVersionFiles = metadataVersionFiles(TABLE_NAME);
    Assert.assertEquals(1, metadataVersionFiles.size());

    TableIdentifier anotherTableIdentifier = TableIdentifier.of(DB_NAME, TABLE_NAME + "_new");
    Table anotherTable = catalog.registerTable(anotherTableIdentifier, metadataVersionFiles.get(0));

    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(AvroSchemaUtil.convert(schema, "test"));
    List<GenericData.Record> records = Lists.newArrayList(
        recordBuilder.set("id", 1L).build(),
        recordBuilder.set("id", 2L).build(),
        recordBuilder.set("id", 3L).build()
    );

    String fileLocation = table.location().replace("file:", "") + "/data/file-1.avro";
    try (FileAppender<GenericData.Record> writer = Avro.write(Files.localOutput(fileLocation))
        .schema(schema)
        .named("test")
        .build()) {
      for (GenericData.Record rec : records) {
        writer.add(rec);
      }
    }
    DataFile file = DataFiles.builder(table.spec())
        .withRecordCount(3)
        .withPath(fileLocation)
        .withFileSizeInBytes(Files.localInput(fileLocation).getLength())
        .build();
    table.newAppend().appendFile(file).commit();

    String anotherFileLocation = anotherTable.location().replace("file:", "") + "/data/file-2.avro";
    try (FileAppender<GenericData.Record> writer = Avro.write(Files.localOutput(anotherFileLocation))
        .schema(schema)
        .named("test")
        .build()) {
      for (GenericData.Record rec : records) {
        writer.add(rec);
      }
    }
    DataFile anotherFile = DataFiles.builder(anotherTable.spec())
        .withRecordCount(3)
        .withPath(anotherFileLocation)
        .withFileSizeInBytes(Files.localInput(anotherFileLocation).getLength())
        .build();
    anotherTable.newAppend().appendFile(anotherFile).commit();

    // verify that both tables continue to function independently
    Assert.assertNotEquals(table.currentSnapshot().manifests(), anotherTable.currentSnapshot().manifests());
  }
}
