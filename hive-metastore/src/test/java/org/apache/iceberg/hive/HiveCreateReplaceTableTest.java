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

import static org.apache.iceberg.PartitionSpec.builderFor;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class HiveCreateReplaceTableTest extends HiveMetastoreTest {

  private static final String TABLE_NAME = "tbl";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(DB_NAME, TABLE_NAME);
  private static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get()), required(4, "data", Types.StringType.get()));
  private static final PartitionSpec SPEC = builderFor(SCHEMA).identity("id").build();

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private String tableLocation;

  @Before
  public void createTableLocation() throws IOException {
    tableLocation = temp.newFolder("hive-").getPath();
  }

  @After
  public void cleanup() {
    catalog.dropTable(TABLE_IDENTIFIER);
  }

  @Test
  public void testCreateTableTxn() {
    Assert.assertFalse("Table should not exist", catalog.tableExists(TABLE_IDENTIFIER));

    Transaction txn =
        catalog.newCreateTableTransaction(
            TABLE_IDENTIFIER, SCHEMA, SPEC, tableLocation, Maps.newHashMap());
    txn.updateProperties().set("prop", "value").commit();

    // verify the table is still not visible before the transaction is committed
    Assert.assertFalse(catalog.tableExists(TABLE_IDENTIFIER));

    txn.commitTransaction();

    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    Assert.assertEquals("Table props should match", "value", table.properties().get("prop"));
  }

  @Test
  public void testCreateTableTxnTableCreatedConcurrently() {
    Assert.assertFalse("Table should not exist", catalog.tableExists(TABLE_IDENTIFIER));

    Transaction txn =
        catalog.newCreateTableTransaction(
            TABLE_IDENTIFIER, SCHEMA, SPEC, tableLocation, Maps.newHashMap());

    // create the table concurrently
    catalog.createTable(TABLE_IDENTIFIER, SCHEMA, SPEC);
    Assert.assertTrue("Table should be created", catalog.tableExists(TABLE_IDENTIFIER));

    Assertions.assertThatThrownBy(txn::commitTransaction)
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessage("Table already exists: hivedb.tbl");
  }

  @Test
  public void testCreateTableTxnAndAppend() {
    Assert.assertFalse("Table should not exist", catalog.tableExists(TABLE_IDENTIFIER));

    Transaction txn =
        catalog.newCreateTableTransaction(
            TABLE_IDENTIFIER, SCHEMA, SPEC, tableLocation, Maps.newHashMap());

    AppendFiles append = txn.newAppend();
    DataFile dataFile =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-a.parquet")
            .withFileSizeInBytes(0)
            .withRecordCount(1)
            .build();
    append.appendFile(dataFile);
    append.commit();
    txn.commitTransaction();

    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    Snapshot snapshot = table.currentSnapshot();
    Assert.assertTrue(
        "Table should have one manifest file", snapshot.allManifests(table.io()).size() == 1);
  }

  @Test
  public void testCreateTableTxnTableAlreadyExists() {
    Assert.assertFalse("Table should not exist", catalog.tableExists(TABLE_IDENTIFIER));

    // create a table before starting a transaction
    catalog.createTable(TABLE_IDENTIFIER, SCHEMA, SPEC);
    Assert.assertTrue("Table should be created", catalog.tableExists(TABLE_IDENTIFIER));

    Assertions.assertThatThrownBy(
            () ->
                catalog.newCreateTableTransaction(
                    TABLE_IDENTIFIER, SCHEMA, SPEC, tableLocation, Maps.newHashMap()))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessage("Table already exists: hivedb.tbl");
  }

  @Test
  public void testReplaceTableTxn() {
    catalog.createTable(TABLE_IDENTIFIER, SCHEMA, SPEC, tableLocation, Maps.newHashMap());
    Assert.assertTrue("Table should exist", catalog.tableExists(TABLE_IDENTIFIER));

    Transaction txn = catalog.newReplaceTableTransaction(TABLE_IDENTIFIER, SCHEMA, false);
    txn.commitTransaction();

    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    PartitionSpec v1Expected =
        PartitionSpec.builderFor(table.schema()).alwaysNull("id", "id").withSpecId(1).build();
    Assert.assertEquals("Table should have a spec with one void field", v1Expected, table.spec());
  }

  @Test
  public void testReplaceTableTxnTableNotExists() {
    Assertions.assertThatThrownBy(
            () -> catalog.newReplaceTableTransaction(TABLE_IDENTIFIER, SCHEMA, SPEC, false))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessage("Table does not exist: hivedb.tbl");
  }

  @Test
  public void testReplaceTableTxnTableDeletedConcurrently() {
    catalog.createTable(TABLE_IDENTIFIER, SCHEMA, SPEC, tableLocation, Maps.newHashMap());
    Assert.assertTrue("Table should exist", catalog.tableExists(TABLE_IDENTIFIER));

    Transaction txn = catalog.newReplaceTableTransaction(TABLE_IDENTIFIER, SCHEMA, SPEC, false);

    catalog.dropTable(TABLE_IDENTIFIER);

    txn.updateProperties().set("prop", "value").commit();

    Assertions.assertThatThrownBy(txn::commitTransaction)
        .isInstanceOf(NoSuchTableException.class)
        .hasMessage("No such table: hivedb.tbl");
  }

  @Test
  public void testReplaceTableTxnTableModifiedConcurrently() {
    Table table =
        catalog.createTable(TABLE_IDENTIFIER, SCHEMA, SPEC, tableLocation, Maps.newHashMap());
    Assert.assertTrue("Table should exist", catalog.tableExists(TABLE_IDENTIFIER));

    Transaction txn = catalog.newReplaceTableTransaction(TABLE_IDENTIFIER, SCHEMA, SPEC, false);

    // update the table concurrently
    table.updateProperties().set("another-prop", "another-value").commit();

    txn.updateProperties().set("prop", "value").commit();
    txn.commitTransaction();

    // the replace should still succeed
    table = catalog.loadTable(TABLE_IDENTIFIER);
    Assert.assertNull("Table props should be updated", table.properties().get("another-prop"));
    Assert.assertEquals("Table props should match", "value", table.properties().get("prop"));
  }

  @Test
  public void testCreateOrReplaceTableTxnTableNotExists() {
    Assert.assertFalse("Table should not exist", catalog.tableExists(TABLE_IDENTIFIER));

    Transaction txn = catalog.newReplaceTableTransaction(TABLE_IDENTIFIER, SCHEMA, SPEC, true);
    txn.updateProperties().set("prop", "value").commit();
    txn.commitTransaction();

    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    Assert.assertEquals("Table props should match", "value", table.properties().get("prop"));
  }

  @Test
  public void testCreateOrReplaceTableTxnTableExists() {
    catalog.createTable(TABLE_IDENTIFIER, SCHEMA, SPEC, tableLocation, Maps.newHashMap());
    Assert.assertTrue("Table should exist", catalog.tableExists(TABLE_IDENTIFIER));

    Transaction txn = catalog.newReplaceTableTransaction(TABLE_IDENTIFIER, SCHEMA, true);
    txn.commitTransaction();

    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    PartitionSpec v1Expected =
        PartitionSpec.builderFor(table.schema()).alwaysNull("id", "id").withSpecId(1).build();
    Assert.assertEquals("Table should have a spec with one void field", v1Expected, table.spec());
  }

  @Test
  public void testCreateOrReplaceTableTxnTableDeletedConcurrently() {
    Assert.assertFalse("Table should not exist", catalog.tableExists(TABLE_IDENTIFIER));

    catalog.createTable(TABLE_IDENTIFIER, SCHEMA, SPEC);
    Assert.assertTrue("Table should be created", catalog.tableExists(TABLE_IDENTIFIER));

    Transaction txn =
        catalog.newReplaceTableTransaction(
            TABLE_IDENTIFIER,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            tableLocation,
            Maps.newHashMap(),
            true);
    txn.updateProperties().set("prop", "value").commit();

    // drop the table concurrently
    catalog.dropTable(TABLE_IDENTIFIER);

    // expect the transaction to succeed anyway
    txn.commitTransaction();

    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    Assert.assertEquals("Table props should match", "value", table.properties().get("prop"));
  }

  @Test
  public void testCreateOrReplaceTableTxnTableCreatedConcurrently() {
    Assert.assertFalse("Table should not exist", catalog.tableExists(TABLE_IDENTIFIER));

    Transaction txn =
        catalog.newReplaceTableTransaction(
            TABLE_IDENTIFIER,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            tableLocation,
            Maps.newHashMap(),
            true);
    txn.updateProperties().set("prop", "value").commit();

    // create the table concurrently
    catalog.createTable(TABLE_IDENTIFIER, SCHEMA, SPEC);
    Assert.assertTrue("Table should be created", catalog.tableExists(TABLE_IDENTIFIER));

    // expect the transaction to succeed anyway
    txn.commitTransaction();

    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    Assert.assertEquals("Partition spec should match", PartitionSpec.unpartitioned(), table.spec());
    Assert.assertEquals("Table props should match", "value", table.properties().get("prop"));
  }

  @Test
  public void testCreateTableTxnWithGlobalTableLocation() {
    Assert.assertFalse("Table should not exist", catalog.tableExists(TABLE_IDENTIFIER));

    Transaction txn =
        catalog.newCreateTableTransaction(
            TABLE_IDENTIFIER, SCHEMA, SPEC, "file:///" + tableLocation, Maps.newHashMap());
    txn.commitTransaction();

    Table table = catalog.loadTable(TABLE_IDENTIFIER);

    DataFile dataFile =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-a.parquet")
            .withFileSizeInBytes(0)
            .withRecordCount(1)
            .build();

    table.newAppend().appendFile(dataFile).commit();

    Assert.assertEquals("Write should succeed", 1, Iterables.size(table.snapshots()));
  }
}
