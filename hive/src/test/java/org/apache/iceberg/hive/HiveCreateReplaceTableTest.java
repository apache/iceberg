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

import com.google.common.collect.Maps;
import java.io.IOException;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.AssertHelpers;
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
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.PartitionSpec.builderFor;
import static org.apache.iceberg.types.Types.NestedField.required;

public class HiveCreateReplaceTableTest extends HiveMetastoreTest {

  private static final String TABLE_NAME = "tbl";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(DB_NAME, TABLE_NAME);
  private static final Schema SCHEMA = new Schema(
      required(3, "id", Types.IntegerType.get()),
      required(4, "data", Types.StringType.get())
  );
  private static final PartitionSpec SPEC = builderFor(SCHEMA)
      .identity("id")
      .build();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

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

    Transaction txn = catalog.newCreateTableTransaction(
        TABLE_IDENTIFIER, SCHEMA, SPEC, tableLocation, Maps.newHashMap());
    txn.updateProperties()
        .set("prop", "value")
        .commit();

    // verify the table is still not visible before the transaction is committed
    Assert.assertFalse(catalog.tableExists(TABLE_IDENTIFIER));

    txn.commitTransaction();

    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    Assert.assertEquals("Table props should match", "value", table.properties().get("prop"));
  }

  @Test
  public void testCreateTableTxnTableCreatedConcurrently() {
    Assert.assertFalse("Table should not exist", catalog.tableExists(TABLE_IDENTIFIER));

    Transaction txn = catalog.newCreateTableTransaction(
        TABLE_IDENTIFIER, SCHEMA, SPEC, tableLocation, Maps.newHashMap());

    // create the table concurrently
    catalog.createTable(TABLE_IDENTIFIER, SCHEMA, SPEC);
    Assert.assertTrue("Table should be created", catalog.tableExists(TABLE_IDENTIFIER));

    AssertHelpers.assertThrows(
        "Create table txn should fail",
        AlreadyExistsException.class,
        "Table already exists: hivedb.tbl",
        txn::commitTransaction);
  }

  @Test
  public void testCreateTableTxnAndAppend() {
    Assert.assertFalse("Table should not exist", catalog.tableExists(TABLE_IDENTIFIER));

    Transaction txn = catalog.newCreateTableTransaction(
        TABLE_IDENTIFIER, SCHEMA, SPEC, tableLocation, Maps.newHashMap());

    AppendFiles append = txn.newAppend();
    DataFile dataFile = DataFiles.builder(SPEC)
        .withPath("/path/to/data-a.parquet")
        .withFileSizeInBytes(0)
        .withRecordCount(1)
        .build();
    append.appendFile(dataFile);
    append.commit();
    txn.commitTransaction();

    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    Snapshot snapshot = table.currentSnapshot();
    Assert.assertTrue("Table should have one manifest file", snapshot.manifests().size() == 1);
  }

  @Test
  public void testCreateTableTxnTableAlreadyExists() {
    Assert.assertFalse("Table should not exist", catalog.tableExists(TABLE_IDENTIFIER));

    // create a table before starting a transaction
    catalog.createTable(TABLE_IDENTIFIER, SCHEMA, SPEC);
    Assert.assertTrue("Table should be created", catalog.tableExists(TABLE_IDENTIFIER));

    AssertHelpers.assertThrows(
        "Should not be possible to start a new create table txn",
        AlreadyExistsException.class,
        "Table already exists: hivedb.tbl",
        () -> catalog.newCreateTableTransaction(TABLE_IDENTIFIER, SCHEMA, SPEC, tableLocation, Maps.newHashMap()));
  }

  @Test
  public void testReplaceTableTxn() {
    catalog.createTable(TABLE_IDENTIFIER, SCHEMA, SPEC, tableLocation, Maps.newHashMap());
    Assert.assertTrue("Table should exist", catalog.tableExists(TABLE_IDENTIFIER));

    Transaction txn = catalog.newReplaceTableTransaction(TABLE_IDENTIFIER, SCHEMA, false);
    txn.commitTransaction();

    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    Assert.assertEquals("Partition spec should match", PartitionSpec.unpartitioned(), table.spec());
  }

  @Test
  public void testReplaceTableTxnTableNotExists() {
    AssertHelpers.assertThrows(
        "Should not be possible to start a new replace table txn",
        NoSuchTableException.class,
        "No such table: hivedb.tbl",
        () -> catalog.newReplaceTableTransaction(TABLE_IDENTIFIER, SCHEMA, SPEC, false));
  }

  @Test
  public void testReplaceTableTxnTableDeletedConcurrently() {
    catalog.createTable(TABLE_IDENTIFIER, SCHEMA, SPEC, tableLocation, Maps.newHashMap());
    Assert.assertTrue("Table should exist", catalog.tableExists(TABLE_IDENTIFIER));

    Transaction txn = catalog.newReplaceTableTransaction(TABLE_IDENTIFIER, SCHEMA, SPEC, false);

    catalog.dropTable(TABLE_IDENTIFIER);

    txn.updateProperties()
        .set("prop", "value")
        .commit();

    AssertHelpers.assertThrows(
        "Replace table txn should fail",
        NoSuchTableException.class,
        "No such table: hivedb.tbl",
        txn::commitTransaction);
  }

  @Test
  public void testReplaceTableTxnTableModifiedConcurrently() {
    Table table = catalog.createTable(TABLE_IDENTIFIER, SCHEMA, SPEC, tableLocation, Maps.newHashMap());
    Assert.assertTrue("Table should exist", catalog.tableExists(TABLE_IDENTIFIER));

    Transaction txn = catalog.newReplaceTableTransaction(TABLE_IDENTIFIER, SCHEMA, SPEC, false);

    // update the table concurrently
    table.updateProperties()
        .set("another-prop", "another-value")
        .commit();

    txn.updateProperties()
        .set("prop", "value")
        .commit();
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
    txn.updateProperties()
        .set("prop", "value")
        .commit();
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
    Assert.assertEquals("Partition spec should match", PartitionSpec.unpartitioned(), table.spec());
  }

  @Test
  public void testCreateOrReplaceTableTxnTableDeletedConcurrently() {
    Assert.assertFalse("Table should not exist", catalog.tableExists(TABLE_IDENTIFIER));

    catalog.createTable(TABLE_IDENTIFIER, SCHEMA, SPEC);
    Assert.assertTrue("Table should be created", catalog.tableExists(TABLE_IDENTIFIER));

    Transaction txn = catalog.newReplaceTableTransaction(
        TABLE_IDENTIFIER, SCHEMA, PartitionSpec.unpartitioned(), tableLocation, Maps.newHashMap(), true);
    txn.updateProperties()
        .set("prop", "value")
        .commit();

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

    Transaction txn = catalog.newReplaceTableTransaction(
        TABLE_IDENTIFIER, SCHEMA, PartitionSpec.unpartitioned(), tableLocation, Maps.newHashMap(), true);
    txn.updateProperties()
        .set("prop", "value")
        .commit();

    // create the table concurrently
    catalog.createTable(TABLE_IDENTIFIER, SCHEMA, SPEC);
    Assert.assertTrue("Table should be created", catalog.tableExists(TABLE_IDENTIFIER));

    // expect the transaction to succeed anyway
    txn.commitTransaction();

    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    Assert.assertEquals("Partition spec should match", PartitionSpec.unpartitioned(), table.spec());
    Assert.assertEquals("Table props should match", "value", table.properties().get("prop"));
  }
}
