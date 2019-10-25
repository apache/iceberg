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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.required;


public class TestHiveMetadataPreservingCatalog extends HiveMetastoreTest {

  private static final String TABLE_NAME = "tbl";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(DB_NAME, TABLE_NAME);
  private static final Schema SCHEMA = new Schema(
      required(3, "id", Types.IntegerType.get()),
      required(4, "data", Types.StringType.get())
  );

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private String tableLocation;

  @BeforeClass
  public static void setCustomHiveCatalog() {
    catalog = new HiveMetadataPreservingCatalog(hiveConf);
  }

  @Before
  public void createTableLocation() throws IOException {
    tableLocation = temp.newFolder("hive-").getPath();
  }

  @After
  public void cleanup() {
    catalog.dropTable(TABLE_IDENTIFIER);
  }

  @Test
  public void shouldNotThrowErrorIfTableExists() {
    try {
      metastoreClient.createTable(hiveTable());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    catalog.createTable(TABLE_IDENTIFIER, SCHEMA);
  }

  @Test
  public void shouldNotOverrideExistingHiveMetadata() {
    try {
      metastoreClient.createTable(hiveTable());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    org.apache.iceberg.Table table = catalog.createTable(TABLE_IDENTIFIER, SCHEMA);
    Transaction txn = table.newTransaction();
    txn.updateSchema().addColumn("newCol", Types.IntegerType.get()).commit();
    txn.updateProperties().set("testProp", "dummy").commit();
    txn.commitTransaction();

    try {
      Table hiveTable = metastoreClient.getTable(DB_NAME, TABLE_NAME);
      Assert.assertEquals(2, hiveTable.getSd().getCols().size());
      Assert.assertFalse(hiveTable.getParameters().containsKey("testProp"));
      Assert.assertEquals("org.apache.hadoop.hive.ql.io.orc.OrcSerde",
          hiveTable.getSd().getSerdeInfo().getSerializationLib());
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  private Table hiveTable() {
    final long currentTimeMillis = System.currentTimeMillis();

    final StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setCols(ImmutableList.of(new FieldSchema("id", "int", ""),
        new FieldSchema("data", "string", "")));
    storageDescriptor.setLocation(tableLocation);
    storageDescriptor.setOutputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
    storageDescriptor.setInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat");
    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setSerializationLib("org.apache.hadoop.hive.ql.io.orc.OrcSerde");
    storageDescriptor.setSerdeInfo(serDeInfo);

    Table tbl = new Table(TABLE_NAME,
        DB_NAME,
        System.getProperty("user.name"),
        (int) currentTimeMillis / 1000,
        (int) currentTimeMillis / 1000,
        Integer.MAX_VALUE,
        storageDescriptor,
        Collections.emptyList(),
        new HashMap<>(),
        null,
        null,
        TableType.EXTERNAL_TABLE.toString());
    tbl.getParameters().put("EXTERNAL", "TRUE"); // using the external table type also requires this

    return tbl;
  }

}
