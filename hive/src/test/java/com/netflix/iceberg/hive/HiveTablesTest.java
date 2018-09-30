/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.iceberg.hive;

import com.netflix.iceberg.exceptions.CommitFailedException;
import com.netflix.iceberg.types.Types;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.netflix.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static com.netflix.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static com.netflix.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;

public class HiveTablesTest extends HiveTableBaseTest {
  @Test
  public void testCreate() throws TException {
    // Table should be created in hive metastore
    final Table table = metastoreClient.getTable(DB_NAME, TABLE_NAME);

    // check parameters are in expected state
    final Map<String, String> parameters = table.getParameters();
    Assert.assertNotNull(parameters);
    Assert.assertTrue(ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(parameters.get(TABLE_TYPE_PROP)));
    Assert.assertTrue(ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(table.getTableType()));

    // Ensure the table is pointing to empty location
    Assert.assertEquals(getTableLocation(TABLE_NAME) , table.getSd().getLocation());

    // Ensure it is stored as unpartitioned table in hive.
    Assert.assertEquals(0 , table.getPartitionKeysSize());

    // Only 1 snapshotFile Should exist and no manifests should exist
    Assert.assertEquals(1, metadataVersionFiles(TABLE_NAME).size());
    Assert.assertEquals(0, manifestFiles(TABLE_NAME).size());

    final com.netflix.iceberg.Table icebergTable = new HiveTables(hiveConf).load(DB_NAME, TABLE_NAME);
    // Iceberg schema should match the loaded table
    Assert.assertEquals(schema.asStruct(), icebergTable.schema().asStruct());
  }

  @Test
  public void testExistingTableUpdate() throws TException {
    com.netflix.iceberg.Table icebergTable = new HiveTables(hiveConf).load(DB_NAME, TABLE_NAME);
    // add a column
    icebergTable.updateSchema().addColumn("data", Types.LongType.get()).commit();

    icebergTable = new HiveTables(hiveConf).load(DB_NAME, TABLE_NAME);

    // Only 2 snapshotFile Should exist and no manifests should exist
    Assert.assertEquals(2, metadataVersionFiles(TABLE_NAME).size());
    Assert.assertEquals(0, manifestFiles(TABLE_NAME).size());
    Assert.assertEquals(altered.asStruct(), icebergTable.schema().asStruct());

    final Table table = metastoreClient.getTable(DB_NAME, TABLE_NAME);
    final List<String> hiveColumns = table.getSd().getCols().stream().map(f -> f.getName()).collect(Collectors.toList());
    final List<String> icebergColumns = altered.columns().stream().map(f -> f.name()).collect(Collectors.toList());
    Assert.assertEquals(icebergColumns, hiveColumns);
  }

  @Test(expected = CommitFailedException.class)
  public void testFailure() throws TException {
    com.netflix.iceberg.Table icebergTable = new HiveTables(hiveConf).load(DB_NAME, TABLE_NAME);
    final Table table = metastoreClient.getTable(DB_NAME, TABLE_NAME);
    final String dummyLocation = "dummylocation";
    table.getParameters().put(METADATA_LOCATION_PROP, dummyLocation);
    metastoreClient.alter_table(DB_NAME, TABLE_NAME, table);
    icebergTable.updateSchema()
            .addColumn("data", Types.LongType.get())
            .commit();
  }
}
