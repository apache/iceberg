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

package org.apache.iceberg;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructProjection;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.TableTestBase.FILE_A;
import static org.apache.iceberg.TableTestBase.FILE_A2_DELETES;
import static org.apache.iceberg.TableTestBase.FILE_A_DELETES;
import static org.apache.iceberg.TableTestBase.FILE_B;
import static org.apache.iceberg.TableTestBase.FILE_B_DELETES;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestPartitionTableScans {

  private static final Configuration CONF = new Configuration();
  private static final HadoopTables TABLES = new HadoopTables(CONF);

  private static final Schema SCHEMA =
      new Schema(required(1, "id", Types.IntegerType.get()), required(2, "data", Types.StringType.get()));

  private Table table;
  private File tableLocation;

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  @Before
  public void before() throws IOException {
    tableLocation = new File(temp.newFolder(), "table");
  }

  private void preparePartitionedTable() {
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newFastAppend().appendFile(FILE_B).commit();
  }

  private void preparePartitionedTableWithDeleteFiles() {
    table.newRowDelta().addRows(FILE_A).addDeletes(FILE_A_DELETES).commit();
    table.newRowDelta().addDeletes(FILE_A2_DELETES).commit();
    table.newRowDelta().addRows(FILE_B).addDeletes(FILE_B_DELETES).commit();
  }

  @Test
  public void testPartitionsTableScanWithDeleteFilesInFilter() {
    table = TABLES.create(
        SCHEMA,
        PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build(),
        tableLocation.toString());
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();
    preparePartitionedTableWithDeleteFiles();

    PartitionsTable partitionsTable = (PartitionsTable) TABLES.load(table.location() + "#partitions");

    // filter bucket = 0
    Expression set = Expressions.equal("partition.data_bucket", 0);
    StaticTableScan staticTableScan = (StaticTableScan) partitionsTable.newScan().filter(set);
    DataTask task = partitionsTable.task(staticTableScan);

    List<StructLike> rows = Lists.newArrayList(task.rows());
    Assert.assertEquals("Should have one record", 1, rows.size());

    StructLike record = rows.get(0);
    Assert.assertEquals(
        "Should have correct bucket partition",
        0,
        record.get(0, StructProjection.class).get(0, Integer.class).intValue());
    Assert.assertEquals(
        "Should have correct records",
        FILE_A.recordCount(),
        record.get(1, Long.class).longValue());
    Assert.assertEquals("Should have correct file counts", 1,
        record.get(2, Integer.class).intValue());
    Assert.assertEquals("Should have correct delete file count", 2,
        record.get(3, Integer.class).intValue());
    Assert.assertEquals("Should have correct equality delete count", FILE_A_DELETES.recordCount(),
        record.get(4, Long.class).intValue());
    Assert.assertEquals("Should have correct position delete count", FILE_A2_DELETES.recordCount(),
        record.get(5, Long.class).intValue());
  }

  @Test
  public void testPartitionsTableScanWithoutDeleteFilesInFilter() {
    table = TABLES.create(
        SCHEMA,
        PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build(),
        tableLocation.toString());
    preparePartitionedTable();

    PartitionsTable partitionsTable = (PartitionsTable) TABLES.load(table.location() + "#partitions");

    Expression set = Expressions.equal("partition.data_bucket", 0);
    StaticTableScan staticTableScan = (StaticTableScan) partitionsTable.newScan().filter(set);
    DataTask task = partitionsTable.task(staticTableScan);

    List<StructLike> rows = Lists.newArrayList(task.rows());
    Assert.assertEquals("Should have 1 records", 1, rows.size());

    StructLike record = rows.get(0);
    Assert.assertEquals(
        "Should have correct bucket partition",
        0,
        record.get(0, StructProjection.class).get(0, Integer.class).intValue());
    Assert.assertEquals("Should have correct records", FILE_A.recordCount(),
        record.get(1, Long.class).longValue());
    Assert.assertEquals("Should have correct file counts", 1,
        record.get(2, Integer.class).intValue());
    Assert.assertEquals("Should have correct delete file count", 0,
        record.get(3, Integer.class).intValue());
    Assert.assertEquals("Should have correct equality delete count", 0L,
        record.get(4, Long.class).intValue());
    Assert.assertEquals("Should have correct position delete count", 0L,
        record.get(5, Long.class).intValue());
  }
}
