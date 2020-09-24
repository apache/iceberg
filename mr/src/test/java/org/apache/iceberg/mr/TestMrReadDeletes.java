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

package org.apache.iceberg.mr;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.data.DeletesReadTest;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestMrReadDeletes extends DeletesReadTest {
  private TestHelper helper;
  private InputFormatConfig.ConfigBuilder builder;
  private Configuration conf;

  // parametrized variables
  private final TestIcebergInputFormats.TestInputFormat.Factory<Record> testInputFormat;
  private final FileFormat fileFormat;

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { "IcebergInputFormat", FileFormat.PARQUET },
        new Object[] { "IcebergInputFormat", FileFormat.AVRO },
        new Object[] { "IcebergInputFormat", FileFormat.ORC },
        new Object[] { "MapredIcebergInputFormat", FileFormat.PARQUET },
        new Object[] { "MapredIcebergInputFormat", FileFormat.AVRO },
        new Object[] { "MapredIcebergInputFormat", FileFormat.ORC },
    };
  }

  @Override
  public Table createTable(String name, Schema schema, PartitionSpec spec) throws IOException {
    Table table;
    conf = new Configuration();
    HadoopTables tables = new HadoopTables(conf);
    File location = temp.newFolder(testInputFormat.name(), fileFormat.name());
    Assert.assertTrue(location.delete());
    helper = new TestHelper(conf, tables, location.toString(), schema, spec, fileFormat, temp);
    table = helper.createTable();

    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata meta = ops.current();
    ops.commit(meta, meta.upgradeToFormatVersion(2));

    return table;
  }

  @Override
  public StructLikeSet rowSet(Table table, String... columns) {
    Schema projected = table.schema().select(columns);
    StructLikeSet set = StructLikeSet.create(projected.asStruct());
    set.addAll(testInputFormat.create(builder.project(projected).conf()).getRecords());

    return set;
  }

  public TestMrReadDeletes(String inputFormat, FileFormat fileFormat) {
    this.testInputFormat = TestIcebergInputFormats.TestInputFormat.newFactory(inputFormat,
        TestIcebergInputFormats.TestIcebergInputFormat::create);
    this.fileFormat = fileFormat;
  }

  @Before
  public void prepareData() throws IOException {
    this.table = createTable(null, SCHEMA, SPEC);
    generateTestData();
    helper.appendToTable(dataFile);

    builder = new InputFormatConfig.ConfigBuilder(conf).readFrom(table.location());
  }
}
