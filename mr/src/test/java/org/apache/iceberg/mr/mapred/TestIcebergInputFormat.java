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

package org.apache.iceberg.mr.mapred;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.TestHelpers;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(StandaloneHiveRunner.class)
public class TestIcebergInputFormat {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @HiveSQL(files = {}, autoStart = true)
  private HiveShell shell;

  private IcebergInputFormat inputFormat = new IcebergInputFormat();
  private File tableLocation;
  private JobConf conf = new JobConf();

  @Before
  public void before() throws IOException {
    tableLocation = temp.newFolder();
    Schema schema = new Schema(required(1, "id", Types.LongType.get()),
            optional(2, "data", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.unpartitioned();

    HadoopTables tables = new HadoopTables();
    Table table = tables.create(schema, spec, tableLocation.getAbsolutePath());

    List<Record> data = new ArrayList<>();
    data.add(TestHelpers.createSimpleRecord(1L, "Michael"));
    data.add(TestHelpers.createSimpleRecord(2L, "Andy"));
    data.add(TestHelpers.createSimpleRecord(3L, "Berta"));

    DataFile fileA = TestHelpers.writeFile(temp.newFile(), table, null, FileFormat.PARQUET, data);
    table.newAppend().appendFile(fileA).commit();
  }

  @Test
  public void testGetSplits() throws IOException {
    IcebergInputFormat format = new IcebergInputFormat();
    conf.set(InputFormatConfig.TABLE_LOCATION, tableLocation.getAbsolutePath());
    conf.set(InputFormatConfig.TABLE_NAME, "source_db.table_a");
    InputSplit[] splits = format.getSplits(conf, 1);
    assertEquals(splits.length, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetSplitsNoLocation() throws IOException {
    conf.set(InputFormatConfig.CATALOG_NAME, InputFormatConfig.HADOOP_TABLES);
    conf.set(InputFormatConfig.TABLE_NAME, "source_db.table_a");
    inputFormat.getSplits(conf, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetSplitsNoName() throws IOException {
    conf.set(InputFormatConfig.CATALOG_NAME, InputFormatConfig.HADOOP_TABLES);
    conf.set(InputFormatConfig.TABLE_LOCATION, "file:" + tableLocation.getAbsolutePath());
    inputFormat.getSplits(conf, 1);
  }

  @Ignore("Requires SerDe")
  @Test
  public void testInputFormat() {
    shell.execute("CREATE DATABASE source_db");
    shell.execute(new StringBuilder()
            .append("CREATE TABLE source_db.table_a ")
            .append("ROW FORMAT SERDE 'org.apache.iceberg.mr.mapred.IcebergSerDe' ")
            .append("STORED AS ")
            .append("INPUTFORMAT 'org.apache.iceberg.mr.mapred.IcebergInputFormat' ")
            .append("OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' ")
            .append("LOCATION '")
            .append(tableLocation.getAbsolutePath())
            .append("' TBLPROPERTIES ('iceberg.catalog'='hadoop.tables'")
            .append(")")
            .toString());

    List<Object[]> result = shell.executeStatement("SELECT id, data FROM source_db.table_a");

    assertEquals(3, result.size());
    assertArrayEquals(new Object[]{1L, "Michael"}, result.get(0));
    assertArrayEquals(new Object[]{2L, "Andy"}, result.get(1));
    assertArrayEquals(new Object[]{3L, "Berta"}, result.get(2));
  }

  private List<Record> readRecords(JobConf jobConf) throws IOException {
    InputSplit[] splits = inputFormat.getSplits(jobConf, 1);
    RecordReader reader = inputFormat.getRecordReader(splits[0], jobConf, null);
    List<Record> records = new ArrayList<>();
    IcebergWritable value = (IcebergWritable) reader.createValue();
    while (reader.next(null, value)) {
      records.add(value.getRecord().copy());
    }
    return records;
  }

}
