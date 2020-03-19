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

import com.google.common.collect.Lists;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.iceberg.mr.mapred.IcebergInputFormat;
import org.iceberg.mr.mapred.IcebergWritable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


@RunWith(StandaloneHiveRunner.class)
public class TestIcebergInputFormat {

  @HiveSQL(files = {}, autoStart = true)
  private HiveShell shell;

  private File tableLocation;
  private Table table;

  @Before
  public void before() throws IOException {
    tableLocation = java.nio.file.Files.createTempDirectory("temp").toFile();
    Schema schema = new Schema(optional(1, "name", Types.StringType.get()),
        optional(2, "salary", Types.LongType.get()));
    PartitionSpec spec = PartitionSpec.unpartitioned();
    HadoopTables tables = new HadoopTables();
    table = tables.create(schema, spec, tableLocation.getAbsolutePath());

    DataFile fileA = DataFiles
        .builder(spec)
        .withPath("src/test/resources/test-table/data/00000-1-c7557bc3-ae0d-46fb-804e-e9806abf81c7-00000.parquet")
        .withFileSizeInBytes(1024)
        .withRecordCount(3) // needs at least one record or else metrics will filter it out
        .build();

    table.newAppend().appendFile(fileA).commit();
  }

  @Test
  public void testInputFormat() {
    shell.execute("CREATE DATABASE source_db");
    shell.execute(new StringBuilder()
            .append("CREATE TABLE source_db.table_a ")
            .append("ROW FORMAT SERDE 'com.expediagroup.hiveberg.IcebergSerDe' ")
            .append("STORED AS ")
            .append("INPUTFORMAT 'com.expediagroup.hiveberg.IcebergInputFormat' ")
            .append("OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' ")
            .append("LOCATION '")
            .append(tableLocation.getAbsolutePath())
            .append("'")
            .toString());

    List<Object[]> result = shell.executeStatement("SELECT * FROM source_db.table_a");

    assertEquals(3, result.size());
    assertArrayEquals(new Object[]{"Michael", 3000L}, result.get(0));
    assertArrayEquals(new Object[]{"Andy", 3000L}, result.get(1));
    assertArrayEquals(new Object[]{"Berta", 4000L}, result.get(2));
  }

  @Test
  public void testGetSplits() throws IOException {
    IcebergInputFormat format = new IcebergInputFormat();
    JobConf conf = new JobConf();
    conf.set("location", "file:" + tableLocation);
    InputSplit[] splits = format.getSplits(conf, 1);
    assertEquals(splits.length, 1);
  }

  @Test
  public void testGetRecordReader() throws IOException {
    IcebergInputFormat format = new IcebergInputFormat();
    JobConf conf = new JobConf();
    conf.set("location", "file:" + tableLocation);
    InputSplit[] splits = format.getSplits(conf, 1);
    RecordReader reader = format.getRecordReader(splits[0], conf, null);
    IcebergWritable value = (IcebergWritable) reader.createValue();

    List<Record> records = Lists.newArrayList();
    boolean unfinished = true;
    while (unfinished) {
      if (reader.next(null, value)) {
        records.add(value.getRecord().copy());
      } else  {
        unfinished = false;
      }
    }
    assertEquals(3, records.size());
  }

  @After
  public void after() throws IOException {
    FileUtils.deleteDirectory(tableLocation);
  }
}
