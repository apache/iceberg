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

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.commons.compress.utils.Lists;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.junit.Assert.assertEquals;

public class TestIcebergInputFormat {

  private static final Logger LOG = LoggerFactory.getLogger(TestIcebergInputFormat.class);

  private File tableLocation;
  private Table table;

  //TODO flesh out with more tests of the IF itself
  //TODO: do we still need the table data etc. if we're not testing from Hive?
  
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
