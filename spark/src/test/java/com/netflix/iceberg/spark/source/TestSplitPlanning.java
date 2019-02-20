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

package com.netflix.iceberg.spark.source;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.netflix.iceberg.AppendFiles;
import com.netflix.iceberg.DataFile;
import com.netflix.iceberg.DataFiles;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.Table;
import com.netflix.iceberg.TableProperties;
import com.netflix.iceberg.hadoop.HadoopTables;
import com.netflix.iceberg.types.Types;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.netflix.iceberg.types.Types.NestedField.optional;

public class TestSplitPlanning {

  private static final Configuration CONF = new Configuration();
  private static final HadoopTables TABLES = new HadoopTables(CONF);
  private static final Schema SCHEMA = new Schema(
    optional(1, "id", Types.IntegerType.get()),
    optional(2, "data", Types.StringType.get())
  );

  private static SparkSession spark = null;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private Table table = null;
  private String tableLocation = null;

  @BeforeClass
  public static void startSpark() {
    TestSplitPlanning.spark = SparkSession.builder().master("local").getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession spark = TestSplitPlanning.spark;
    TestSplitPlanning.spark = null;
    spark.stop();
  }

  @Before
  public void setupTable() throws IOException {
    File tableDir = temp.newFolder();
    this.tableLocation = tableDir.toURI().toString();
    this.table = TABLES.create(SCHEMA, tableLocation);
  }

  @Test
  public void testBasicSplitPlanning() {
    List<DataFile> files128MB = newFiles(4, 128 * 1024 * 1024);
    appendFiles(files128MB);
    Dataset<Row> df1 = spark.read().format("iceberg").load(tableLocation);
    Assert.assertEquals(4, df1.toJavaRDD().getNumPartitions());

    List<DataFile> files32MB = newFiles(16, 32 * 1024 * 1024);
    appendFiles(files32MB);
    Dataset<Row> df2 = spark.read().format("iceberg").load(tableLocation);
    Assert.assertEquals(8, df2.toJavaRDD().getNumPartitions());
  }

  @Test
  public void testSplitPlanningWithSmallFiles() {
    table.updateProperties().set(TableProperties.SPLIT_LOOKBACK, "30").commit();
    List<DataFile> files60MB = newFiles(50, 60 * 1024 * 1024);
    List<DataFile> files5KB = newFiles(370, 5 * 1024);
    Iterable<DataFile> files = Iterables.concat(files60MB, files5KB);
    appendFiles(files);
    Dataset<Row> df = spark.read().format("iceberg").load(tableLocation);
    Assert.assertEquals(35, df.toJavaRDD().getNumPartitions());
  }

  @Test
  public void testSplitPlanningWithNoMinWeight() {
    table.updateProperties()
      .set(TableProperties.SPLIT_LOOKBACK, "30")
      .set(TableProperties.SPLIT_MIN_FILE_WEIGHT, "0")
      .commit();
    List<DataFile> files60MB = newFiles(2, 60 * 1024 * 1024);
    List<DataFile> files5KB = newFiles(100, 5 * 1024);
    Iterable<DataFile> files = Iterables.concat(files60MB, files5KB);
    appendFiles(files);
    Dataset<Row> df = spark.read().format("iceberg").load(tableLocation);
    Assert.assertEquals(1, df.toJavaRDD().getNumPartitions());
  }

  private void appendFiles(Iterable<DataFile> files) {
    AppendFiles appendFiles = table.newAppend();
    files.forEach(appendFiles::appendFile);
    appendFiles.commit();
  }

  private List<DataFile> newFiles(int numFiles, long sizeInBytes) {
    List<DataFile> files = Lists.newArrayList();
    for (int fileNum = 0; fileNum < numFiles; fileNum++) {
      files.add(newFile(sizeInBytes));
    }
    return files;
  }

  private DataFile newFile(long sizeInBytes) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
      .withPath("/path/to/data-a.parquet")
      .withFileSizeInBytes(sizeInBytes)
      .withRecordCount(2)
      .build();
  }
}
