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

package org.apache.iceberg.spark.source;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestDataSourceOptions {

  private static final Configuration CONF = new Configuration();
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get())
  );
  private static SparkSession spark = null;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @BeforeClass
  public static void startSpark() {
    TestDataSourceOptions.spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestDataSourceOptions.spark;
    TestDataSourceOptions.spark = null;
    currentSpark.stop();
  }

  @Test
  public void testWriteFormatOptionOverridesTableProperties() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.DEFAULT_FILE_FORMAT, "avro");
    Table table = tables.create(SCHEMA, spec, options, tableLocation);

    List<SimpleRecord> expectedRecords = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );
    Dataset<Row> df = spark.createDataFrame(expectedRecords, SimpleRecord.class);
    df.select("id", "data").write()
        .format("iceberg")
        .option("write-format", "parquet")
        .mode("append")
        .save(tableLocation);

    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      tasks.forEach(task -> {
        FileFormat fileFormat = FileFormat.fromFileName(task.file().path());
        Assert.assertEquals(FileFormat.PARQUET, fileFormat);
      });
    }
  }

  @Test
  public void testNoWriteFormatOption() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.DEFAULT_FILE_FORMAT, "avro");
    Table table = tables.create(SCHEMA, spec, options, tableLocation);

    List<SimpleRecord> expectedRecords = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );
    Dataset<Row> df = spark.createDataFrame(expectedRecords, SimpleRecord.class);
    df.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      tasks.forEach(task -> {
        FileFormat fileFormat = FileFormat.fromFileName(task.file().path());
        Assert.assertEquals(FileFormat.AVRO, fileFormat);
      });
    }
  }

  @Test
  public void testHadoopOptions() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();
    Configuration sparkHadoopConf = spark.sparkContext().hadoopConfiguration();
    String originalDefaultFS = sparkHadoopConf.get("fs.default.name");

    try {
      HadoopTables tables = new HadoopTables(CONF);
      PartitionSpec spec = PartitionSpec.unpartitioned();
      Map<String, String> options = Maps.newHashMap();
      tables.create(SCHEMA, spec, options, tableLocation);

      // set an invalid value for 'fs.default.name' in Spark Hadoop config
      // to verify that 'hadoop.' data source options are propagated correctly
      sparkHadoopConf.set("fs.default.name", "hdfs://localhost:9000");

      List<SimpleRecord> expectedRecords = Lists.newArrayList(
          new SimpleRecord(1, "a"),
          new SimpleRecord(2, "b")
      );
      Dataset<Row> originalDf = spark.createDataFrame(expectedRecords, SimpleRecord.class);
      originalDf.select("id", "data").write()
          .format("iceberg")
          .mode("append")
          .option("hadoop.fs.default.name", "file:///")
          .save(tableLocation);

      Dataset<Row> resultDf = spark.read()
          .format("iceberg")
          .option("hadoop.fs.default.name", "file:///")
          .load(tableLocation);
      List<SimpleRecord> resultRecords = resultDf.orderBy("id")
          .as(Encoders.bean(SimpleRecord.class))
          .collectAsList();

      Assert.assertEquals("Records should match", expectedRecords, resultRecords);
    } finally {
      sparkHadoopConf.set("fs.default.name", originalDefaultFS);
    }
  }

  @Test
  public void testSplitOptionsOverridesTableProperties() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.SPLIT_SIZE, String.valueOf(128L * 1024 * 1024)); // 128Mb
    tables.create(SCHEMA, spec, options, tableLocation);

    List<SimpleRecord> expectedRecords = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b")
    );
    Dataset<Row> originalDf = spark.createDataFrame(expectedRecords, SimpleRecord.class);
    originalDf.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    Dataset<Row> resultDf = spark.read()
        .format("iceberg")
        .option("split-size", String.valueOf(562L)) // 562 bytes is the size of SimpleRecord(1,"a")
        .load(tableLocation);

    Assert.assertEquals("Spark partitions should match", 2, resultDf.javaRDD().getNumPartitions());
  }
}
