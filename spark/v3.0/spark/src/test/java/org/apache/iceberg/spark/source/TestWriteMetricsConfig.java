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

import static org.apache.iceberg.spark.SparkSchemaUtil.convert;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestWriteMetricsConfig {

  private static final Configuration CONF = new Configuration();
  private static final Schema SIMPLE_SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));
  private static final Schema COMPLEX_SCHEMA =
      new Schema(
          required(1, "longCol", Types.IntegerType.get()),
          optional(2, "strCol", Types.StringType.get()),
          required(
              3,
              "record",
              Types.StructType.of(
                  required(4, "id", Types.IntegerType.get()),
                  required(5, "data", Types.StringType.get()))));

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private static SparkSession spark = null;
  private static JavaSparkContext sc = null;

  @BeforeClass
  public static void startSpark() {
    TestWriteMetricsConfig.spark = SparkSession.builder().master("local[2]").getOrCreate();
    TestWriteMetricsConfig.sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestWriteMetricsConfig.spark;
    TestWriteMetricsConfig.spark = null;
    TestWriteMetricsConfig.sc = null;
    currentSpark.stop();
  }

  @Test
  public void testFullMetricsCollectionForParquet() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.DEFAULT_WRITE_METRICS_MODE, "full");
    Table table = tables.create(SIMPLE_SCHEMA, spec, properties, tableLocation);

    List<SimpleRecord> expectedRecords =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));
    Dataset<Row> df = spark.createDataFrame(expectedRecords, SimpleRecord.class);
    df.select("id", "data")
        .coalesce(1)
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, "parquet")
        .mode(SaveMode.Append)
        .save(tableLocation);

    for (FileScanTask task : table.newScan().includeColumnStats().planFiles()) {
      DataFile file = task.file();
      Assert.assertEquals(2, file.nullValueCounts().size());
      Assert.assertEquals(2, file.valueCounts().size());
      Assert.assertEquals(2, file.lowerBounds().size());
      Assert.assertEquals(2, file.upperBounds().size());
    }
  }

  @Test
  public void testCountMetricsCollectionForParquet() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.DEFAULT_WRITE_METRICS_MODE, "counts");
    Table table = tables.create(SIMPLE_SCHEMA, spec, properties, tableLocation);

    List<SimpleRecord> expectedRecords =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));
    Dataset<Row> df = spark.createDataFrame(expectedRecords, SimpleRecord.class);
    df.select("id", "data")
        .coalesce(1)
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, "parquet")
        .mode(SaveMode.Append)
        .save(tableLocation);

    for (FileScanTask task : table.newScan().includeColumnStats().planFiles()) {
      DataFile file = task.file();
      Assert.assertEquals(2, file.nullValueCounts().size());
      Assert.assertEquals(2, file.valueCounts().size());
      Assert.assertTrue(file.lowerBounds().isEmpty());
      Assert.assertTrue(file.upperBounds().isEmpty());
    }
  }

  @Test
  public void testNoMetricsCollectionForParquet() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none");
    Table table = tables.create(SIMPLE_SCHEMA, spec, properties, tableLocation);

    List<SimpleRecord> expectedRecords =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));
    Dataset<Row> df = spark.createDataFrame(expectedRecords, SimpleRecord.class);
    df.select("id", "data")
        .coalesce(1)
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, "parquet")
        .mode(SaveMode.Append)
        .save(tableLocation);

    for (FileScanTask task : table.newScan().includeColumnStats().planFiles()) {
      DataFile file = task.file();
      Assert.assertTrue(file.nullValueCounts().isEmpty());
      Assert.assertTrue(file.valueCounts().isEmpty());
      Assert.assertTrue(file.lowerBounds().isEmpty());
      Assert.assertTrue(file.upperBounds().isEmpty());
    }
  }

  @Test
  public void testCustomMetricCollectionForParquet() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.DEFAULT_WRITE_METRICS_MODE, "counts");
    properties.put("write.metadata.metrics.column.id", "full");
    Table table = tables.create(SIMPLE_SCHEMA, spec, properties, tableLocation);

    List<SimpleRecord> expectedRecords =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));
    Dataset<Row> df = spark.createDataFrame(expectedRecords, SimpleRecord.class);
    df.select("id", "data")
        .coalesce(1)
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, "parquet")
        .mode(SaveMode.Append)
        .save(tableLocation);

    Schema schema = table.schema();
    Types.NestedField id = schema.findField("id");
    for (FileScanTask task : table.newScan().includeColumnStats().planFiles()) {
      DataFile file = task.file();
      Assert.assertEquals(2, file.nullValueCounts().size());
      Assert.assertEquals(2, file.valueCounts().size());
      Assert.assertEquals(1, file.lowerBounds().size());
      Assert.assertTrue(file.lowerBounds().containsKey(id.fieldId()));
      Assert.assertEquals(1, file.upperBounds().size());
      Assert.assertTrue(file.upperBounds().containsKey(id.fieldId()));
    }
  }

  @Test
  public void testBadCustomMetricCollectionForParquet() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.DEFAULT_WRITE_METRICS_MODE, "counts");
    properties.put("write.metadata.metrics.column.ids", "full");

    AssertHelpers.assertThrows(
        "Creating a table with invalid metrics should fail",
        ValidationException.class,
        null,
        () -> tables.create(SIMPLE_SCHEMA, spec, properties, tableLocation));
  }

  @Test
  public void testCustomMetricCollectionForNestedParquet() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(COMPLEX_SCHEMA).identity("strCol").build();
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none");
    properties.put("write.metadata.metrics.column.longCol", "counts");
    properties.put("write.metadata.metrics.column.record.id", "full");
    properties.put("write.metadata.metrics.column.record.data", "truncate(2)");
    Table table = tables.create(COMPLEX_SCHEMA, spec, properties, tableLocation);

    Iterable<InternalRow> rows = RandomData.generateSpark(COMPLEX_SCHEMA, 10, 0);
    JavaRDD<InternalRow> rdd = sc.parallelize(Lists.newArrayList(rows));
    Dataset<Row> df =
        spark.internalCreateDataFrame(JavaRDD.toRDD(rdd), convert(COMPLEX_SCHEMA), false);

    df.coalesce(1)
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, "parquet")
        .mode(SaveMode.Append)
        .save(tableLocation);

    Schema schema = table.schema();
    Types.NestedField longCol = schema.findField("longCol");
    Types.NestedField recordId = schema.findField("record.id");
    Types.NestedField recordData = schema.findField("record.data");
    for (FileScanTask task : table.newScan().includeColumnStats().planFiles()) {
      DataFile file = task.file();

      Map<Integer, Long> nullValueCounts = file.nullValueCounts();
      Assert.assertEquals(3, nullValueCounts.size());
      Assert.assertTrue(nullValueCounts.containsKey(longCol.fieldId()));
      Assert.assertTrue(nullValueCounts.containsKey(recordId.fieldId()));
      Assert.assertTrue(nullValueCounts.containsKey(recordData.fieldId()));

      Map<Integer, Long> valueCounts = file.valueCounts();
      Assert.assertEquals(3, valueCounts.size());
      Assert.assertTrue(valueCounts.containsKey(longCol.fieldId()));
      Assert.assertTrue(valueCounts.containsKey(recordId.fieldId()));
      Assert.assertTrue(valueCounts.containsKey(recordData.fieldId()));

      Map<Integer, ByteBuffer> lowerBounds = file.lowerBounds();
      Assert.assertEquals(2, lowerBounds.size());
      Assert.assertTrue(lowerBounds.containsKey(recordId.fieldId()));
      ByteBuffer recordDataLowerBound = lowerBounds.get(recordData.fieldId());
      Assert.assertEquals(2, ByteBuffers.toByteArray(recordDataLowerBound).length);

      Map<Integer, ByteBuffer> upperBounds = file.upperBounds();
      Assert.assertEquals(2, upperBounds.size());
      Assert.assertTrue(upperBounds.containsKey(recordId.fieldId()));
      ByteBuffer recordDataUpperBound = upperBounds.get(recordData.fieldId());
      Assert.assertEquals(2, ByteBuffers.toByteArray(recordDataUpperBound).length);
    }
  }
}
