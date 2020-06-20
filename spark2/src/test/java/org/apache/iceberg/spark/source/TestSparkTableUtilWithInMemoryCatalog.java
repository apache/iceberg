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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.SparkTableUtil.SparkPartition;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.collection.Seq;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestSparkTableUtilWithInMemoryCatalog {

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get())
  );
  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
      .identity("data")
      .build();

  private static SparkSession spark;

  @BeforeClass
  public static void startSpark() {
    TestSparkTableUtilWithInMemoryCatalog.spark = SparkSession.builder()
        .master("local[2]")
        .getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestSparkTableUtilWithInMemoryCatalog.spark;
    TestSparkTableUtilWithInMemoryCatalog.spark = null;
    currentSpark.stop();
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private String tableLocation = null;

  @Before
  public void setupTableLocation() throws Exception {
    File tableDir = temp.newFolder();
    this.tableLocation = tableDir.toURI().toString();
  }

  @Test
  public void testImportUnpartitionedTable() throws IOException {
    Map<String, String> props = Maps.newHashMap();
    props.put(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none");
    props.put(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "data", "full");
    Table table = TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), props, tableLocation);

    List<SimpleRecord> records = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );

    File parquetTableDir = temp.newFolder("parquet_table");
    String parquetTableLocation = parquetTableDir.toURI().toString();

    try {
      Dataset<Row> inputDF = spark.createDataFrame(records, SimpleRecord.class).coalesce(1);
      inputDF.select("id", "data").write()
          .format("parquet")
          .mode("append")
          .option("path", parquetTableLocation)
          .saveAsTable("parquet_table");

      File stagingDir = temp.newFolder("staging-dir");
      SparkTableUtil.importSparkTable(spark, new TableIdentifier("parquet_table"), table, stagingDir.toString());

      List<SimpleRecord> actualRecords = spark.read()
          .format("iceberg")
          .load(tableLocation)
          .orderBy("id")
          .as(Encoders.bean(SimpleRecord.class))
          .collectAsList();

      Assert.assertEquals("Result rows should match", records, actualRecords);

      Dataset<Row> fileDF = spark.read().format("iceberg").load(tableLocation + "#files");
      Types.NestedField idField = table.schema().findField("id");
      checkFieldMetrics(fileDF, idField, true);
      Types.NestedField dataField = table.schema().findField("data");
      checkFieldMetrics(fileDF, dataField, false);
    } finally {
      spark.sql("DROP TABLE parquet_table");
    }
  }

  @Test
  public void testImportPartitionedTable() throws IOException {
    Map<String, String> props = Maps.newHashMap();
    props.put(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none");
    props.put(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "data", "full");
    Table table = TABLES.create(SCHEMA, SPEC, props, tableLocation);

    List<SimpleRecord> records = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );

    File parquetTableDir = temp.newFolder("parquet_table");
    String parquetTableLocation = parquetTableDir.toURI().toString();

    try {
      Dataset<Row> inputDF = spark.createDataFrame(records, SimpleRecord.class);
      inputDF.select("id", "data").write()
          .format("parquet")
          .mode("append")
          .option("path", parquetTableLocation)
          .partitionBy("data")
          .saveAsTable("parquet_table");

      Assert.assertEquals(
          "Should have 3 partitions",
          3, SparkTableUtil.getPartitions(spark, "parquet_table").size());

      Assert.assertEquals(
          "Should have 1 partition where data = 'a'",
          1, SparkTableUtil.getPartitionsByFilter(spark, "parquet_table", "data = 'a'").size());

      File stagingDir = temp.newFolder("staging-dir");
      SparkTableUtil.importSparkTable(spark, new TableIdentifier("parquet_table"), table, stagingDir.toString());

      List<SimpleRecord> actualRecords = spark.read()
          .format("iceberg")
          .load(tableLocation)
          .orderBy("id")
          .as(Encoders.bean(SimpleRecord.class))
          .collectAsList();

      Assert.assertEquals("Result rows should match", records, actualRecords);

      Dataset<Row> fileDF = spark.read().format("iceberg").load(tableLocation + "#files");
      Types.NestedField idField = table.schema().findField("id");
      checkFieldMetrics(fileDF, idField, true);
      // 'data' is a partition column and is not physically present in files written by Spark
      Types.NestedField dataField = table.schema().findField("data");
      checkFieldMetrics(fileDF, dataField, true);
    } finally {
      spark.sql("DROP TABLE parquet_table");
    }
  }

  @Test
  public void testImportPartitions() throws IOException {
    Table table = TABLES.create(SCHEMA, SPEC, tableLocation);

    List<SimpleRecord> records = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );

    File parquetTableDir = temp.newFolder("parquet_table");
    String parquetTableLocation = parquetTableDir.toURI().toString();

    try {
      Dataset<Row> inputDF = spark.createDataFrame(records, SimpleRecord.class);
      inputDF.select("id", "data").write()
          .format("parquet")
          .mode("append")
          .option("path", parquetTableLocation)
          .partitionBy("data")
          .saveAsTable("parquet_table");

      File stagingDir = temp.newFolder("staging-dir");
      Seq<SparkPartition> partitions = SparkTableUtil.getPartitionsByFilter(spark, "parquet_table", "data = 'a'");
      SparkTableUtil.importSparkPartitions(spark, partitions, table, table.spec(), stagingDir.toString());

      List<SimpleRecord> expectedRecords = Lists.newArrayList(new SimpleRecord(1, "a"));

      List<SimpleRecord> actualRecords = spark.read()
          .format("iceberg")
          .load(tableLocation)
          .orderBy("id")
          .as(Encoders.bean(SimpleRecord.class))
          .collectAsList();

      Assert.assertEquals("Result rows should match", expectedRecords, actualRecords);
    } finally {
      spark.sql("DROP TABLE parquet_table");
    }
  }

  @Test
  public void testImportPartitionsWithSnapshotInheritance() throws IOException {
    Table table = TABLES.create(SCHEMA, SPEC, tableLocation);

    table.updateProperties()
        .set(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, "true")
        .commit();

    List<SimpleRecord> records = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );

    File parquetTableDir = temp.newFolder("parquet_table");
    String parquetTableLocation = parquetTableDir.toURI().toString();

    try {
      Dataset<Row> inputDF = spark.createDataFrame(records, SimpleRecord.class);
      inputDF.select("id", "data").write()
          .format("parquet")
          .mode("append")
          .option("path", parquetTableLocation)
          .partitionBy("data")
          .saveAsTable("parquet_table");

      File stagingDir = temp.newFolder("staging-dir");
      Seq<SparkPartition> partitions = SparkTableUtil.getPartitionsByFilter(spark, "parquet_table", "data = 'a'");
      SparkTableUtil.importSparkPartitions(spark, partitions, table, table.spec(), stagingDir.toString());

      List<SimpleRecord> expectedRecords = Lists.newArrayList(new SimpleRecord(1, "a"));

      List<SimpleRecord> actualRecords = spark.read()
          .format("iceberg")
          .load(tableLocation)
          .orderBy("id")
          .as(Encoders.bean(SimpleRecord.class))
          .collectAsList();

      Assert.assertEquals("Result rows should match", expectedRecords, actualRecords);
    } finally {
      spark.sql("DROP TABLE parquet_table");
    }
  }

  private void checkFieldMetrics(Dataset<Row> fileDF, Types.NestedField field, boolean isNull) {
    List<Row> metricRows = fileDF
        .selectExpr(
            String.format("lower_bounds['%d']", field.fieldId()),
            String.format("upper_bounds['%d']", field.fieldId())
        )
        .collectAsList();

    metricRows.forEach(row -> {
      Assert.assertEquals("Invalid metrics for column: " + field.name(), isNull, row.isNullAt(0));
      Assert.assertEquals("Invalid metrics for column: " + field.name(), isNull, row.isNullAt(1));
    });
  }
}
