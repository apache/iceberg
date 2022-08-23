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
package org.apache.iceberg.spark.action;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.io.Files;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.spark.sql.functions.lit;

@Fork(1)
@State(Scope.Benchmark)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.SingleShotTime)
@Timeout(time = 1000, timeUnit = TimeUnit.HOURS)
public class DeleteOrphanFilesBenchmark {

  private static final String NAME = "delete_orphan_perf";

  private final Configuration hadoopConf = initHadoopConf();
  private SparkSession spark;
  private final List<String> paths = Lists.newArrayList();

  @Setup
  public void setupBench() {
    setupSpark();
  }

  @TearDown
  public void teardownBench() {
    tearDownSpark();
  }

  @Setup(Level.Iteration)
  public void setupIteration() throws NoSuchTableException, ParseException {
    initTable();
    appendData();
  }

  @TearDown(Level.Iteration)
  public void cleanUpIteration() {
    cleanupFiles();
  }

  @Benchmark
  @Threads(1)
  public void testDeleteOrphanFiles() {
    Timestamp timestamp = new Timestamp(10000);
    Dataset<Row> rowDataset = spark.createDataset(paths, Encoders.STRING()).withColumnRenamed("value", "file_path")
        .withColumn("last_modified", lit(timestamp));

    SparkActions.get(spark).deleteOrphanFiles(table())
        .compareToFileList(rowDataset)
        .execute();
  }

  protected Configuration initHadoopConf() {
    return new Configuration();
  }

  protected final void initTable() {
    spark.sql(String.format("DROP TABLE IF EXISTS %s", NAME));
    spark.sql(
        String.format("CREATE TABLE %s(id INT, name STRING)" +
            " USING ICEBERG" +
            " TBLPROPERTIES ( 'format-version' = '2'," +
            " 'compatibility.snapshot-id-inheritance.enabled' = 'true')", NAME));
  }

  private void appendData() throws NoSuchTableException, ParseException {
    Schema schema =
        new Schema(
            required(3, "id", Types.IntegerType.get()), required(4, "data", Types.StringType.get()));

// Partition spec used to create tables
    PartitionSpec SPEC = PartitionSpec.builderFor(schema).build();

    for (int i = 0; i < 1000; i++) {
      AppendFiles appendFiles = table().newFastAppend();
      for (int j = 0; j < 10000; j++) {
        String path = String.format("$location/path/to/data-%d-%d.parquet", i, j);
        DataFile dataFile = DataFiles.builder(SPEC)
            .withPath(path)
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
        appendFiles.appendFile(dataFile);
      }
      appendFiles.commit();
    }
  }

  protected final Table table() {
    try {
      return Spark3Util.loadIcebergTable(spark(), NAME);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected final SparkSession spark() {
    return spark;
  }

  protected String getCatalogWarehouse() {
    return Files.createTempDir().getAbsolutePath() + "/" + UUID.randomUUID() + "/";
  }

  protected void cleanupFiles() {
    spark.sql("DROP TABLE IF EXISTS " + NAME);
  }

  protected void setupSpark() {
    SparkSession.Builder builder =
        SparkSession.builder()
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hadoop")
            .config("spark.sql.catalog.spark_catalog.warehouse", getCatalogWarehouse())
            .master("local[*]");
    spark = builder.getOrCreate();
    Configuration sparkHadoopConf = spark.sessionState().newHadoopConf();
    hadoopConf.forEach(entry -> sparkHadoopConf.set(entry.getKey(), entry.getValue()));
  }

  protected void tearDownSpark() {
    spark.stop();
  }
}
