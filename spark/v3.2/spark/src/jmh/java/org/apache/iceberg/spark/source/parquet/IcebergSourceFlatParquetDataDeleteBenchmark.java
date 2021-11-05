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

package org.apache.iceberg.spark.source.parquet;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.source.IcebergSourceBenchmark;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.internal.SQLConf;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

import static org.apache.iceberg.TableProperties.SPLIT_OPEN_FILE_COST;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.spark.sql.functions.current_date;
import static org.apache.spark.sql.functions.date_add;
import static org.apache.spark.sql.functions.expr;

/**
 * A benchmark that evaluates the non-vectorized read and vectorized read with pos-delete in the Spark data source for
 * Iceberg. 5% of rows are deleted in each data file.
 * <p>
 * This class uses a dataset with a flat schema.
 * To run this benchmark for spark-3:
 * <code>
 *   ./gradlew :iceberg-spark:iceberg-spark3:jmh
 *       -PjmhIncludeRegex=IcebergSourceFlatParquetDataDeleteBenchmark
 *       -PjmhOutputPath=benchmark/iceberg-source-flat-parquet-data-delete-benchmark-result.txt
 * </code>
 */
public class IcebergSourceFlatParquetDataDeleteBenchmark extends IcebergSourceBenchmark {

  private static final int NUM_FILES = 50;
  private static final int NUM_ROWS = 100 * 1000;
  private static final double PERCENTAGE_DELETE_ROW = 0.05;

  @Setup
  public void setupBenchmark() throws IOException {
    setupSpark();
    appendData();
  }

  @TearDown
  public void tearDownBenchmark() throws IOException {
    tearDownSpark();
    cleanupFiles();
  }

  @Benchmark
  @Threads(1)
  public void readIceberg() {
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.put(SPLIT_OPEN_FILE_COST, Integer.toString(128 * 1024 * 1024));
    withTableProperties(tableProperties, () -> {
      String tableLocation = table().location();
      Dataset<Row> df = spark().read().format("iceberg").load(tableLocation);
      materialize(df);
    });
  }

  @Benchmark
  @Threads(1)
  public void readIcebergVectorized() {
    Map<String, String> conf = Maps.newHashMap();
    conf.put(SQLConf.PARQUET_VECTORIZED_READER_ENABLED().key(), "true");
    conf.put(SQLConf.FILES_OPEN_COST_IN_BYTES().key(), Integer.toString(128 * 1024 * 1024));
    withSQLConf(conf, () -> {
      String tableLocation = table().location();
      Dataset<Row> df = spark().read().format("iceberg").load(tableLocation);
      materialize(df);
    });
  }

  private void appendData() throws IOException {
    for (int fileNum = 1; fileNum <= NUM_FILES; fileNum++) {
      Dataset<Row> df = spark().range(NUM_ROWS)
          .withColumnRenamed("id", "longCol")
          .withColumn("intCol", expr("CAST(longCol AS INT)"))
          .withColumn("floatCol", expr("CAST(longCol AS FLOAT)"))
          .withColumn("doubleCol", expr("CAST(longCol AS DOUBLE)"))
          .withColumn("decimalCol", expr("CAST(longCol AS DECIMAL(20, 5))"))
          .withColumn("dateCol", date_add(current_date(), fileNum))
          .withColumn("timestampCol", expr("TO_TIMESTAMP(dateCol)"))
          .withColumn("stringCol", expr("CAST(dateCol AS STRING)"));
      appendAsFile(df);

      // add pos-deletes
      table().refresh();
      for (DataFile file : table().currentSnapshot().addedFiles()) {
        writePosDeletes(file.path(), NUM_ROWS, PERCENTAGE_DELETE_ROW);
      }
    }
  }

  @Override
  protected final Table initTable() {
    Schema schema = new Schema(
        required(1, "longCol", Types.LongType.get()),
        required(2, "intCol", Types.IntegerType.get()),
        required(3, "floatCol", Types.FloatType.get()),
        optional(4, "doubleCol", Types.DoubleType.get()),
        optional(5, "decimalCol", Types.DecimalType.of(20, 5)),
        optional(6, "dateCol", Types.DateType.get()),
        optional(7, "timestampCol", Types.TimestampType.withZone()),
        optional(8, "stringCol", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
    HadoopTables tables = new HadoopTables(hadoopConf());
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.METADATA_COMPRESSION, "gzip");
    properties.put(TableProperties.FORMAT_VERSION, "2");
    return tables.create(schema, partitionSpec, properties, newTableLocation());
  }

  @Override
  protected Configuration initHadoopConf() {
    return new Configuration();
  }

  @Override
  protected FileFormat fileFormat() {
    return FileFormat.PARQUET;
  }
}
