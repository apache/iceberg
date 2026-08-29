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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.util.Collections;
import org.apache.iceberg.Schema;
import org.apache.iceberg.actions.SizeBasedFileRewritePlanner;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * A benchmark that evaluates the performance of the rewrite data files action in Spark.
 *
 * <p>To run this benchmark for spark-4.1: <code>
 *   ./gradlew :iceberg-spark:iceberg-spark-4.1_2.13:jmh
 *       -PjmhIncludeRegex=IcebergDataCompactionBenchmark.rewriteDataFiles
 *       -PjmhOutputPath=benchmark/data-compaction-benchmark-results.txt
 *       -PjmhJsonOutputPath=benchmark/data-compaction-benchmark-results.json
 * </code>
 */
@Warmup(iterations = 3)
@Measurement(iterations = 10)
public class IcebergDataCompactionBenchmark extends IcebergCompactionBenchmark {

  private static final String[] NAMESPACE = new String[] {"default"};
  private static final String NAME = "compactbench";
  private static final Identifier IDENT = Identifier.of(NAMESPACE, NAME);
  private static final long TOTAL_ROWS = 2_000_000L;

  @Param({"250", "500", "1000", "2000"})
  private int numFiles;

  @Override
  protected String tableName() {
    return NAME;
  }

  @Benchmark
  @Threads(1)
  public void rewriteDataFiles() {
    SparkActions.get()
        .rewriteDataFiles(table())
        .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
        .execute();
  }

  @Override
  protected final void initTable() {
    Schema schema =
        new Schema(
            required(1, "intCol", Types.IntegerType.get()),
            required(2, "stringCol", Types.StringType.get()),
            optional(3, "nullCol", Types.StringType.get()));

    SparkSessionCatalog<?> catalog;
    try {
      catalog =
          (SparkSessionCatalog<?>)
              Spark3Util.catalogAndIdentifier(spark(), "spark_catalog").catalog();
      catalog.dropTable(IDENT);
      catalog.createTable(
          IDENT, SparkSchemaUtil.convert(schema), new Transform[0], Collections.emptyMap());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void appendData() {
    Dataset<Row> df =
        spark()
            .range(0, TOTAL_ROWS)
            .withColumn("intCol", col("id").cast("int"))
            .withColumn("stringCol", concat(lit("foo_"), col("id")))
            .withColumn("nullCol", lit(null).cast("string"))
            .drop("id")
            .repartition(numFiles);
    writeData(df);
  }
}
