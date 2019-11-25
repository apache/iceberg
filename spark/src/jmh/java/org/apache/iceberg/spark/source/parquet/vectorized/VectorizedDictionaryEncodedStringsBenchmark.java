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

package org.apache.iceberg.spark.source.parquet.vectorized;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.pmod;
import static org.apache.spark.sql.functions.when;

public class VectorizedDictionaryEncodedStringsBenchmark extends VectorizedDictionaryEncodedBenchmark {
  @Override
  protected final Table initTable() {
    Schema schema = new Schema(
        optional(1, "longCol", Types.LongType.get()), optional(2, "stringCol", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
    HadoopTables tables = new HadoopTables(hadoopConf());
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.METADATA_COMPRESSION, "gzip");
    return tables.create(schema, partitionSpec, properties, newTableLocation());
  }

  @Override
  protected void appendData() {
    for (int fileNum = 1; fileNum <= NUM_FILES; fileNum++) {
      Dataset<Row> df = spark().range(NUM_ROWS)
          .withColumn(
              "longCol",
              when(pmod(col("id"), lit(9))
                  .equalTo(lit(0)), lit(0L))
                  //.when(expr("id > NUM_ROWS/2"), lit(UUID.randomUUID().toString()))
                  .when(pmod(col("id"), lit(9))
                      .equalTo(lit(1)), lit(1L))
                  .when(pmod(col("id"), lit(9))
                      .equalTo(lit(2)), lit(2L))
                  .when(pmod(col("id"), lit(9))
                      .equalTo(lit(3)), lit(3L))
                  .when(pmod(col("id"), lit(9))
                      .equalTo(lit(4)), lit(4L))
                  .when(pmod(col("id"), lit(9))
                      .equalTo(lit(5)), lit(5L))
                  .when(pmod(col("id"), lit(9))
                      .equalTo(lit(6)), lit(6L))
                  .when(pmod(col("id"), lit(9))
                      .equalTo(lit(7)), lit(7L))
                  .when(pmod(col("id"), lit(9))
                      .equalTo(lit(8)), lit(8L))
                  .otherwise(lit(2L)))
          .drop("id")
          .withColumn(
              "stringCol",
              when(col("longCol")
                  .equalTo(lit(1L)), lit("1"))
                  .when(col("longCol")
                      .equalTo(lit(2L)), lit("2"))
                  .when(col("longCol")
                      .equalTo(lit(3L)), lit("3"))
                  .when(col("longCol")
                      .equalTo(lit(4L)), lit("4"))
                  .when(col("longCol")
                      .equalTo(lit(5L)), lit("5"))
                  .when(col("longCol")
                      .equalTo(lit(6L)), lit("6"))
                  .when(col("longCol")
                      .equalTo(lit(7L)), lit("7"))
                  .when(col("longCol")
                      .equalTo(lit(8L)), lit("8"))
                  .when(col("longCol")
                      .equalTo(lit(9L)), lit("9")));
      appendAsFile(df);
    }
  }
}
