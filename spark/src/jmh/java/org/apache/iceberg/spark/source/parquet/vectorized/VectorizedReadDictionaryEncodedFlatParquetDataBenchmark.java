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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.openjdk.jmh.annotations.Setup;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.pmod;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.to_timestamp;
import static org.apache.spark.sql.functions.when;

/**
 * Benchmark to compare performance of reading Parquet dictionary encoded data with a flat schema using vectorized
 * Iceberg read path and the built-in file source in Spark.
 * <p>
 * To run this benchmark for either spark-2 or spark-3:
 * <code>
 *   ./gradlew :iceberg-spark[2|3]:jmh
 *       -PjmhIncludeRegex=VectorizedReadDictionaryEncodedFlatParquetDataBenchmark
 *       -PjmhOutputPath=benchmark/results.txt
 * </code>
 */
public class VectorizedReadDictionaryEncodedFlatParquetDataBenchmark extends VectorizedReadFlatParquetDataBenchmark {

  @Setup
  @Override
  public void setupBenchmark() {
    setupSpark(true);
    appendData();
    // Allow unsafe memory access to avoid the costly check arrow does to check if index is within bounds
    System.setProperty("arrow.enable_unsafe_memory_access", "true");
    // Disable expensive null check for every get(index) call.
    // Iceberg manages nullability checks itself instead of relying on arrow.
    System.setProperty("arrow.enable_null_check_for_get", "false");
  }

  @Override
  Map<String, String> parquetWriteProps() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.METADATA_COMPRESSION, "gzip");
    return properties;
  }

  @Override
  void appendData() {
    for (int fileNum = 1; fileNum <= NUM_FILES; fileNum++) {
      Dataset<Row> df = withLongColumnDictEncoded();
      df = withIntColumnDictEncoded(df);
      df = withFloatColumnDictEncoded(df);
      df = withDoubleColumnDictEncoded(df);
      df = withDecimalColumnDictEncoded(df);
      df = withDateColumnDictEncoded(df);
      df = withTimestampColumnDictEncoded(df);
      df = withStringColumnDictEncoded(df);
      appendAsFile(df);
    }
  }

  private Dataset<Row> withLongColumnDictEncoded() {
    return spark().range(NUM_ROWS_PER_FILE)
        .withColumn(
            "longCol",
            when(pmod(col("id"), lit(9)).equalTo(lit(0)), lit(0L))
                .when(pmod(col("id"), lit(9)).equalTo(lit(1)), lit(1L))
                .when(pmod(col("id"), lit(9)).equalTo(lit(2)), lit(2L))
                .when(pmod(col("id"), lit(9)).equalTo(lit(3)), lit(3L))
                .when(pmod(col("id"), lit(9)).equalTo(lit(4)), lit(4L))
                .when(pmod(col("id"), lit(9)).equalTo(lit(5)), lit(5L))
                .when(pmod(col("id"), lit(9)).equalTo(lit(6)), lit(6L))
                .when(pmod(col("id"), lit(9)).equalTo(lit(7)), lit(7L))
                .when(pmod(col("id"), lit(9)).equalTo(lit(8)), lit(8L)))
        .drop("id");
  }

  private static Dataset<Row> withIntColumnDictEncoded(Dataset<Row> df) {
    return df.withColumn(
        "intCol",
        when(modColumn(9, 0), lit(0))
            .when(modColumn(9, 1), lit(1))
            .when(modColumn(9, 2), lit(2))
            .when(modColumn(9, 3), lit(3))
            .when(modColumn(9, 4), lit(4))
            .when(modColumn(9, 5), lit(5))
            .when(modColumn(9, 6), lit(6))
            .when(modColumn(9, 7), lit(7))
            .when(modColumn(9, 8), lit(8)));
  }

  private static Dataset<Row> withFloatColumnDictEncoded(Dataset<Row> df) {
    return df.withColumn(
        "floatCol",
        when(modColumn(9, 0), lit(0.0f))
            .when(modColumn(9, 1), lit(1.0f))
            .when(modColumn(9, 2), lit(2.0f))
            .when(modColumn(9, 3), lit(3.0f))
            .when(modColumn(9, 4), lit(4.0f))
            .when(modColumn(9, 5), lit(5.0f))
            .when(modColumn(9, 6), lit(6.0f))
            .when(modColumn(9, 7), lit(7.0f))
            .when(modColumn(9, 8), lit(8.0f)));
  }

  private static Dataset<Row> withDoubleColumnDictEncoded(Dataset<Row> df) {
    return df.withColumn(
        "doubleCol",
        when(modColumn(9, 0), lit(0.0d))
            .when(modColumn(9, 1), lit(1.0d))
            .when(modColumn(9, 2), lit(2.0d))
            .when(modColumn(9, 3), lit(3.0d))
            .when(modColumn(9, 4), lit(4.0d))
            .when(modColumn(9, 5), lit(5.0d))
            .when(modColumn(9, 6), lit(6.0d))
            .when(modColumn(9, 7), lit(7.0d))
            .when(modColumn(9, 8), lit(8.0d)));
  }

  private static Dataset<Row> withDecimalColumnDictEncoded(Dataset<Row> df) {
    Types.DecimalType type = Types.DecimalType.of(20, 5);
    return df.withColumn(
        "decimalCol",
        when(modColumn(9, 0), bigDecimal(type, 0))
            .when(modColumn(9, 1), bigDecimal(type, 1))
            .when(modColumn(9, 2), bigDecimal(type, 2))
            .when(modColumn(9, 3), bigDecimal(type, 3))
            .when(modColumn(9, 4), bigDecimal(type, 4))
            .when(modColumn(9, 5), bigDecimal(type, 5))
            .when(modColumn(9, 6), bigDecimal(type, 6))
            .when(modColumn(9, 7), bigDecimal(type, 7))
            .when(modColumn(9, 8), bigDecimal(type, 8)));
  }

  private static Dataset<Row> withDateColumnDictEncoded(Dataset<Row> df) {
    return df.withColumn(
        "dateCol",
        when(modColumn(9, 0), to_date(lit("04/12/2019"), "MM/dd/yyyy"))
            .when(modColumn(9, 1), to_date(lit("04/13/2019"), "MM/dd/yyyy"))
            .when(modColumn(9, 2), to_date(lit("04/14/2019"), "MM/dd/yyyy"))
            .when(modColumn(9, 3), to_date(lit("04/15/2019"), "MM/dd/yyyy"))
            .when(modColumn(9, 4), to_date(lit("04/16/2019"), "MM/dd/yyyy"))
            .when(modColumn(9, 5), to_date(lit("04/17/2019"), "MM/dd/yyyy"))
            .when(modColumn(9, 6), to_date(lit("04/18/2019"), "MM/dd/yyyy"))
            .when(modColumn(9, 7), to_date(lit("04/19/2019"), "MM/dd/yyyy"))
            .when(modColumn(9, 8), to_date(lit("04/20/2019"), "MM/dd/yyyy")));
  }

  private static Dataset<Row> withTimestampColumnDictEncoded(Dataset<Row> df) {
    return df.withColumn(
        "timestampCol",
        when(modColumn(9, 0), to_timestamp(lit("04/12/2019"), "MM/dd/yyyy"))
            .when(modColumn(9, 1), to_timestamp(lit("04/13/2019"), "MM/dd/yyyy"))
            .when(modColumn(9, 2), to_timestamp(lit("04/14/2019"), "MM/dd/yyyy"))
            .when(modColumn(9, 3), to_timestamp(lit("04/15/2019"), "MM/dd/yyyy"))
            .when(modColumn(9, 4), to_timestamp(lit("04/16/2019"), "MM/dd/yyyy"))
            .when(modColumn(9, 5), to_timestamp(lit("04/17/2019"), "MM/dd/yyyy"))
            .when(modColumn(9, 6), to_timestamp(lit("04/18/2019"), "MM/dd/yyyy"))
            .when(modColumn(9, 7), to_timestamp(lit("04/19/2019"), "MM/dd/yyyy"))
            .when(modColumn(9, 8), to_timestamp(lit("04/20/2019"), "MM/dd/yyyy")));
  }

  private static Dataset<Row> withStringColumnDictEncoded(Dataset<Row> df) {
    return df.withColumn(
        "stringCol",
        when(pmod(col("longCol"), lit(9)).equalTo(lit(0)), lit("0"))
            .when(modColumn(9, 1), lit("1"))
            .when(modColumn(9, 2), lit("2"))
            .when(modColumn(9, 3), lit("3"))
            .when(modColumn(9, 4), lit("4"))
            .when(modColumn(9, 5), lit("5"))
            .when(modColumn(9, 6), lit("6"))
            .when(modColumn(9, 7), lit("7"))
            .when(modColumn(9, 8), lit("8")));
  }

  private static Column modColumn(int divisor, int remainder) {
    return pmod(col("longCol"), lit(divisor)).equalTo(lit(remainder));
  }

  private static BigDecimal bigDecimal(Types.DecimalType type, int value) {
    BigInteger unscaled = new BigInteger(String.valueOf(value + 1));
    return new BigDecimal(unscaled, type.scale());
  }
}
