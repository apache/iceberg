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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_add;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.pmod;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.to_timestamp;

import java.util.Map;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.openjdk.jmh.annotations.Setup;

/**
 * Benchmark to compare performance of reading Parquet dictionary encoded data with a flat schema
 * using vectorized Iceberg read path and the built-in file source in Spark.
 *
 * <p>To run this benchmark for spark-3.3: <code>
 *   ./gradlew -DsparkVersions=3.3 :iceberg-spark:iceberg-spark-3.3_2.12:jmh \
 *       -PjmhIncludeRegex=VectorizedReadDictionaryEncodedFlatParquetDataBenchmark \
 *       -PjmhOutputPath=benchmark/results.txt
 * </code>
 */
public class VectorizedReadDictionaryEncodedFlatParquetDataBenchmark
    extends VectorizedReadFlatParquetDataBenchmark {

  @Setup
  @Override
  public void setupBenchmark() {
    setupSpark(true);
    appendData();
  }

  @Override
  Map<String, String> parquetWriteProps() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.METADATA_COMPRESSION, "gzip");
    return properties;
  }

  @Override
  void appendData() {
    Dataset<Row> df = idDF();
    df = withLongColumnDictEncoded(df);
    df = withIntColumnDictEncoded(df);
    df = withFloatColumnDictEncoded(df);
    df = withDoubleColumnDictEncoded(df);
    df = withBigDecimalColumnNotDictEncoded(df); // no dictionary for fixed len binary in Parquet v1
    df = withDecimalColumnDictEncoded(df);
    df = withDateColumnDictEncoded(df);
    df = withTimestampColumnDictEncoded(df);
    df = withStringColumnDictEncoded(df);
    df = df.drop("id");
    df.write().format("iceberg").mode(SaveMode.Append).save(table().location());
  }

  private static Column modColumn() {
    return pmod(col("id"), lit(9));
  }

  private Dataset<Row> idDF() {
    return spark().range(0, NUM_ROWS_PER_FILE * NUM_FILES, 1, NUM_FILES).toDF();
  }

  private static Dataset<Row> withLongColumnDictEncoded(Dataset<Row> df) {
    return df.withColumn("longCol", modColumn().cast(DataTypes.LongType));
  }

  private static Dataset<Row> withIntColumnDictEncoded(Dataset<Row> df) {
    return df.withColumn("intCol", modColumn().cast(DataTypes.IntegerType));
  }

  private static Dataset<Row> withFloatColumnDictEncoded(Dataset<Row> df) {
    return df.withColumn("floatCol", modColumn().cast(DataTypes.FloatType));
  }

  private static Dataset<Row> withDoubleColumnDictEncoded(Dataset<Row> df) {
    return df.withColumn("doubleCol", modColumn().cast(DataTypes.DoubleType));
  }

  private static Dataset<Row> withBigDecimalColumnNotDictEncoded(Dataset<Row> df) {
    return df.withColumn("bigDecimalCol", modColumn().cast("decimal(20,5)"));
  }

  private static Dataset<Row> withDecimalColumnDictEncoded(Dataset<Row> df) {
    return df.withColumn("decimalCol", modColumn().cast("decimal(18,5)"));
  }

  private static Dataset<Row> withDateColumnDictEncoded(Dataset<Row> df) {
    Column days = modColumn().cast(DataTypes.ShortType);
    return df.withColumn("dateCol", date_add(to_date(lit("04/12/2019"), "MM/dd/yyyy"), days));
  }

  private static Dataset<Row> withTimestampColumnDictEncoded(Dataset<Row> df) {
    Column days = modColumn().cast(DataTypes.ShortType);
    return df.withColumn(
        "timestampCol", to_timestamp(date_add(to_date(lit("04/12/2019"), "MM/dd/yyyy"), days)));
  }

  private static Dataset<Row> withStringColumnDictEncoded(Dataset<Row> df) {
    return df.withColumn("stringCol", modColumn().cast(DataTypes.StringType));
  }
}
