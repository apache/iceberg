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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import org.apache.iceberg.spark.IcebergSpark;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.VarcharType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestIcebergSpark {

  private static SparkSession spark = null;

  @BeforeAll
  public static void startSpark() {
    TestIcebergSpark.spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterAll
  public static void stopSpark() {
    SparkSession currentSpark = TestIcebergSpark.spark;
    TestIcebergSpark.spark = null;
    currentSpark.stop();
  }

  @Test
  public void testRegisterIntegerBucketUDF() {
    IcebergSpark.registerBucketUDF(spark, "iceberg_bucket_int_16", DataTypes.IntegerType, 16);
    List<Row> results = spark.sql("SELECT iceberg_bucket_int_16(1)").collectAsList();

    assertThat(results).hasSize(1);
    assertThat((int) Transforms.bucket(16).bind(Types.IntegerType.get()).apply(1))
            .isEqualTo(results.get(0).getInt(0));
  }

  @Test
  public void testRegisterShortBucketUDF() {
    IcebergSpark.registerBucketUDF(spark, "iceberg_bucket_short_16", DataTypes.ShortType, 16);
    List<Row> results = spark.sql("SELECT iceberg_bucket_short_16(1S)").collectAsList();
    assertThat(results).hasSize(1);
    assertThat((int) Transforms.bucket(16).bind(Types.IntegerType.get()).apply(1))
            .isEqualTo(results.get(0).getInt(0));
  }

  @Test
  public void testRegisterByteBucketUDF() {
    IcebergSpark.registerBucketUDF(spark, "iceberg_bucket_byte_16", DataTypes.ByteType, 16);
    List<Row> results = spark.sql("SELECT iceberg_bucket_byte_16(1Y)").collectAsList();
    assertThat(results).hasSize(1);
    assertThat((int) Transforms.bucket(16).bind(Types.IntegerType.get()).apply(1))
            .isEqualTo(results.get(0).getInt(0));
  }

  @Test
  public void testRegisterLongBucketUDF() {
    IcebergSpark.registerBucketUDF(spark, "iceberg_bucket_long_16", DataTypes.LongType, 16);
    List<Row> results = spark.sql("SELECT iceberg_bucket_long_16(1L)").collectAsList();
    assertThat(results).hasSize(1);
    assertThat((int) Transforms.bucket(16).bind(Types.LongType.get()).apply(1L))
            .isEqualTo(results.get(0).getInt(0));
  }

  @Test
  public void testRegisterStringBucketUDF() {
    IcebergSpark.registerBucketUDF(spark, "iceberg_bucket_string_16", DataTypes.StringType, 16);
    List<Row> results = spark.sql("SELECT iceberg_bucket_string_16('hello')").collectAsList();
    assertThat(results).hasSize(1);
    assertThat((int) Transforms.bucket(16).bind(Types.StringType.get()).apply("hello"))
            .isEqualTo(results.get(0).getInt(0));
  }

  @Test
  public void testRegisterCharBucketUDF() {
    IcebergSpark.registerBucketUDF(spark, "iceberg_bucket_char_16", new CharType(5), 16);
    List<Row> results = spark.sql("SELECT iceberg_bucket_char_16('hello')").collectAsList();
    assertThat(results).hasSize(1);
    assertThat((int) Transforms.bucket(16).bind(Types.StringType.get()).apply("hello"))
            .isEqualTo(results.get(0).getInt(0));
  }

  @Test
  public void testRegisterVarCharBucketUDF() {
    IcebergSpark.registerBucketUDF(spark, "iceberg_bucket_varchar_16", new VarcharType(5), 16);
    List<Row> results = spark.sql("SELECT iceberg_bucket_varchar_16('hello')").collectAsList();
    assertThat(results).hasSize(1);
    assertThat((int) Transforms.bucket(16).bind(Types.StringType.get()).apply("hello"))
            .isEqualTo(results.get(0).getInt(0));
  }

  @Test
  public void testRegisterDateBucketUDF() {
    IcebergSpark.registerBucketUDF(spark, "iceberg_bucket_date_16", DataTypes.DateType, 16);
    List<Row> results =
        spark.sql("SELECT iceberg_bucket_date_16(DATE '2021-06-30')").collectAsList();
    assertThat(results).hasSize(1);
    assertThat((int)
            Transforms.bucket(16)
                    .bind(Types.DateType.get())
                    .apply(DateTimeUtils.fromJavaDate(Date.valueOf("2021-06-30"))))
            .isEqualTo(results.get(0).getInt(0));
  }

  @Test
  public void testRegisterTimestampBucketUDF() {
    IcebergSpark.registerBucketUDF(
        spark, "iceberg_bucket_timestamp_16", DataTypes.TimestampType, 16);
    List<Row> results =
        spark
            .sql("SELECT iceberg_bucket_timestamp_16(TIMESTAMP '2021-06-30 00:00:00.000')")
            .collectAsList();
    assertThat(results).hasSize(1);
    assertThat((int)
            Transforms.bucket(16)
                    .bind(Types.TimestampType.withZone())
                    .apply(
                            DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf("2021-06-30 00:00:00.000"))))
            .isEqualTo(results.get(0).getInt(0));
  }

  @Test
  public void testRegisterBinaryBucketUDF() {
    IcebergSpark.registerBucketUDF(spark, "iceberg_bucket_binary_16", DataTypes.BinaryType, 16);
    List<Row> results = spark.sql("SELECT iceberg_bucket_binary_16(X'0020001F')").collectAsList();
    assertThat(results).hasSize(1);
    assertThat((int)
            Transforms.bucket(16)
                    .bind(Types.BinaryType.get())
                    .apply(ByteBuffer.wrap(new byte[] {0x00, 0x20, 0x00, 0x1F})))
            .isEqualTo(results.get(0).getInt(0));
  }

  @Test
  public void testRegisterDecimalBucketUDF() {
    IcebergSpark.registerBucketUDF(spark, "iceberg_bucket_decimal_16", new DecimalType(4, 2), 16);
    List<Row> results = spark.sql("SELECT iceberg_bucket_decimal_16(11.11)").collectAsList();
    assertThat(results).hasSize(1);
    assertThat((int) Transforms.bucket(16).bind(Types.DecimalType.of(4, 2)).apply(new BigDecimal("11.11")))
            .isEqualTo(results.get(0).getInt(0));
  }

  @Test
  public void testRegisterBooleanBucketUDF() {
    assertThatThrownBy(
            () ->
                IcebergSpark.registerBucketUDF(
                    spark, "iceberg_bucket_boolean_16", DataTypes.BooleanType, 16))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot bucket by type: boolean");
  }

  @Test
  public void testRegisterDoubleBucketUDF() {
    assertThatThrownBy(
            () ->
                IcebergSpark.registerBucketUDF(
                    spark, "iceberg_bucket_double_16", DataTypes.DoubleType, 16))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot bucket by type: double");
  }

  @Test
  public void testRegisterFloatBucketUDF() {
    assertThatThrownBy(
            () ->
                IcebergSpark.registerBucketUDF(
                    spark, "iceberg_bucket_float_16", DataTypes.FloatType, 16))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot bucket by type: float");
  }

  @Test
  public void testRegisterIntegerTruncateUDF() {
    IcebergSpark.registerTruncateUDF(spark, "iceberg_truncate_int_4", DataTypes.IntegerType, 4);
    List<Row> results = spark.sql("SELECT iceberg_truncate_int_4(1)").collectAsList();
    assertThat(results).hasSize(1);
    assertThat(Transforms.truncate(4).bind(Types.IntegerType.get()).apply(1))
            .isEqualTo(results.get(0).getInt(0));
  }

  @Test
  public void testRegisterLongTruncateUDF() {
    IcebergSpark.registerTruncateUDF(spark, "iceberg_truncate_long_4", DataTypes.LongType, 4);
    List<Row> results = spark.sql("SELECT iceberg_truncate_long_4(1L)").collectAsList();
    assertThat(results).hasSize(1);
    assertThat(Transforms.truncate(4).bind(Types.LongType.get()).apply(1L))
            .isEqualTo(results.get(0).getLong(0));
  }

  @Test
  public void testRegisterDecimalTruncateUDF() {
    IcebergSpark.registerTruncateUDF(spark, "iceberg_truncate_decimal_4", new DecimalType(4, 2), 4);
    List<Row> results = spark.sql("SELECT iceberg_truncate_decimal_4(11.11)").collectAsList();
    assertThat(results).hasSize(1);
    assertThat(Transforms.truncate(4).bind(Types.DecimalType.of(4, 2)).apply(new BigDecimal("11.11"))).isEqualTo(results.get(0).getDecimal(0));
  }

  @Test
  public void testRegisterStringTruncateUDF() {
    IcebergSpark.registerTruncateUDF(spark, "iceberg_truncate_string_4", DataTypes.StringType, 4);
    List<Row> results = spark.sql("SELECT iceberg_truncate_string_4('hello')").collectAsList();
    assertThat(results).hasSize(1);
    assertThat(Transforms.truncate(4).bind(Types.StringType.get()).apply("hello"))
            .isEqualTo(results.get(0).getString(0));
  }
}
