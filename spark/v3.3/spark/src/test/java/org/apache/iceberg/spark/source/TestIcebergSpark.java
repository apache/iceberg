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
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestIcebergSpark {

  private static SparkSession spark = null;

  @BeforeClass
  public static void startSpark() {
    TestIcebergSpark.spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestIcebergSpark.spark;
    TestIcebergSpark.spark = null;
    currentSpark.stop();
  }

  @Test
  public void testRegisterIntegerBucketUDF() {
    IcebergSpark.registerBucketUDF(spark, "iceberg_bucket_int_16", DataTypes.IntegerType, 16);
    List<Row> results = spark.sql("SELECT iceberg_bucket_int_16(1)").collectAsList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(
        (int) Transforms.bucket(Types.IntegerType.get(), 16).apply(1), results.get(0).getInt(0));
  }

  @Test
  public void testRegisterShortBucketUDF() {
    IcebergSpark.registerBucketUDF(spark, "iceberg_bucket_short_16", DataTypes.ShortType, 16);
    List<Row> results = spark.sql("SELECT iceberg_bucket_short_16(1S)").collectAsList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(
        (int) Transforms.bucket(Types.IntegerType.get(), 16).apply(1), results.get(0).getInt(0));
  }

  @Test
  public void testRegisterByteBucketUDF() {
    IcebergSpark.registerBucketUDF(spark, "iceberg_bucket_byte_16", DataTypes.ByteType, 16);
    List<Row> results = spark.sql("SELECT iceberg_bucket_byte_16(1Y)").collectAsList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(
        (int) Transforms.bucket(Types.IntegerType.get(), 16).apply(1), results.get(0).getInt(0));
  }

  @Test
  public void testRegisterLongBucketUDF() {
    IcebergSpark.registerBucketUDF(spark, "iceberg_bucket_long_16", DataTypes.LongType, 16);
    List<Row> results = spark.sql("SELECT iceberg_bucket_long_16(1L)").collectAsList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(
        (int) Transforms.bucket(Types.LongType.get(), 16).apply(1L), results.get(0).getInt(0));
  }

  @Test
  public void testRegisterStringBucketUDF() {
    IcebergSpark.registerBucketUDF(spark, "iceberg_bucket_string_16", DataTypes.StringType, 16);
    List<Row> results = spark.sql("SELECT iceberg_bucket_string_16('hello')").collectAsList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(
        (int) Transforms.bucket(Types.StringType.get(), 16).apply("hello"),
        results.get(0).getInt(0));
  }

  @Test
  public void testRegisterCharBucketUDF() {
    IcebergSpark.registerBucketUDF(spark, "iceberg_bucket_char_16", new CharType(5), 16);
    List<Row> results = spark.sql("SELECT iceberg_bucket_char_16('hello')").collectAsList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(
        (int) Transforms.bucket(Types.StringType.get(), 16).apply("hello"),
        results.get(0).getInt(0));
  }

  @Test
  public void testRegisterVarCharBucketUDF() {
    IcebergSpark.registerBucketUDF(spark, "iceberg_bucket_varchar_16", new VarcharType(5), 16);
    List<Row> results = spark.sql("SELECT iceberg_bucket_varchar_16('hello')").collectAsList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(
        (int) Transforms.bucket(Types.StringType.get(), 16).apply("hello"),
        results.get(0).getInt(0));
  }

  @Test
  public void testRegisterDateBucketUDF() {
    IcebergSpark.registerBucketUDF(spark, "iceberg_bucket_date_16", DataTypes.DateType, 16);
    List<Row> results =
        spark.sql("SELECT iceberg_bucket_date_16(DATE '2021-06-30')").collectAsList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(
        (int)
            Transforms.bucket(Types.DateType.get(), 16)
                .apply(DateTimeUtils.fromJavaDate(Date.valueOf("2021-06-30"))),
        results.get(0).getInt(0));
  }

  @Test
  public void testRegisterTimestampBucketUDF() {
    IcebergSpark.registerBucketUDF(
        spark, "iceberg_bucket_timestamp_16", DataTypes.TimestampType, 16);
    List<Row> results =
        spark
            .sql("SELECT iceberg_bucket_timestamp_16(TIMESTAMP '2021-06-30 00:00:00.000')")
            .collectAsList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(
        (int)
            Transforms.bucket(Types.TimestampType.withZone(), 16)
                .apply(
                    DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf("2021-06-30 00:00:00.000"))),
        results.get(0).getInt(0));
  }

  @Test
  public void testRegisterBinaryBucketUDF() {
    IcebergSpark.registerBucketUDF(spark, "iceberg_bucket_binary_16", DataTypes.BinaryType, 16);
    List<Row> results = spark.sql("SELECT iceberg_bucket_binary_16(X'0020001F')").collectAsList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(
        (int)
            Transforms.bucket(Types.BinaryType.get(), 16)
                .apply(ByteBuffer.wrap(new byte[] {0x00, 0x20, 0x00, 0x1F})),
        results.get(0).getInt(0));
  }

  @Test
  public void testRegisterDecimalBucketUDF() {
    IcebergSpark.registerBucketUDF(spark, "iceberg_bucket_decimal_16", new DecimalType(4, 2), 16);
    List<Row> results = spark.sql("SELECT iceberg_bucket_decimal_16(11.11)").collectAsList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(
        (int) Transforms.bucket(Types.DecimalType.of(4, 2), 16).apply(new BigDecimal("11.11")),
        results.get(0).getInt(0));
  }

  @Test
  public void testRegisterBooleanBucketUDF() {
    Assertions.assertThatThrownBy(
            () ->
                IcebergSpark.registerBucketUDF(
                    spark, "iceberg_bucket_boolean_16", DataTypes.BooleanType, 16))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot bucket by type: boolean");
  }

  @Test
  public void testRegisterDoubleBucketUDF() {
    Assertions.assertThatThrownBy(
            () ->
                IcebergSpark.registerBucketUDF(
                    spark, "iceberg_bucket_double_16", DataTypes.DoubleType, 16))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot bucket by type: double");
  }

  @Test
  public void testRegisterFloatBucketUDF() {
    Assertions.assertThatThrownBy(
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
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(
        Transforms.truncate(Types.IntegerType.get(), 4).apply(1), results.get(0).getInt(0));
  }

  @Test
  public void testRegisterLongTruncateUDF() {
    IcebergSpark.registerTruncateUDF(spark, "iceberg_truncate_long_4", DataTypes.LongType, 4);
    List<Row> results = spark.sql("SELECT iceberg_truncate_long_4(1L)").collectAsList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(
        Transforms.truncate(Types.LongType.get(), 4).apply(1L), results.get(0).getLong(0));
  }

  @Test
  public void testRegisterDecimalTruncateUDF() {
    IcebergSpark.registerTruncateUDF(spark, "iceberg_truncate_decimal_4", new DecimalType(4, 2), 4);
    List<Row> results = spark.sql("SELECT iceberg_truncate_decimal_4(11.11)").collectAsList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(
        Transforms.truncate(Types.DecimalType.of(4, 2), 4).apply(new BigDecimal("11.11")),
        results.get(0).getDecimal(0));
  }

  @Test
  public void testRegisterStringTruncateUDF() {
    IcebergSpark.registerTruncateUDF(spark, "iceberg_truncate_string_4", DataTypes.StringType, 4);
    List<Row> results = spark.sql("SELECT iceberg_truncate_string_4('hello')").collectAsList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(
        Transforms.truncate(Types.StringType.get(), 4).apply("hello"), results.get(0).getString(0));
  }
}
