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
package org.apache.iceberg.spark.sql;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.io.BaseEncoding;
import org.apache.iceberg.spark.SparkTestBaseWithCatalog;
import org.apache.iceberg.spark.functions.BucketFunction;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.types.DataTypes;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestSparkBucketFunction extends SparkTestBaseWithCatalog {
  @Before
  public void useCatalog() {
    sql("USE %s", catalogName);
  }

  @Test
  public void testSpecValues() {
    Assert.assertEquals(
        "Spec example: hash(34) = 2017239379",
        2017239379,
        new BucketFunction.BucketInt(DataTypes.IntegerType).hash(34));

    Assert.assertEquals(
        "Spec example: hash(34L) = 2017239379",
        2017239379,
        new BucketFunction.BucketLong(DataTypes.LongType).hash(34L));

    Assert.assertEquals(
        "Spec example: hash(decimal2(14.20)) = -500754589",
        -500754589,
        new BucketFunction.BucketDecimal(DataTypes.createDecimalType(9, 2))
            .hash(new BigDecimal("14.20")));

    Literal<Integer> date = Literal.of("2017-11-16").to(Types.DateType.get());
    Assert.assertEquals(
        "Spec example: hash(2017-11-16) = -653330422",
        -653330422,
        new BucketFunction.BucketInt(DataTypes.DateType).hash(date.value()));

    Literal<Long> timestampVal =
        Literal.of("2017-11-16T22:31:08").to(Types.TimestampType.withoutZone());
    Assert.assertEquals(
        "Spec example: hash(2017-11-16T22:31:08) = -2047944441",
        -2047944441,
        new BucketFunction.BucketLong(DataTypes.TimestampType).hash(timestampVal.value()));

    Assert.assertEquals(
        "Spec example: hash(\"iceberg\") = 1210000089",
        1210000089,
        new BucketFunction.BucketString().hash("iceberg"));

    ByteBuffer bytes = ByteBuffer.wrap(new byte[] {0, 1, 2, 3});
    Assert.assertEquals(
        "Spec example: hash([00 01 02 03]) = -188683207",
        -188683207,
        new BucketFunction.BucketBinary().hash(bytes));
  }

  @Test
  public void testBucketIntegers() {
    Assert.assertEquals(
        "Byte type should bucket similarly to integer",
        3,
        scalarSql("SELECT system.bucket(10, 8Y)"));
    Assert.assertEquals(
        "Short type should bucket similarly to integer",
        3,
        scalarSql("SELECT system.bucket(10, 8S)"));
    // Integers
    Assert.assertEquals(3, scalarSql("SELECT system.bucket(10, 8)"));
    Assert.assertEquals(79, scalarSql("SELECT system.bucket(100, 34)"));
    Assert.assertNull(scalarSql("SELECT system.bucket(1, CAST(null AS INT))"));
  }

  @Test
  public void testBucketDates() {
    Assert.assertEquals(3, scalarSql("SELECT system.bucket(10, date('1970-01-09'))"));
    Assert.assertEquals(79, scalarSql("SELECT system.bucket(100, date('1970-02-04'))"));
    Assert.assertNull(scalarSql("SELECT system.bucket(1, CAST(null AS DATE))"));
  }

  @Test
  public void testBucketLong() {
    Assert.assertEquals(79, scalarSql("SELECT system.bucket(100, 34L)"));
    Assert.assertEquals(76, scalarSql("SELECT system.bucket(100, 0L)"));
    Assert.assertEquals(97, scalarSql("SELECT system.bucket(100, -34L)"));
    Assert.assertEquals(0, scalarSql("SELECT system.bucket(2, -1L)"));
    Assert.assertNull(scalarSql("SELECT system.bucket(2, CAST(null AS LONG))"));
  }

  @Test
  public void testBucketDecimal() {
    Assert.assertEquals(56, scalarSql("SELECT system.bucket(64, CAST('12.34' as DECIMAL(9, 2)))"));
    Assert.assertEquals(13, scalarSql("SELECT system.bucket(18, CAST('12.30' as DECIMAL(9, 2)))"));
    Assert.assertEquals(2, scalarSql("SELECT system.bucket(16, CAST('12.999' as DECIMAL(9, 3)))"));
    Assert.assertEquals(21, scalarSql("SELECT system.bucket(32, CAST('0.05' as DECIMAL(5, 2)))"));
    Assert.assertEquals(85, scalarSql("SELECT system.bucket(128, CAST('0.05' as DECIMAL(9, 2)))"));
    Assert.assertEquals(3, scalarSql("SELECT system.bucket(18, CAST('0.05' as DECIMAL(9, 2)))"));

    Assert.assertNull(
        "Null input should return null",
        scalarSql("SELECT system.bucket(2, CAST(null AS decimal))"));
  }

  @Test
  public void testBucketTimestamp() {
    Assert.assertEquals(
        99, scalarSql("SELECT system.bucket(100, TIMESTAMP '1997-01-01 00:00:00 UTC+00:00')"));
    Assert.assertEquals(
        85, scalarSql("SELECT system.bucket(100, TIMESTAMP '1997-01-31 09:26:56 UTC+00:00')"));
    Assert.assertEquals(
        62, scalarSql("SELECT system.bucket(100, TIMESTAMP '2022-08-08 00:00:00 UTC+00:00')"));
    Assert.assertNull(scalarSql("SELECT system.bucket(2, CAST(null AS timestamp))"));
  }

  @Test
  public void testBucketString() {
    Assert.assertEquals(4, scalarSql("SELECT system.bucket(5, 'abcdefg')"));
    Assert.assertEquals(122, scalarSql("SELECT system.bucket(128, 'abc')"));
    Assert.assertEquals(54, scalarSql("SELECT system.bucket(64, 'abcde')"));
    Assert.assertEquals(8, scalarSql("SELECT system.bucket(12, '测试')"));
    Assert.assertEquals(1, scalarSql("SELECT system.bucket(16, '测试raul试测')"));
    Assert.assertEquals(
        "Varchar should work like string",
        1,
        scalarSql("SELECT system.bucket(16, CAST('测试raul试测' AS varchar(8)))"));
    Assert.assertEquals(
        "Char should work like string",
        1,
        scalarSql("SELECT system.bucket(16, CAST('测试raul试测' AS char(8)))"));
    Assert.assertEquals(
        "Should not fail on the empty string", 0, scalarSql("SELECT system.bucket(16, '')"));
    Assert.assertNull(
        "Null input should return null as output",
        scalarSql("SELECT system.bucket(16, CAST(null AS string))"));
  }

  @Test
  public void testBucketBinary() {
    Assert.assertEquals(
        1, scalarSql("SELECT system.bucket(10, X'0102030405060708090a0b0c0d0e0f')"));
    Assert.assertEquals(10, scalarSql("SELECT system.bucket(12, %s)", asBytesLiteral("abcdefg")));
    Assert.assertEquals(13, scalarSql("SELECT system.bucket(18, %s)", asBytesLiteral("abc\0\0")));
    Assert.assertEquals(42, scalarSql("SELECT system.bucket(48, %s)", asBytesLiteral("abc")));
    Assert.assertEquals(3, scalarSql("SELECT system.bucket(16, %s)", asBytesLiteral("测试_")));

    Assert.assertNull(
        "Null input should return null as output",
        scalarSql("SELECT system.bucket(100, CAST(null AS binary))"));
  }

  @Test
  public void testNumBucketsAcceptsShortAndByte() {
    Assert.assertEquals(
        "Short types should be usable for the number of buckets field",
        1,
        scalarSql("SELECT system.bucket(5S, 1L)"));

    Assert.assertEquals(
        "Byte types should be allowed for the number of buckets field",
        1,
        scalarSql("SELECT system.bucket(5Y, 1)"));
  }

  @Test
  public void testWrongNumberOfArguments() {
    Assertions.assertThatThrownBy(() -> scalarSql("SELECT system.bucket()"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'bucket' cannot process input: (): Wrong number of inputs (expected numBuckets and value)");

    Assertions.assertThatThrownBy(() -> scalarSql("SELECT system.bucket(1)"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'bucket' cannot process input: (int): Wrong number of inputs (expected numBuckets and value)");

    Assertions.assertThatThrownBy(() -> scalarSql("SELECT system.bucket(1, 1L, 1)"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'bucket' cannot process input: (int, bigint, int): Wrong number of inputs (expected numBuckets and value)");
  }

  @Test
  public void testInvalidTypesCannotBeUsedForNumberOfBuckets() {
    Assertions.assertThatThrownBy(
            () -> scalarSql("SELECT system.bucket(CAST('12.34' as DECIMAL(9, 2)), 10)"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'bucket' cannot process input: (decimal(9,2), int): Expected number of buckets to be tinyint, shortint or int");

    Assertions.assertThatThrownBy(() -> scalarSql("SELECT system.bucket(12L, 10)"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'bucket' cannot process input: (bigint, int): Expected number of buckets to be tinyint, shortint or int");

    Assertions.assertThatThrownBy(() -> scalarSql("SELECT system.bucket('5', 10)"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'bucket' cannot process input: (string, int): Expected number of buckets to be tinyint, shortint or int");

    Assertions.assertThatThrownBy(
            () -> scalarSql("SELECT system.bucket(INTERVAL '100-00' YEAR TO MONTH, 10)"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'bucket' cannot process input: (interval year to month, int): Expected number of buckets to be tinyint, shortint or int");

    Assertions.assertThatThrownBy(
            () ->
                scalarSql("SELECT system.bucket(CAST('11 23:4:0' AS INTERVAL DAY TO SECOND), 10)"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'bucket' cannot process input: (interval day to second, int): Expected number of buckets to be tinyint, shortint or int");
  }

  @Test
  public void testInvalidTypesForBucketColumn() {
    Assertions.assertThatThrownBy(
            () -> scalarSql("SELECT system.bucket(10, cast(12.3456 as float))"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'bucket' cannot process input: (int, float): Expected column to be date, tinyint, smallint, int, bigint, decimal, timestamp, string, or binary");

    Assertions.assertThatThrownBy(
            () -> scalarSql("SELECT system.bucket(10, cast(12.3456 as double))"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'bucket' cannot process input: (int, double): Expected column to be date, tinyint, smallint, int, bigint, decimal, timestamp, string, or binary");

    Assertions.assertThatThrownBy(() -> scalarSql("SELECT system.bucket(10, true)"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith("Function 'bucket' cannot process input: (int, boolean)");

    Assertions.assertThatThrownBy(() -> scalarSql("SELECT system.bucket(10, map(1, 1))"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith("Function 'bucket' cannot process input: (int, map<int,int>)");

    Assertions.assertThatThrownBy(() -> scalarSql("SELECT system.bucket(10, array(1L))"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith("Function 'bucket' cannot process input: (int, array<bigint>)");

    Assertions.assertThatThrownBy(
            () -> scalarSql("SELECT system.bucket(10, INTERVAL '100-00' YEAR TO MONTH)"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'bucket' cannot process input: (int, interval year to month)");

    Assertions.assertThatThrownBy(
            () ->
                scalarSql("SELECT system.bucket(10, CAST('11 23:4:0' AS INTERVAL DAY TO SECOND))"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'bucket' cannot process input: (int, interval day to second)");
  }

  @Test
  public void testThatMagicFunctionsAreInvoked() {
    // TinyInt
    Assertions.assertThat(scalarSql("EXPLAIN EXTENDED SELECT system.bucket(5, 6Y)"))
        .asString()
        .isNotNull()
        .contains("staticinvoke(class org.apache.iceberg.spark.functions.BucketFunction$BucketInt");

    // SmallInt
    Assertions.assertThat(scalarSql("EXPLAIN EXTENDED SELECT system.bucket(5, 6S)"))
        .asString()
        .isNotNull()
        .contains("staticinvoke(class org.apache.iceberg.spark.functions.BucketFunction$BucketInt");

    // Int
    Assertions.assertThat(scalarSql("EXPLAIN EXTENDED SELECT system.bucket(5, 6)"))
        .asString()
        .isNotNull()
        .contains("staticinvoke(class org.apache.iceberg.spark.functions.BucketFunction$BucketInt");

    // Date
    Assertions.assertThat(
            scalarSql("EXPLAIN EXTENDED SELECT system.bucket(100, DATE '2022-08-08')"))
        .asString()
        .isNotNull()
        .contains("staticinvoke(class org.apache.iceberg.spark.functions.BucketFunction$BucketInt");

    // Long
    Assertions.assertThat(scalarSql("EXPLAIN EXTENDED SELECT system.bucket(5, 6L)"))
        .asString()
        .isNotNull()
        .contains(
            "staticinvoke(class org.apache.iceberg.spark.functions.BucketFunction$BucketLong");

    // Timestamp
    Assertions.assertThat(
            scalarSql("EXPLAIN EXTENDED SELECT system.bucket(100, TIMESTAMP '2022-08-08')"))
        .asString()
        .isNotNull()
        .contains(
            "staticinvoke(class org.apache.iceberg.spark.functions.BucketFunction$BucketLong");

    // String
    Assertions.assertThat(scalarSql("EXPLAIN EXTENDED SELECT system.bucket(5, 'abcdefg')"))
        .asString()
        .isNotNull()
        .contains(
            "staticinvoke(class org.apache.iceberg.spark.functions.BucketFunction$BucketString");

    // Decimal
    Assertions.assertThat(
            scalarSql("EXPLAIN EXTENDED SELECT system.bucket(5, CAST('12.34' AS DECIMAL))"))
        .asString()
        .isNotNull()
        .contains(
            "staticinvoke(class org.apache.iceberg.spark.functions.BucketFunction$BucketDecimal");

    // Binary
    Assertions.assertThat(
            scalarSql("EXPLAIN EXTENDED SELECT system.bucket(4, X'0102030405060708')"))
        .asString()
        .isNotNull()
        .contains(
            "staticinvoke(class org.apache.iceberg.spark.functions.BucketFunction$BucketBinary");
  }

  private String asBytesLiteral(String value) {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    return "X'" + BaseEncoding.base16().encode(bytes) + "'";
  }
}
