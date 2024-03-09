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

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.io.BaseEncoding;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.iceberg.spark.functions.BucketFunction;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.types.DataTypes;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestSparkBucketFunction extends TestBaseWithCatalog {
  @BeforeEach
  public void useCatalog() {
    sql("USE %s", catalogName);
  }

  @TestTemplate
  public void testSpecValues() {
    assertThat(new BucketFunction.BucketInt(DataTypes.IntegerType).hash(34))
        .as("Spec example: hash(34) = 2017239379")
        .isEqualTo(2017239379);

    assertThat(new BucketFunction.BucketLong(DataTypes.IntegerType).hash(34L))
        .as("Spec example: hash(34L) = 2017239379")
        .isEqualTo(2017239379);

    assertThat(
            new BucketFunction.BucketDecimal(DataTypes.createDecimalType(9, 2))
                .hash(new BigDecimal("14.20")))
        .as("Spec example: hash(decimal2(14.20)) = -500754589")
        .isEqualTo(-500754589);

    Literal<Integer> date = Literal.of("2017-11-16").to(Types.DateType.get());
    assertThat(new BucketFunction.BucketInt(DataTypes.DateType).hash(date.value()))
        .as("Spec example: hash(2017-11-16) = -653330422")
        .isEqualTo(-653330422);

    Literal<Long> timestampVal =
        Literal.of("2017-11-16T22:31:08").to(Types.TimestampType.withoutZone());
    assertThat(new BucketFunction.BucketLong(DataTypes.TimestampType).hash(timestampVal.value()))
        .as("Spec example: hash(2017-11-16T22:31:08) = -2047944441")
        .isEqualTo(-2047944441);

    Literal<Long> timestampntzVal =
        Literal.of("2017-11-16T22:31:08").to(Types.TimestampType.withoutZone());
    assertThat(
            new BucketFunction.BucketLong(DataTypes.TimestampNTZType).hash(timestampntzVal.value()))
        .as("Spec example: hash(2017-11-16T22:31:08) = -2047944441")
        .isEqualTo(-2047944441);

    assertThat(new BucketFunction.BucketString().hash("iceberg"))
        .as("Spec example: hash(\"iceberg\") = 1210000089")
        .isEqualTo(1210000089);

    ByteBuffer bytes = ByteBuffer.wrap(new byte[] {0, 1, 2, 3});
    assertThat(new BucketFunction.BucketBinary().hash(bytes))
        .as("Spec example: hash([00 01 02 03]) = -188683207")
        .isEqualTo(-188683207);
  }

  @TestTemplate
  public void testBucketIntegers() {
    assertThat(scalarSql("SELECT system.bucket(10, 8Y)"))
        .as("Byte type should bucket similarly to integer")
        .isEqualTo(3);
    assertThat(scalarSql("SELECT system.bucket(10, 8S)"))
        .as("Short type should bucket similarly to integer")
        .isEqualTo(3);
    // Integers
    assertThat(scalarSql("SELECT system.bucket(10, 8)")).isEqualTo(3);
    assertThat(scalarSql("SELECT system.bucket(100, 34)")).isEqualTo(79);
    assertThat(scalarSql("SELECT system.bucket(1, CAST(null AS INT))")).isNull();
  }

  @TestTemplate
  public void testBucketDates() {
    assertThat(scalarSql("SELECT system.bucket(10, date('1970-01-09'))")).isEqualTo(3);
    assertThat(scalarSql("SELECT system.bucket(100, date('1970-02-04'))")).isEqualTo(79);
    assertThat(scalarSql("SELECT system.bucket(1, CAST(null AS DATE))")).isNull();
  }

  @TestTemplate
  public void testBucketLong() {
    assertThat(scalarSql("SELECT system.bucket(100, 34L)")).isEqualTo(79);
    assertThat(scalarSql("SELECT system.bucket(100, 0L)")).isEqualTo(76);
    assertThat(scalarSql("SELECT system.bucket(100, -34L)")).isEqualTo(97);
    assertThat(scalarSql("SELECT system.bucket(2, -1L)")).isEqualTo(0);
    assertThat(scalarSql("SELECT system.bucket(2, CAST(null AS LONG))")).isNull();
  }

  @TestTemplate
  public void testBucketDecimal() {
    assertThat(scalarSql("SELECT system.bucket(64, CAST('12.34' as DECIMAL(9, 2)))")).isEqualTo(56);
    assertThat(scalarSql("SELECT system.bucket(18, CAST('12.30' as DECIMAL(9, 2)))")).isEqualTo(13);
    assertThat(scalarSql("SELECT system.bucket(16, CAST('12.999' as DECIMAL(9, 3)))")).isEqualTo(2);
    assertThat(scalarSql("SELECT system.bucket(32, CAST('0.05' as DECIMAL(5, 2)))")).isEqualTo(21);
    assertThat(scalarSql("SELECT system.bucket(128, CAST('0.05' as DECIMAL(9, 2)))")).isEqualTo(85);
    assertThat(scalarSql("SELECT system.bucket(18, CAST('0.05' as DECIMAL(9, 2)))")).isEqualTo(3);

    assertThat(scalarSql("SELECT system.bucket(2, CAST(null AS decimal))"))
        .as("Null input should return null")
        .isNull();
  }

  @TestTemplate
  public void testBucketTimestamp() {
    assertThat(scalarSql("SELECT system.bucket(100, TIMESTAMP '1997-01-01 00:00:00 UTC+00:00')"))
        .isEqualTo(99);
    assertThat(scalarSql("SELECT system.bucket(100, TIMESTAMP '1997-01-31 09:26:56 UTC+00:00')"))
        .isEqualTo(85);
    assertThat(scalarSql("SELECT system.bucket(100, TIMESTAMP '2022-08-08 00:00:00 UTC+00:00')"))
        .isEqualTo(62);
    assertThat(scalarSql("SELECT system.bucket(2, CAST(null AS timestamp))")).isNull();
  }

  @TestTemplate
  public void testBucketString() {
    assertThat(scalarSql("SELECT system.bucket(5, 'abcdefg')")).isEqualTo(4);
    assertThat(scalarSql("SELECT system.bucket(128, 'abc')")).isEqualTo(122);
    assertThat(scalarSql("SELECT system.bucket(64, 'abcde')")).isEqualTo(54);
    assertThat(scalarSql("SELECT system.bucket(12, '测试')")).isEqualTo(8);
    assertThat(scalarSql("SELECT system.bucket(16, '测试raul试测')")).isEqualTo(1);
    assertThat(scalarSql("SELECT system.bucket(16, CAST('测试raul试测' AS varchar(8)))"))
        .as("Varchar should work like string")
        .isEqualTo(1);
    assertThat(scalarSql("SELECT system.bucket(16, CAST('测试raul试测' AS char(8)))"))
        .as("Char should work like string")
        .isEqualTo(1);
    assertThat(scalarSql("SELECT system.bucket(16, '')"))
        .as("Should not fail on the empty string")
        .isEqualTo(0);
    assertThat(scalarSql("SELECT system.bucket(16, CAST(null AS string))"))
        .as("Null input should return null as output")
        .isNull();
  }

  @TestTemplate
  public void testBucketBinary() {
    assertThat(scalarSql("SELECT system.bucket(10, X'0102030405060708090a0b0c0d0e0f')"))
        .isEqualTo(1);
    assertThat(scalarSql("SELECT system.bucket(12, %s)", asBytesLiteral("abcdefg"))).isEqualTo(10);
    assertThat(scalarSql("SELECT system.bucket(18, %s)", asBytesLiteral("abc\0\0"))).isEqualTo(13);
    assertThat(scalarSql("SELECT system.bucket(48, %s)", asBytesLiteral("abc"))).isEqualTo(42);
    assertThat(scalarSql("SELECT system.bucket(16, %s)", asBytesLiteral("测试_"))).isEqualTo(3);

    assertThat(scalarSql("SELECT system.bucket(100, CAST(null AS binary))"))
        .as("Null input should return null as output")
        .isNull();
  }

  @TestTemplate
  public void testNumBucketsAcceptsShortAndByte() {
    assertThat(scalarSql("SELECT system.bucket(5S, 1L)"))
        .as("Short types should be usable for the number of buckets field")
        .isEqualTo(1);

    assertThat(scalarSql("SELECT system.bucket(5Y, 1)"))
        .as("Byte types should be allowed for the number of buckets field")
        .isEqualTo(1);
  }

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
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
