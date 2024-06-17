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
import java.nio.charset.StandardCharsets;
import org.apache.iceberg.relocated.com.google.common.io.BaseEncoding;
import org.apache.iceberg.spark.SparkTestBaseWithCatalog;
import org.apache.spark.sql.AnalysisException;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestSparkTruncateFunction extends SparkTestBaseWithCatalog {
  public TestSparkTruncateFunction() {}

  @Before
  public void useCatalog() {
    sql("USE %s", catalogName);
  }

  @Test
  public void testTruncateTinyInt() {
    Assert.assertEquals((byte) 0, scalarSql("SELECT system.truncate(10, 0Y)"));
    Assert.assertEquals((byte) 0, scalarSql("SELECT system.truncate(10, 1Y)"));
    Assert.assertEquals((byte) 0, scalarSql("SELECT system.truncate(10, 5Y)"));
    Assert.assertEquals((byte) 0, scalarSql("SELECT system.truncate(10, 9Y)"));
    Assert.assertEquals((byte) 10, scalarSql("SELECT system.truncate(10, 10Y)"));
    Assert.assertEquals((byte) 10, scalarSql("SELECT system.truncate(10, 11Y)"));
    Assert.assertEquals((byte) -10, scalarSql("SELECT system.truncate(10, -1Y)"));
    Assert.assertEquals((byte) -10, scalarSql("SELECT system.truncate(10, -5Y)"));
    Assert.assertEquals((byte) -10, scalarSql("SELECT system.truncate(10, -10Y)"));
    Assert.assertEquals((byte) -20, scalarSql("SELECT system.truncate(10, -11Y)"));

    // Check that different widths can be used
    Assert.assertEquals((byte) -2, scalarSql("SELECT system.truncate(2, -1Y)"));

    Assert.assertNull(
        "Null input should return null",
        scalarSql("SELECT system.truncate(2, CAST(null AS tinyint))"));
  }

  @Test
  public void testTruncateSmallInt() {
    Assert.assertEquals((short) 0, scalarSql("SELECT system.truncate(10, 0S)"));
    Assert.assertEquals((short) 0, scalarSql("SELECT system.truncate(10, 1S)"));
    Assert.assertEquals((short) 0, scalarSql("SELECT system.truncate(10, 5S)"));
    Assert.assertEquals((short) 0, scalarSql("SELECT system.truncate(10, 9S)"));
    Assert.assertEquals((short) 10, scalarSql("SELECT system.truncate(10, 10S)"));
    Assert.assertEquals((short) 10, scalarSql("SELECT system.truncate(10, 11S)"));
    Assert.assertEquals((short) -10, scalarSql("SELECT system.truncate(10, -1S)"));
    Assert.assertEquals((short) -10, scalarSql("SELECT system.truncate(10, -5S)"));
    Assert.assertEquals((short) -10, scalarSql("SELECT system.truncate(10, -10S)"));
    Assert.assertEquals((short) -20, scalarSql("SELECT system.truncate(10, -11S)"));

    // Check that different widths can be used
    Assert.assertEquals((short) -2, scalarSql("SELECT system.truncate(2, -1S)"));

    Assert.assertNull(
        "Null input should return null",
        scalarSql("SELECT system.truncate(2, CAST(null AS smallint))"));
  }

  @Test
  public void testTruncateInt() {
    Assert.assertEquals(0, scalarSql("SELECT system.truncate(10, 0)"));
    Assert.assertEquals(0, scalarSql("SELECT system.truncate(10, 1)"));
    Assert.assertEquals(0, scalarSql("SELECT system.truncate(10, 5)"));
    Assert.assertEquals(0, scalarSql("SELECT system.truncate(10, 9)"));
    Assert.assertEquals(10, scalarSql("SELECT system.truncate(10, 10)"));
    Assert.assertEquals(10, scalarSql("SELECT system.truncate(10, 11)"));
    Assert.assertEquals(-10, scalarSql("SELECT system.truncate(10, -1)"));
    Assert.assertEquals(-10, scalarSql("SELECT system.truncate(10, -5)"));
    Assert.assertEquals(-10, scalarSql("SELECT system.truncate(10, -10)"));
    Assert.assertEquals(-20, scalarSql("SELECT system.truncate(10, -11)"));

    // Check that different widths can be used
    Assert.assertEquals(-2, scalarSql("SELECT system.truncate(2, -1)"));
    Assert.assertEquals(0, scalarSql("SELECT system.truncate(300, 1)"));

    Assert.assertNull(
        "Null input should return null", scalarSql("SELECT system.truncate(2, CAST(null AS int))"));
  }

  @Test
  public void testTruncateBigInt() {
    Assert.assertEquals(0L, scalarSql("SELECT system.truncate(10, 0L)"));
    Assert.assertEquals(0L, scalarSql("SELECT system.truncate(10, 1L)"));
    Assert.assertEquals(0L, scalarSql("SELECT system.truncate(10, 5L)"));
    Assert.assertEquals(0L, scalarSql("SELECT system.truncate(10, 9L)"));
    Assert.assertEquals(10L, scalarSql("SELECT system.truncate(10, 10L)"));
    Assert.assertEquals(10L, scalarSql("SELECT system.truncate(10, 11L)"));
    Assert.assertEquals(-10L, scalarSql("SELECT system.truncate(10, -1L)"));
    Assert.assertEquals(-10L, scalarSql("SELECT system.truncate(10, -5L)"));
    Assert.assertEquals(-10L, scalarSql("SELECT system.truncate(10, -10L)"));
    Assert.assertEquals(-20L, scalarSql("SELECT system.truncate(10, -11L)"));

    // Check that different widths can be used
    Assert.assertEquals(-2L, scalarSql("SELECT system.truncate(2, -1L)"));

    Assert.assertNull(
        "Null input should return null",
        scalarSql("SELECT system.truncate(2, CAST(null AS bigint))"));
  }

  @Test
  public void testTruncateDecimal() {
    // decimal truncation works by applying the decimal scale to the width: ie 10 scale 2 = 0.10
    Assert.assertEquals(
        new BigDecimal("12.30"),
        scalarSql("SELECT system.truncate(10, CAST(%s as DECIMAL(9, 2)))", "12.34"));

    Assert.assertEquals(
        new BigDecimal("12.30"),
        scalarSql("SELECT system.truncate(10, CAST(%s as DECIMAL(9, 2)))", "12.30"));

    Assert.assertEquals(
        new BigDecimal("12.290"),
        scalarSql("SELECT system.truncate(10, CAST(%s as DECIMAL(9, 3)))", "12.299"));

    Assert.assertEquals(
        new BigDecimal("0.03"),
        scalarSql("SELECT system.truncate(3, CAST(%s as DECIMAL(5, 2)))", "0.05"));

    Assert.assertEquals(
        new BigDecimal("0.00"),
        scalarSql("SELECT system.truncate(10, CAST(%s as DECIMAL(9, 2)))", "0.05"));

    Assert.assertEquals(
        new BigDecimal("-0.10"),
        scalarSql("SELECT system.truncate(10, CAST(%s as DECIMAL(9, 2)))", "-0.05"));

    Assert.assertEquals(
        "Implicit decimal scale and precision should be allowed",
        new BigDecimal("12345.3480"),
        scalarSql("SELECT system.truncate(10, 12345.3482)"));

    BigDecimal truncatedDecimal =
        (BigDecimal) scalarSql("SELECT system.truncate(10, CAST(%s as DECIMAL(6, 4)))", "-0.05");
    Assert.assertEquals(
        "Truncating a decimal should return a decimal with the same scale",
        4,
        truncatedDecimal.scale());

    Assert.assertEquals(
        "Truncating a decimal should return a decimal with the correct scale",
        BigDecimal.valueOf(-500, 4),
        truncatedDecimal);

    Assert.assertNull(
        "Null input should return null",
        scalarSql("SELECT system.truncate(2, CAST(null AS decimal))"));
  }

  @SuppressWarnings("checkstyle:AvoidEscapedUnicodeCharacters")
  @Test
  public void testTruncateString() {
    Assert.assertEquals(
        "Should system.truncate strings longer than length",
        "abcde",
        scalarSql("SELECT system.truncate(5, 'abcdefg')"));

    Assert.assertEquals(
        "Should not pad strings shorter than length",
        "abc",
        scalarSql("SELECT system.truncate(5, 'abc')"));

    Assert.assertEquals(
        "Should not alter strings equal to length",
        "abcde",
        scalarSql("SELECT system.truncate(5, 'abcde')"));

    Assert.assertEquals(
        "Strings with multibyte unicode characters should should truncate along codepoint boundaries",
        "イロ",
        scalarSql("SELECT system.truncate(2, 'イロハニホヘト')"));

    Assert.assertEquals(
        "Strings with multibyte unicode characters should truncate along codepoint boundaries",
        "イロハ",
        scalarSql("SELECT system.truncate(3, 'イロハニホヘト')"));

    Assert.assertEquals(
        "Strings with multibyte unicode characters should not alter input with fewer codepoints than width",
        "イロハニホヘト",
        scalarSql("SELECT system.truncate(7, 'イロハニホヘト')"));

    String stringWithTwoCodePointsEachFourBytes = "\uD800\uDC00\uD800\uDC00";
    Assert.assertEquals(
        "String truncation on four byte codepoints should work as expected",
        "\uD800\uDC00",
        scalarSql("SELECT system.truncate(1, '%s')", stringWithTwoCodePointsEachFourBytes));

    Assert.assertEquals(
        "Should handle three-byte UTF-8 characters appropriately",
        "测",
        scalarSql("SELECT system.truncate(1, '测试')"));

    Assert.assertEquals(
        "Should handle three-byte UTF-8 characters mixed with two byte utf-8 characters",
        "测试ra",
        scalarSql("SELECT system.truncate(4, '测试raul试测')"));

    Assert.assertEquals(
        "Should not fail on the empty string", "", scalarSql("SELECT system.truncate(10, '')"));

    Assert.assertNull(
        "Null input should return null as output",
        scalarSql("SELECT system.truncate(3, CAST(null AS string))"));

    Assert.assertEquals(
        "Varchar should work like string",
        "测试ra",
        scalarSql("SELECT system.truncate(4, CAST('测试raul试测' AS varchar(8)))"));

    Assert.assertEquals(
        "Char should work like string",
        "测试ra",
        scalarSql("SELECT system.truncate(4, CAST('测试raul试测' AS char(8)))"));
  }

  @Test
  public void testTruncateBinary() {
    Assert.assertArrayEquals(
        new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
        (byte[]) scalarSql("SELECT system.truncate(10, X'0102030405060708090a0b0c0d0e0f')"));
    Assert.assertArrayEquals(
        "Should return the same input when value is equal to truncation width",
        "abc".getBytes(StandardCharsets.UTF_8),
        (byte[]) scalarSql("SELECT system.truncate(3, %s)", asBytesLiteral("abcdefg")));
    Assert.assertArrayEquals(
        "Should not truncate, pad, or trim the input when its length is less than the width",
        "abc\0\0".getBytes(StandardCharsets.UTF_8),
        (byte[]) scalarSql("SELECT system.truncate(10, %s)", asBytesLiteral("abc\0\0")));
    Assert.assertArrayEquals(
        "Should not pad the input when its length is equal to the width",
        "abc".getBytes(StandardCharsets.UTF_8),
        (byte[]) scalarSql("SELECT system.truncate(3, %s)", asBytesLiteral("abc")));
    Assert.assertArrayEquals(
        "Should handle three-byte UTF-8 characters appropriately",
        "测试".getBytes(StandardCharsets.UTF_8),
        (byte[]) scalarSql("SELECT system.truncate(6, %s)", asBytesLiteral("测试_")));

    Assert.assertNull(
        "Null input should return null as output",
        scalarSql("SELECT system.truncate(3, CAST(null AS binary))"));
  }

  @Test
  public void testTruncateUsingDataframeForWidthWithVaryingWidth() {
    // This situation is atypical but allowed. Typically, width is static as data is partitioned on
    // one width.
    long rumRows = 10L;
    long numNonZero =
        spark
            .range(rumRows)
            .toDF("value")
            .selectExpr("CAST(value + 1 AS INT) AS width", "value")
            .selectExpr("system.truncate(width, value) as truncated_value")
            .filter("truncated_value == 0")
            .count();
    Assert.assertEquals(
        "A truncate function with variable widths should be usable on dataframe columns",
        rumRows,
        numNonZero);
  }

  @Test
  public void testWidthAcceptsShortAndByte() {
    Assert.assertEquals(
        "Short types should be usable for the width field",
        0L,
        scalarSql("SELECT system.truncate(5S, 1L)"));

    Assert.assertEquals(
        "Byte types should be allowed for the width field",
        0,
        scalarSql("SELECT system.truncate(5Y, 1)"));
  }

  @Test
  public void testWrongNumberOfArguments() {
    Assertions.assertThatThrownBy(() -> scalarSql("SELECT system.truncate()"))
        .as("Function resolution should not work with zero arguments")
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "Function 'truncate' cannot process input: (): Wrong number of inputs (expected width and value)");

    Assertions.assertThatThrownBy(() -> scalarSql("SELECT system.truncate(1)"))
        .as("Function resolution should not work with only one argument")
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "Function 'truncate' cannot process input: (int): Wrong number of inputs (expected width and value)");

    Assertions.assertThatThrownBy(() -> scalarSql("SELECT system.truncate(1, 1L, 1)"))
        .as("Function resolution should not work with more than two arguments")
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "Function 'truncate' cannot process input: (int, bigint, int): Wrong number of inputs (expected width and value)");
  }

  @Test
  public void testInvalidTypesCannotBeUsedForWidth() {
    Assertions.assertThatThrownBy(
            () -> scalarSql("SELECT system.truncate(CAST('12.34' as DECIMAL(9, 2)), 10)"))
        .as("Decimal type should not be coercible to the width field")
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "Function 'truncate' cannot process input: (decimal(9,2), int): Expected truncation width to be tinyint, shortint or int");

    Assertions.assertThatThrownBy(() -> scalarSql("SELECT system.truncate('5', 10)"))
        .as("String type should not be coercible to the width field")
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "Function 'truncate' cannot process input: (string, int): Expected truncation width to be tinyint, shortint or int");

    Assertions.assertThatThrownBy(
            () -> scalarSql("SELECT system.truncate(INTERVAL '100-00' YEAR TO MONTH, 10)"))
        .as("Interval year to month type should not be coercible to the width field")
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "Function 'truncate' cannot process input: (interval year to month, int): Expected truncation width to be tinyint, shortint or int");

    Assertions.assertThatThrownBy(
            () ->
                scalarSql(
                    "SELECT system.truncate(CAST('11 23:4:0' AS INTERVAL DAY TO SECOND), 10)"))
        .as("Interval day-time type should not be coercible to the width field")
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "Function 'truncate' cannot process input: (interval day to second, int): Expected truncation width to be tinyint, shortint or int");
  }

  @Test
  public void testInvalidTypesForTruncationColumn() {
    Assertions.assertThatThrownBy(
            () -> scalarSql("SELECT system.truncate(10, cast(12.3456 as float))"))
        .as("FLoat type should not be truncatable")
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "Function 'truncate' cannot process input: (int, float): Expected truncation col to be tinyint, shortint, int, bigint, decimal, string, or binary");

    Assertions.assertThatThrownBy(
            () -> scalarSql("SELECT system.truncate(10, cast(12.3456 as double))"))
        .as("Double type should not be truncatable")
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "Function 'truncate' cannot process input: (int, double): Expected truncation col to be tinyint, shortint, int, bigint, decimal, string, or binary");

    Assertions.assertThatThrownBy(() -> scalarSql("SELECT system.truncate(10, true)"))
        .as("Boolean type should not be truncatable")
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "Function 'truncate' cannot process input: (int, boolean): Expected truncation col to be tinyint, shortint, int, bigint, decimal, string, or binary");

    Assertions.assertThatThrownBy(() -> scalarSql("SELECT system.truncate(10, map(1, 1))"))
        .as("Map types should not be truncatable")
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "Function 'truncate' cannot process input: (int, map<int,int>): Expected truncation col to be tinyint, shortint, int, bigint, decimal, string, or binary");

    Assertions.assertThatThrownBy(() -> scalarSql("SELECT system.truncate(10, array(1L))"))
        .as("Array types should not be truncatable")
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "Function 'truncate' cannot process input: (int, array<bigint>): Expected truncation col to be tinyint, shortint, int, bigint, decimal, string, or binary");

    Assertions.assertThatThrownBy(
            () -> scalarSql("SELECT system.truncate(10, INTERVAL '100-00' YEAR TO MONTH)"))
        .as("Interval year-to-month type should not be truncatable")
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "Function 'truncate' cannot process input: (int, interval year to month): Expected truncation col to be tinyint, shortint, int, bigint, decimal, string, or binary");

    Assertions.assertThatThrownBy(
            () ->
                scalarSql(
                    "SELECT system.truncate(10, CAST('11 23:4:0' AS INTERVAL DAY TO SECOND))"))
        .as("Interval day-time type should not be truncatable")
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "Function 'truncate' cannot process input: (int, interval day to second): Expected truncation col to be tinyint, shortint, int, bigint, decimal, string, or binary");
  }

  @Test
  public void testMagicFunctionsResolveForTinyIntAndSmallIntWidths() {
    // Magic functions have staticinvoke in the explain output. Nonmagic calls use
    // applyfunctionexpression instead.
    String tinyIntWidthExplain =
        (String) scalarSql("EXPLAIN EXTENDED SELECT system.truncate(1Y, 6)");
    Assertions.assertThat(tinyIntWidthExplain)
        .contains("cast(1 as int)")
        .contains(
            "staticinvoke(class org.apache.iceberg.spark.functions.TruncateFunction$TruncateInt");

    String smallIntWidth = (String) scalarSql("EXPLAIN EXTENDED SELECT system.truncate(5S, 6L)");
    Assertions.assertThat(smallIntWidth)
        .contains("cast(5 as int)")
        .contains(
            "staticinvoke(class org.apache.iceberg.spark.functions.TruncateFunction$TruncateBigInt");
  }

  @Test
  public void testThatMagicFunctionsAreInvoked() {
    // Magic functions have `staticinvoke` in the explain output.
    // Non-magic calls have `applyfunctionexpression` instead.

    // TinyInt
    Assertions.assertThat(scalarSql("EXPLAIN EXTENDED select system.truncate(5, 6Y)"))
        .asString()
        .isNotNull()
        .contains(
            "staticinvoke(class org.apache.iceberg.spark.functions.TruncateFunction$TruncateTinyInt");

    // SmallInt
    Assertions.assertThat(scalarSql("EXPLAIN EXTENDED select system.truncate(5, 6S)"))
        .asString()
        .isNotNull()
        .contains(
            "staticinvoke(class org.apache.iceberg.spark.functions.TruncateFunction$TruncateSmallInt");

    // Int
    Assertions.assertThat(scalarSql("EXPLAIN EXTENDED select system.truncate(5, 6)"))
        .asString()
        .isNotNull()
        .contains(
            "staticinvoke(class org.apache.iceberg.spark.functions.TruncateFunction$TruncateInt");

    // Long
    Assertions.assertThat(scalarSql("EXPLAIN EXTENDED SELECT system.truncate(5, 6L)"))
        .asString()
        .isNotNull()
        .contains(
            "staticinvoke(class org.apache.iceberg.spark.functions.TruncateFunction$TruncateBigInt");

    // String
    Assertions.assertThat(scalarSql("EXPLAIN EXTENDED SELECT system.truncate(5, 'abcdefg')"))
        .asString()
        .isNotNull()
        .contains(
            "staticinvoke(class org.apache.iceberg.spark.functions.TruncateFunction$TruncateString");

    // Decimal
    Assertions.assertThat(scalarSql("EXPLAIN EXTENDED SELECT system.truncate(5, 12.34)"))
        .asString()
        .isNotNull()
        .contains(
            "staticinvoke(class org.apache.iceberg.spark.functions.TruncateFunction$TruncateDecimal");

    // Binary
    Assertions.assertThat(
            scalarSql("EXPLAIN EXTENDED SELECT system.truncate(4, X'0102030405060708')"))
        .asString()
        .isNotNull()
        .contains(
            "staticinvoke(class org.apache.iceberg.spark.functions.TruncateFunction$TruncateBinary");
  }

  private String asBytesLiteral(String value) {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    return "X'" + BaseEncoding.base16().encode(bytes) + "'";
  }
}
