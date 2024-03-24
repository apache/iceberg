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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import org.apache.iceberg.relocated.com.google.common.io.BaseEncoding;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.spark.sql.AnalysisException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestSparkTruncateFunction extends TestBaseWithCatalog {

  @BeforeEach
  public void useCatalog() {
    sql("USE %s", catalogName);
  }

  @TestTemplate
  public void testTruncateTinyInt() {
    assertThat(scalarSql("SELECT system.truncate(10, 0Y)")).isEqualTo((byte) 0);
    assertThat(scalarSql("SELECT system.truncate(10, 1Y)")).isEqualTo((byte) 0);
    assertThat(scalarSql("SELECT system.truncate(10, 5Y)")).isEqualTo((byte) 0);
    assertThat(scalarSql("SELECT system.truncate(10, 9Y)")).isEqualTo((byte) 0);
    assertThat(scalarSql("SELECT system.truncate(10, 10Y)")).isEqualTo((byte) 10);
    assertThat(scalarSql("SELECT system.truncate(10, 11Y)")).isEqualTo((byte) 10);
    assertThat(scalarSql("SELECT system.truncate(10, -1Y)")).isEqualTo((byte) -10);
    assertThat(scalarSql("SELECT system.truncate(10, -5Y)")).isEqualTo((byte) -10);
    assertThat(scalarSql("SELECT system.truncate(10, -10Y)")).isEqualTo((byte) -10);
    assertThat(scalarSql("SELECT system.truncate(10, -11Y)")).isEqualTo((byte) -20);

    // Check that different widths can be used
    assertThat(scalarSql("SELECT system.truncate(2, -1Y)")).isEqualTo((byte) -2);

    assertThat(scalarSql("SELECT system.truncate(2, CAST(null AS tinyint))"))
        .as("Null input should return null")
        .isNull();
  }

  @TestTemplate
  public void testTruncateSmallInt() {
    assertThat(scalarSql("SELECT system.truncate(10, 0S)")).isEqualTo((short) 0);
    assertThat(scalarSql("SELECT system.truncate(10, 1S)")).isEqualTo((short) 0);
    assertThat(scalarSql("SELECT system.truncate(10, 5S)")).isEqualTo((short) 0);
    assertThat(scalarSql("SELECT system.truncate(10, 9S)")).isEqualTo((short) 0);
    assertThat(scalarSql("SELECT system.truncate(10, 10S)")).isEqualTo((short) 10);
    assertThat(scalarSql("SELECT system.truncate(10, 11S)")).isEqualTo((short) 10);
    assertThat(scalarSql("SELECT system.truncate(10, -1S)")).isEqualTo((short) -10);
    assertThat(scalarSql("SELECT system.truncate(10, -5S)")).isEqualTo((short) -10);
    assertThat(scalarSql("SELECT system.truncate(10, -10S)")).isEqualTo((short) -10);
    assertThat(scalarSql("SELECT system.truncate(10, -11S)")).isEqualTo((short) -20);

    // Check that different widths can be used
    assertThat(scalarSql("SELECT system.truncate(2, -1S)")).isEqualTo((short) -2);

    assertThat(scalarSql("SELECT system.truncate(2, CAST(null AS smallint))"))
        .as("Null input should return null")
        .isNull();
  }

  @TestTemplate
  public void testTruncateInt() {
    assertThat(scalarSql("SELECT system.truncate(10, 0)")).isEqualTo(0);
    assertThat(scalarSql("SELECT system.truncate(10, 1)")).isEqualTo(0);
    assertThat(scalarSql("SELECT system.truncate(10, 5)")).isEqualTo(0);
    assertThat(scalarSql("SELECT system.truncate(10, 9)")).isEqualTo(0);
    assertThat(scalarSql("SELECT system.truncate(10, 10)")).isEqualTo(10);
    assertThat(scalarSql("SELECT system.truncate(10, 11)")).isEqualTo(10);
    assertThat(scalarSql("SELECT system.truncate(10, -1)")).isEqualTo(-10);
    assertThat(scalarSql("SELECT system.truncate(10, -5)")).isEqualTo(-10);
    assertThat(scalarSql("SELECT system.truncate(10, -10)")).isEqualTo(-10);
    assertThat(scalarSql("SELECT system.truncate(10, -11)")).isEqualTo(-20);

    // Check that different widths can be used
    assertThat(scalarSql("SELECT system.truncate(2, -1)")).isEqualTo(-2);
    assertThat(scalarSql("SELECT system.truncate(300, 1)")).isEqualTo(0);

    assertThat(scalarSql("SELECT system.truncate(2, CAST(null AS int))"))
        .as("Null input should return null")
        .isNull();
  }

  @TestTemplate
  public void testTruncateBigInt() {
    assertThat(scalarSql("SELECT system.truncate(10, 0L)")).isEqualTo(0L);
    assertThat(scalarSql("SELECT system.truncate(10, 1L)")).isEqualTo(0L);
    assertThat(scalarSql("SELECT system.truncate(10, 5L)")).isEqualTo(0L);
    assertThat(scalarSql("SELECT system.truncate(10, 9L)")).isEqualTo(0L);
    assertThat(scalarSql("SELECT system.truncate(10, 10L)")).isEqualTo(10L);
    assertThat(scalarSql("SELECT system.truncate(10, 11L)")).isEqualTo(10L);
    assertThat(scalarSql("SELECT system.truncate(10, -1L)")).isEqualTo(-10L);
    assertThat(scalarSql("SELECT system.truncate(10, -5L)")).isEqualTo(-10L);
    assertThat(scalarSql("SELECT system.truncate(10, -10L)")).isEqualTo(-10L);
    assertThat(scalarSql("SELECT system.truncate(10, -11L)")).isEqualTo(-20L);

    // Check that different widths can be used
    assertThat(scalarSql("SELECT system.truncate(2, -1L)")).isEqualTo(-2L);

    assertThat(scalarSql("SELECT system.truncate(2, CAST(null AS bigint))"))
        .as("Null input should return null")
        .isNull();
  }

  @TestTemplate
  public void testTruncateDecimal() {
    // decimal truncation works by applying the decimal scale to the width: ie 10 scale 2 = 0.10
    assertThat(scalarSql("SELECT system.truncate(10, CAST(%s as DECIMAL(9, 2)))", "12.34"))
        .isEqualTo(new BigDecimal("12.30"));

    assertThat(scalarSql("SELECT system.truncate(10, CAST(%s as DECIMAL(9, 2)))", "12.30"))
        .isEqualTo(new BigDecimal("12.30"));

    assertThat(scalarSql("SELECT system.truncate(10, CAST(%s as DECIMAL(9, 3)))", "12.299"))
        .isEqualTo(new BigDecimal("12.290"));

    assertThat(scalarSql("SELECT system.truncate(3, CAST(%s as DECIMAL(5, 2)))", "0.05"))
        .isEqualTo(new BigDecimal("0.03"));

    assertThat(scalarSql("SELECT system.truncate(10, CAST(%s as DECIMAL(9, 2)))", "0.05"))
        .isEqualTo(new BigDecimal("0.00"));

    assertThat(scalarSql("SELECT system.truncate(10, CAST(%s as DECIMAL(9, 2)))", "-0.05"))
        .isEqualTo(new BigDecimal("-0.10"));

    assertThat(scalarSql("SELECT system.truncate(10, 12345.3482)"))
        .as("Implicit decimal scale and precision should be allowed")
        .isEqualTo(new BigDecimal("12345.3480"));

    BigDecimal truncatedDecimal =
        (BigDecimal) scalarSql("SELECT system.truncate(10, CAST(%s as DECIMAL(6, 4)))", "-0.05");
    assertThat(truncatedDecimal.scale())
        .as("Truncating a decimal should return a decimal with the same scale")
        .isEqualTo(4);

    assertThat(truncatedDecimal)
        .as("Truncating a decimal should return a decimal with the correct scale")
        .isEqualTo(BigDecimal.valueOf(-500, 4));

    assertThat(scalarSql("SELECT system.truncate(2, CAST(null AS decimal))"))
        .as("Null input should return null")
        .isNull();
  }

  @SuppressWarnings("checkstyle:AvoidEscapedUnicodeCharacters")
  @TestTemplate
  public void testTruncateString() {
    assertThat(scalarSql("SELECT system.truncate(5, 'abcdefg')"))
        .as("Should system.truncate strings longer than length")
        .isEqualTo("abcde");

    assertThat(scalarSql("SELECT system.truncate(5, 'abc')"))
        .as("Should not pad strings shorter than length")
        .isEqualTo("abc");

    assertThat(scalarSql("SELECT system.truncate(5, 'abcde')"))
        .as("Should not alter strings equal to length")
        .isEqualTo("abcde");

    assertThat(scalarSql("SELECT system.truncate(2, 'イロハニホヘト')"))
        .as("Strings with multibyte unicode characters should truncate along codepoint boundaries")
        .isEqualTo("イロ");

    assertThat(scalarSql("SELECT system.truncate(3, 'イロハニホヘト')"))
        .as("Strings with multibyte unicode characters should truncate along codepoint boundaries")
        .isEqualTo("イロハ");

    assertThat(scalarSql("SELECT system.truncate(7, 'イロハニホヘト')"))
        .as(
            "Strings with multibyte unicode characters should not alter input with fewer codepoints than width")
        .isEqualTo("イロハニホヘト");

    String stringWithTwoCodePointsEachFourBytes = "\uD800\uDC00\uD800\uDC00";
    assertThat(scalarSql("SELECT system.truncate(1, '%s')", stringWithTwoCodePointsEachFourBytes))
        .as("String truncation on four byte codepoints should work as expected")
        .isEqualTo("\uD800\uDC00");

    assertThat(scalarSql("SELECT system.truncate(1, '测试')"))
        .as("Should handle three-byte UTF-8 characters appropriately")
        .isEqualTo("测");

    assertThat(scalarSql("SELECT system.truncate(4, '测试raul试测')"))
        .as("Should handle three-byte UTF-8 characters mixed with two byte utf-8 characters")
        .isEqualTo("测试ra");

    assertThat(scalarSql("SELECT system.truncate(10, '')"))
        .as("Should not fail on the empty string")
        .isEqualTo("");

    assertThat(scalarSql("SELECT system.truncate(3, CAST(null AS string))"))
        .as("Null input should return null as output")
        .isNull();

    assertThat(scalarSql("SELECT system.truncate(4, CAST('测试raul试测' AS varchar(8)))"))
        .as("Varchar should work like string")
        .isEqualTo("测试ra");

    assertThat(scalarSql("SELECT system.truncate(4, CAST('测试raul试测' AS char(8)))"))
        .as("Char should work like string")
        .isEqualTo("测试ra");
  }

  @TestTemplate
  public void testTruncateBinary() {
    assertThat((byte[]) scalarSql("SELECT system.truncate(10, X'0102030405060708090a0b0c0d0e0f')"))
        .isEqualTo(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});

    assertThat((byte[]) scalarSql("SELECT system.truncate(3, %s)", asBytesLiteral("abcdefg")))
        .as("Should return the same input when value is equal to truncation width")
        .isEqualTo("abc".getBytes(StandardCharsets.UTF_8));

    assertThat((byte[]) scalarSql("SELECT system.truncate(10, %s)", asBytesLiteral("abc\0\0")))
        .as("Should not truncate, pad, or trim the input when its length is less than the width")
        .isEqualTo("abc\0\0".getBytes(StandardCharsets.UTF_8));

    assertThat((byte[]) scalarSql("SELECT system.truncate(3, %s)", asBytesLiteral("abc")))
        .as("Should not pad the input when its length is equal to the width")
        .isEqualTo("abc".getBytes(StandardCharsets.UTF_8));

    assertThat((byte[]) scalarSql("SELECT system.truncate(6, %s)", asBytesLiteral("测试_")))
        .as("Should handle three-byte UTF-8 characters appropriately")
        .isEqualTo("测试".getBytes(StandardCharsets.UTF_8));

    assertThat(scalarSql("SELECT system.truncate(3, CAST(null AS binary))"))
        .as("Null input should return null as output")
        .isNull();
  }

  @TestTemplate
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
    assertThat(numNonZero)
        .as("A truncate function with variable widths should be usable on dataframe columns")
        .isEqualTo(rumRows);
  }

  @TestTemplate
  public void testWidthAcceptsShortAndByte() {
    assertThat(scalarSql("SELECT system.truncate(5S, 1L)"))
        .as("Short types should be usable for the width field")
        .isEqualTo(0L);

    assertThat(scalarSql("SELECT system.truncate(5Y, 1)"))
        .as("Byte types should be allowed for the width field")
        .isEqualTo(0);
  }

  @TestTemplate
  public void testWrongNumberOfArguments() {
    assertThatThrownBy(() -> scalarSql("SELECT system.truncate()"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'truncate' cannot process input: (): Wrong number of inputs (expected width and value)");

    assertThatThrownBy(() -> scalarSql("SELECT system.truncate(1)"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'truncate' cannot process input: (int): Wrong number of inputs (expected width and value)");

    assertThatThrownBy(() -> scalarSql("SELECT system.truncate(1, 1L, 1)"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'truncate' cannot process input: (int, bigint, int): Wrong number of inputs (expected width and value)");
  }

  @TestTemplate
  public void testInvalidTypesCannotBeUsedForWidth() {
    assertThatThrownBy(
            () -> scalarSql("SELECT system.truncate(CAST('12.34' as DECIMAL(9, 2)), 10)"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'truncate' cannot process input: (decimal(9,2), int): Expected truncation width to be tinyint, shortint or int");

    assertThatThrownBy(() -> scalarSql("SELECT system.truncate('5', 10)"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'truncate' cannot process input: (string, int): Expected truncation width to be tinyint, shortint or int");

    assertThatThrownBy(
            () -> scalarSql("SELECT system.truncate(INTERVAL '100-00' YEAR TO MONTH, 10)"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'truncate' cannot process input: (interval year to month, int): Expected truncation width to be tinyint, shortint or int");

    assertThatThrownBy(
            () ->
                scalarSql(
                    "SELECT system.truncate(CAST('11 23:4:0' AS INTERVAL DAY TO SECOND), 10)"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'truncate' cannot process input: (interval day to second, int): Expected truncation width to be tinyint, shortint or int");
  }

  @TestTemplate
  public void testInvalidTypesForTruncationColumn() {
    assertThatThrownBy(() -> scalarSql("SELECT system.truncate(10, cast(12.3456 as float))"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'truncate' cannot process input: (int, float): Expected truncation col to be tinyint, shortint, int, bigint, decimal, string, or binary");

    assertThatThrownBy(() -> scalarSql("SELECT system.truncate(10, cast(12.3456 as double))"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'truncate' cannot process input: (int, double): Expected truncation col to be tinyint, shortint, int, bigint, decimal, string, or binary");

    assertThatThrownBy(() -> scalarSql("SELECT system.truncate(10, true)"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'truncate' cannot process input: (int, boolean): Expected truncation col to be tinyint, shortint, int, bigint, decimal, string, or binary");

    assertThatThrownBy(() -> scalarSql("SELECT system.truncate(10, map(1, 1))"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'truncate' cannot process input: (int, map<int,int>): Expected truncation col to be tinyint, shortint, int, bigint, decimal, string, or binary");

    assertThatThrownBy(() -> scalarSql("SELECT system.truncate(10, array(1L))"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'truncate' cannot process input: (int, array<bigint>): Expected truncation col to be tinyint, shortint, int, bigint, decimal, string, or binary");

    assertThatThrownBy(
            () -> scalarSql("SELECT system.truncate(10, INTERVAL '100-00' YEAR TO MONTH)"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'truncate' cannot process input: (int, interval year to month): Expected truncation col to be tinyint, shortint, int, bigint, decimal, string, or binary");

    assertThatThrownBy(
            () ->
                scalarSql(
                    "SELECT system.truncate(10, CAST('11 23:4:0' AS INTERVAL DAY TO SECOND))"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'truncate' cannot process input: (int, interval day to second): Expected truncation col to be tinyint, shortint, int, bigint, decimal, string, or binary");
  }

  @TestTemplate
  public void testMagicFunctionsResolveForTinyIntAndSmallIntWidths() {
    // Magic functions have staticinvoke in the explain output. Nonmagic calls use
    // applyfunctionexpression instead.
    String tinyIntWidthExplain =
        (String) scalarSql("EXPLAIN EXTENDED SELECT system.truncate(1Y, 6)");
    assertThat(tinyIntWidthExplain)
        .contains("cast(1 as int)")
        .contains(
            "staticinvoke(class org.apache.iceberg.spark.functions.TruncateFunction$TruncateInt");

    String smallIntWidth = (String) scalarSql("EXPLAIN EXTENDED SELECT system.truncate(5S, 6L)");
    assertThat(smallIntWidth)
        .contains("cast(5 as int)")
        .contains(
            "staticinvoke(class org.apache.iceberg.spark.functions.TruncateFunction$TruncateBigInt");
  }

  @TestTemplate
  public void testThatMagicFunctionsAreInvoked() {
    // Magic functions have `staticinvoke` in the explain output.
    // Non-magic calls have `applyfunctionexpression` instead.

    // TinyInt
    assertThat(scalarSql("EXPLAIN EXTENDED select system.truncate(5, 6Y)"))
        .asString()
        .isNotNull()
        .contains(
            "staticinvoke(class org.apache.iceberg.spark.functions.TruncateFunction$TruncateTinyInt");

    // SmallInt
    assertThat(scalarSql("EXPLAIN EXTENDED select system.truncate(5, 6S)"))
        .asString()
        .isNotNull()
        .contains(
            "staticinvoke(class org.apache.iceberg.spark.functions.TruncateFunction$TruncateSmallInt");

    // Int
    assertThat(scalarSql("EXPLAIN EXTENDED select system.truncate(5, 6)"))
        .asString()
        .isNotNull()
        .contains(
            "staticinvoke(class org.apache.iceberg.spark.functions.TruncateFunction$TruncateInt");

    // Long
    assertThat(scalarSql("EXPLAIN EXTENDED SELECT system.truncate(5, 6L)"))
        .asString()
        .isNotNull()
        .contains(
            "staticinvoke(class org.apache.iceberg.spark.functions.TruncateFunction$TruncateBigInt");

    // String
    assertThat(scalarSql("EXPLAIN EXTENDED SELECT system.truncate(5, 'abcdefg')"))
        .asString()
        .isNotNull()
        .contains(
            "staticinvoke(class org.apache.iceberg.spark.functions.TruncateFunction$TruncateString");

    // Decimal
    assertThat(scalarSql("EXPLAIN EXTENDED SELECT system.truncate(5, 12.34)"))
        .asString()
        .isNotNull()
        .contains(
            "staticinvoke(class org.apache.iceberg.spark.functions.TruncateFunction$TruncateDecimal");

    // Binary
    assertThat(scalarSql("EXPLAIN EXTENDED SELECT system.truncate(4, X'0102030405060708')"))
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
