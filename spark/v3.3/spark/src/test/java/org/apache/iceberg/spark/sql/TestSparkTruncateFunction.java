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
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.io.BaseEncoding;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkTestBaseWithCatalog;
import org.apache.spark.sql.AnalysisException;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSparkTruncateFunction extends SparkTestBaseWithCatalog {

  // TODO - Add tests for SparkCatalogConfig.SPARK once the `system` namespace is resolvable from the session catalog.
  @Parameterized.Parameters(name = "catalogConfig = {0}")
  public static Object[][] parameters() {
    return new Object[][]{
        {SparkCatalogConfig.HADOOP},
        {SparkCatalogConfig.HIVE}
    };
  }

  private static final Namespace SYSTEM = Namespace.of("system");

  private final String systemNamespace;
  private final boolean isSessionCatalog;

  public TestSparkTruncateFunction(SparkCatalogConfig catalogConfig) {
    super(catalogConfig);
    this.isSessionCatalog = "spark_catalog".equals(catalogName);
    this.systemNamespace = (isSessionCatalog ? "" : catalogName + ".") + SYSTEM;
  }

  @Test
  public void testTruncateUsingSystemNamespaceForNonSessionCatalogs() {
    // Non-session catalogs use v2 function resolution always
    Assume.assumeFalse(isSessionCatalog);

    Assert.assertEquals(
        "Should be able to use the truncate function with the system namespace, for non-session catalogs",
        5, scalarSql("SELECT %s.system.truncate(5, 6)", catalogName));
  }

  @Test
  @Ignore // TODO - Return to this once session catalog is supported.
  public void testTruncateUsingSystemNamespaceSessionCatalogs() {
    // Spark's Session catalog only allows using new functions from a registered namespace.
    Assume.assumeTrue(isSessionCatalog);

    Assert.assertEquals(
        "Should be able to call system.truncate from session catalog, provided that we're in an Iceberg namespace",
        5, scalarSql("SELECT %s.truncate(5, 6)", systemNamespace));

    // Note session catalog can only use special `system` namespace if it's not qualified with catalog or db name.
    AssertHelpers.assertThrows(
        "Session catalog cannot be qualified when using system",
        AnalysisException.class,
        "Undefined function",
        () -> scalarSql("SELECT spark_catalog.system.truncate(5, 6)")
    );

    AssertHelpers.assertThrows(
        "Session catalog only allows usage of system keyword when used on its own",
        AnalysisException.class,
        "Undefined function",
        () -> scalarSql("SELECT system.truncate(6, 5)")
    );
  }

  @Test
  public void testTruncateTinyInt() {
    Assert.assertEquals((byte) 0, scalarSql("SELECT %s.truncate(10, 0Y)", systemNamespace));
    Assert.assertEquals((byte) 0, scalarSql("SELECT %s.truncate(10, 1Y)", systemNamespace));
    Assert.assertEquals((byte) 0, scalarSql("SELECT %s.truncate(10, 5Y)", systemNamespace));
    Assert.assertEquals((byte) 0, scalarSql("SELECT %s.truncate(10, 9Y)", systemNamespace));
    Assert.assertEquals((byte) 10, scalarSql("SELECT %s.truncate(10, 10Y)", systemNamespace));
    Assert.assertEquals((byte) 10, scalarSql("SELECT %s.truncate(10, 11Y)", systemNamespace));
    Assert.assertEquals((byte) -10, scalarSql("SELECT %s.truncate(10, -1Y)", systemNamespace));
    Assert.assertEquals((byte) -10, scalarSql("SELECT %s.truncate(10, -5Y)", systemNamespace));
    Assert.assertEquals((byte) -10, scalarSql("SELECT %s.truncate(10, -10Y)", systemNamespace));
    Assert.assertEquals((byte) -20, scalarSql("SELECT %s.truncate(10, -11Y)", systemNamespace));

    // Check that different widths can be used
    Assert.assertEquals((byte) -2, scalarSql("SELECT %s.truncate(2, -1Y)", systemNamespace));

    // Check that tinyint types are allowed for the width
    Assert.assertEquals((byte) 0, scalarSql("SELECT %s.truncate(5Y, 1Y)", systemNamespace));

    Assert.assertEquals("Null input should return null",
        null, scalarSql("SELECT %s.truncate(2, CAST(null AS tinyint))", systemNamespace));
  }

  @Test
  public void testTruncateSmallInt() {
    Assert.assertEquals((short) 0, scalarSql("SELECT %s.truncate(10, 0S)", systemNamespace));
    Assert.assertEquals((short) 0, scalarSql("SELECT %s.truncate(10, 1S)", systemNamespace));
    Assert.assertEquals((short) 0, scalarSql("SELECT %s.truncate(10, 5S)", systemNamespace));
    Assert.assertEquals((short) 0, scalarSql("SELECT %s.truncate(10, 9S)", systemNamespace));
    Assert.assertEquals((short) 10, scalarSql("SELECT %s.truncate(10, 10S)", systemNamespace));
    Assert.assertEquals((short) 10, scalarSql("SELECT %s.truncate(10, 11S)", systemNamespace));
    Assert.assertEquals((short) -10, scalarSql("SELECT %s.truncate(10, -1S)", systemNamespace));
    Assert.assertEquals((short) -10, scalarSql("SELECT %s.truncate(10, -5S)", systemNamespace));
    Assert.assertEquals((short) -10, scalarSql("SELECT %s.truncate(10, -10S)", systemNamespace));
    Assert.assertEquals((short) -20, scalarSql("SELECT %s.truncate(10, -11S)", systemNamespace));

    // Check that different widths can be used
    Assert.assertEquals((short) -2, scalarSql("SELECT %s.truncate(2, -1S)", systemNamespace));

    // Check that short types are allowed for the width
    Assert.assertEquals((short) 0, scalarSql("SELECT %s.truncate(5S, 1S)", systemNamespace));

    Assert.assertEquals("Null input should return null",
        null, scalarSql("SELECT %s.truncate(2, CAST(null AS smallint))", systemNamespace));
  }

  @Test
  public void testTruncateIntegerLiteralSQL() {
    Assert.assertEquals(0, scalarSql("SELECT %s.truncate(10, %d)", systemNamespace, 0));
    Assert.assertEquals(0, scalarSql("SELECT %s.truncate(10, %d)", systemNamespace, 1));
    Assert.assertEquals(0, scalarSql("SELECT %s.truncate(10, %d)", systemNamespace, 5));
    Assert.assertEquals(0, scalarSql("SELECT %s.truncate(10, %d)", systemNamespace, 9));
    Assert.assertEquals(10, scalarSql("SELECT %s.truncate(10, %d)", systemNamespace, 10));
    Assert.assertEquals(10, scalarSql("SELECT %s.truncate(10, %d)", systemNamespace, 11));
    Assert.assertEquals(-10, scalarSql("SELECT %s.truncate(10, %d)", systemNamespace, -1));
    Assert.assertEquals(-10, scalarSql("SELECT %s.truncate(10, %d)", systemNamespace, -5));
    Assert.assertEquals(-10, scalarSql("SELECT %s.truncate(10, %d)", systemNamespace, -10));
    Assert.assertEquals(-20, scalarSql("SELECT %s.truncate(10, %d)", systemNamespace, -11));

    // Check that different widths can be used
    Assert.assertEquals(-2, scalarSql("SELECT %s.truncate(2, %d)", systemNamespace, -1));
    Assert.assertEquals(0, scalarSql("SELECT %s.truncate(2, %d)", systemNamespace, 1));

    Assert.assertEquals("Null input should return null",
        null, scalarSql("SELECT %s.truncate(2, CAST(null AS int))", systemNamespace));
  }

  @Test
  public void testTruncationWidthFailsWhenNonnegative() {
    AssertHelpers.assertThrows(
        "Non-positive truncation width for integers should throw an exception at runtime",
        IllegalArgumentException.class,
        "Invalid truncate width",
        () -> scalarSql("SELECT %s.truncate(0, 10)", systemNamespace)
    );

    AssertHelpers.assertThrows(
        "Non-positive truncation width for longs should throw an exception at runtime",
        IllegalArgumentException.class,
        "Invalid truncate width",
        () -> scalarSql("SELECT %s.truncate(-1, 10L)", systemNamespace)
    );

    AssertHelpers.assertThrows(
        "Non-positive truncation width for decimals should throw an exception at runtime",
        IllegalArgumentException.class,
        "Invalid truncate width",
        () -> scalarSql("SELECT %s.truncate(0, CAST(12.34 AS decimal(9, 2)))", systemNamespace)
    );

    AssertHelpers.assertThrows(
        "Non-positive truncation width for binary types should throw an exception at runtime",
        IllegalArgumentException.class,
        "Invalid truncate width",
        () -> scalarSql("SELECT %s.truncate(-1, X'01020304')", systemNamespace)
    );
  }

  @Test
  public void testTruncateBigInt() {
    Assert.assertEquals(0L, scalarSql("SELECT %s.truncate(10, 0L)", systemNamespace));
    Assert.assertEquals(0L, scalarSql("SELECT %s.truncate(10, 1L)", systemNamespace));
    Assert.assertEquals(0L, scalarSql("SELECT %s.truncate(10, 5L)", systemNamespace));
    Assert.assertEquals(0L, scalarSql("SELECT %s.truncate(10, 9L)", systemNamespace));
    Assert.assertEquals(10L, scalarSql("SELECT %s.truncate(10, 10L)", systemNamespace));
    Assert.assertEquals(10L, scalarSql("SELECT %s.truncate(10, 11L)", systemNamespace));
    Assert.assertEquals(-10L, scalarSql("SELECT %s.truncate(10, -1L)", systemNamespace));
    Assert.assertEquals(-10L, scalarSql("SELECT %s.truncate(10, -5L)", systemNamespace));
    Assert.assertEquals(-10L, scalarSql("SELECT %s.truncate(10, -10L)", systemNamespace));
    Assert.assertEquals(-20L, scalarSql("SELECT %s.truncate(10, -11L)", systemNamespace));

    // Check that different widths can be used
    Assert.assertEquals(-2L, scalarSql("SELECT %s.truncate(2, -1L)", systemNamespace));

    Assert.assertEquals("Null input should return null",
        null, scalarSql("SELECT %s.truncate(2, CAST(null AS bigint))", systemNamespace));
  }

  @Test
  public void testTruncateDecimalLiteralSQL() {
    // decimal truncation works by applying the decimal scale to the width: ie 10 scale 2 = 0.10
    Assert.assertEquals(new BigDecimal("12.30"),
        scalarSql("SELECT %s.truncate(10, CAST(%f as DECIMAL(9, 2)))", systemNamespace, 12.34));

    Assert.assertEquals(new BigDecimal("12.30"),
        scalarSql("SELECT %s.truncate(10, CAST(%f as DECIMAL(9, 2)))", systemNamespace, 12.30));

    Assert.assertEquals(new BigDecimal("12.290"),
        scalarSql("SELECT %s.truncate(10, CAST(%f as DECIMAL(9, 3)))", systemNamespace, 12.299));

    Assert.assertEquals(new BigDecimal("0.03"),
        scalarSql("SELECT %s.truncate(3, CAST(%f as DECIMAL(5, 2)))", systemNamespace, 0.05));

    Assert.assertEquals(new BigDecimal("0.00"),
        scalarSql("SELECT %s.truncate(10, CAST(%f as DECIMAL(9, 2)))", systemNamespace, 0.05));

    Assert.assertEquals(new BigDecimal("-0.10"),
        scalarSql("SELECT %s.truncate(10, CAST(%f as DECIMAL(9, 2)))", systemNamespace, -0.05));

    Assert.assertEquals("Implicit decimal scale and precision should be allowed",
        new BigDecimal("12345.3480"), scalarSql("SELECT %s.truncate(10, 12345.3482)", systemNamespace));

    Assert.assertEquals("Null input should return null",
        null, scalarSql("SELECT %s.truncate(2, CAST(null AS decimal))", systemNamespace));
  }

  @Test
  public void testInvalidTypesForWidthFailFunctionBinding() {
    AssertHelpers.assertThrows(
        "Decimal type should not be coercible to the width field",
        AnalysisException.class,
        "Expected truncation width to be one of [ByteType, ShortType, IntegerType]",
        () -> scalarSql("SELECT %s.truncate(CAST(12.34 as DECIMAL(9, 2)), 10)", systemNamespace, 12.34)
    );

    AssertHelpers.assertThrows(
        "String type should not be coercible to the width field",
        AnalysisException.class,
        "Expected truncation width to be one of [ByteType, ShortType, IntegerType]",
        () -> scalarSql("SELECT %s.truncate('5', 10)", systemNamespace)
    );

    AssertHelpers.assertThrows(
        "Interval year to month  type should not be coercible to the width field",
        AnalysisException.class,
        "Expected truncation width to be one of [ByteType, ShortType, IntegerType]",
        () -> scalarSql("SELECT %s.truncate(INTERVAL '100-00' YEAR TO MONTH, 10)", systemNamespace)
    );

    AssertHelpers.assertThrows(
        "Interval day-time type should not be coercible to the width field",
        AnalysisException.class,
        "Expected truncation width to be one of [ByteType, ShortType, IntegerType]",
        () -> scalarSql("SELECT %s.truncate(CAST('11 23:4:0' AS INTERVAL DAY TO SECOND), 10)", systemNamespace)
    );
  }

  @Test
  public void testTruncateString() {
    Assert.assertEquals("Should system.truncate strings longer than length",
        "abcde", scalarSql("SELECT %s.truncate(5, 'abcdefg')", systemNamespace));
    Assert.assertEquals("Should not pad strings shorter than length",
        "abc", scalarSql("SELECT %s.truncate(5, 'abc')", systemNamespace));
    Assert.assertEquals("Should not alter strings equal to length",
        "abcde", scalarSql("SELECT %s.truncate(5, 'abcde')", systemNamespace));
    Assert.assertEquals("Should handle three-byte UTF-8 characters appropriately",
        "测",
        scalarSql("SELECT %s.truncate(1, '测试')", systemNamespace));
    Assert.assertEquals("Should handle three-byte UTF-8 characters mixed with two byte utf-8 characters",
        "测试ra",
        scalarSql("SELECT %s.truncate(4, '测试raul试测')", systemNamespace));

    Assert.assertEquals("Null input should return null as output",
        null, scalarSql("SELECT %s.truncate(3, CAST(null AS string))", systemNamespace));

    Assert.assertEquals("Varchar should work like string",
        "测试ra", scalarSql("SELECT %s.truncate(4, CAST('测试raul试测' AS varchar(8)))", systemNamespace));

    Assert.assertEquals("Char should work like string",
        "测试ra", scalarSql("SELECT %s.truncate(4, CAST('测试raul试测' AS char(8)))", systemNamespace));
  }

  @Test
  public void testTruncateBinary() {
    Assert.assertArrayEquals(
        new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
        (byte[]) scalarSql("SELECT %s.truncate(10, X'0102030405060708090a0b0c0d0e0f')", systemNamespace));
    Assert.assertArrayEquals("Should return the same input when value is equal to truncation width",
        "abc".getBytes(StandardCharsets.UTF_8),
        (byte[]) scalarSql("SELECT %s.truncate(3, %s)", systemNamespace, asBytesLiteral("abcdefg")));
    Assert.assertArrayEquals("Should not truncate, pad, or trim the input when its length is less than the width",
        "abc\0\0".getBytes(StandardCharsets.UTF_8),
        (byte[]) scalarSql("SELECT %s.truncate(10, %s)", systemNamespace, asBytesLiteral("abc\0\0")));
    Assert.assertArrayEquals("Should not pad the input when its length is equal to the width",
        "abc".getBytes(StandardCharsets.UTF_8),
        (byte[]) scalarSql("SELECT %s.truncate(3, %s)", systemNamespace, asBytesLiteral("abc")));
    Assert.assertArrayEquals("Should handle three-byte UTF-8 characters appropriately",
        "测试".getBytes(StandardCharsets.UTF_8),
        (byte[]) scalarSql("SELECT %s.truncate(6, %s)", systemNamespace, asBytesLiteral("测试_")));

    Assert.assertEquals("Null input should return null as output",
        null, scalarSql("SELECT %s.truncate(3, CAST(null AS binary))", systemNamespace));
  }

  @Test
  public void testTruncateUsingDataframeForWidthWithVaryingWidth() {
    // This situation is atypical but allowed. Typically width is static
    long rumRows = 10L;
    long numNonZero = spark.range(rumRows)
        .toDF("value")
        .selectExpr("CAST(value +1 AS INT) AS width", "value")
        .selectExpr(String.format("%s.truncate(width, value) as truncated_value", systemNamespace))
        .filter("truncated_value == 0")
        .count();
    Assert.assertEquals("A truncate function with variable widths should be usable on dataframe columns",
        rumRows, numNonZero);
  }

  @Test
  public void testThatMagicFunctionsAreInvoked() {
    // Magic functions have staticinvoke in the explain output. Nonmagic calls have applyfunctionexpression instead.
    // TinyInt
    Assertions.assertThat(scalarSql("EXPLAIN EXTENDED select %s.truncate(5, 6Y)", systemNamespace))
        .asString().isNotNull()
        .contains("staticinvoke(class org.apache.iceberg.spark.functions.TruncateFunction$TruncateTinyInt");

    // SmallInt
    Assertions.assertThat(scalarSql("EXPLAIN EXTENDED select %s.truncate(5, 6S)", systemNamespace))
        .asString().isNotNull()
        .contains("staticinvoke(class org.apache.iceberg.spark.functions.TruncateFunction$TruncateSmallInt");

    // Int
    Assertions.assertThat(scalarSql("EXPLAIN EXTENDED select %s.truncate(5, 6)", systemNamespace))
        .asString().isNotNull()
        .contains("staticinvoke(class org.apache.iceberg.spark.functions.TruncateFunction$TruncateInt");

    // Long
    Assertions.assertThat(scalarSql("EXPLAIN EXTENDED SELECT %s.truncate(5, 6L)", systemNamespace))
        .asString().isNotNull()
        .contains("staticinvoke(class org.apache.iceberg.spark.functions.TruncateFunction$TruncateBigInt");

    // String
    Assertions.assertThat(scalarSql("EXPLAIN EXTENDED SELECT %s.truncate(5, 'abcdefg')", systemNamespace))
        .asString().isNotNull()
        .contains("staticinvoke(class org.apache.iceberg.spark.functions.TruncateFunction$TruncateString");

    // Decimal
    Assertions.assertThat(scalarSql("EXPLAIN EXTENDED SELECT %s.truncate(5, 12.34)", systemNamespace))
        .asString().isNotNull()
        .contains("staticinvoke(class org.apache.iceberg.spark.functions.TruncateFunction$TruncateDecimal");

    // Binary
    Assertions.assertThat(scalarSql("EXPLAIN EXTENDED SELECT %s.truncate(4, X'0102030405060708')", systemNamespace))
        .asString().isNotNull()
        .contains("staticinvoke(class org.apache.iceberg.spark.functions.TruncateFunction$TruncateBinary");
  }

  @Test
  public void testMagicFunctionsResolveForTinyIntAndSmallIntWidths() {
    // Magic functions have staticinvoke in the explain output. Nonmagic calls use applyfunctionexpression instead.
    String tinyIntWidthExplain = (String) scalarSql("EXPLAIN EXTENDED SELECT %s.truncate(1Y, 6)", systemNamespace);
    Assertions.assertThat(tinyIntWidthExplain)
        .contains("cast(1 as int)")
        .contains("staticinvoke(class org.apache.iceberg.spark.functions.TruncateFunction$TruncateInt");

    String smallIntWidth = (String) scalarSql("EXPLAIN EXTENDED SELECT %s.truncate(5S, 6L)", systemNamespace);
    Assertions.assertThat(smallIntWidth)
        .contains("cast(5 as int)")
        .contains("staticinvoke(class org.apache.iceberg.spark.functions.TruncateFunction$TruncateBigInt");
  }

  private String asBytesLiteral(String value) {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    return "X'" + BaseEncoding.base16().encode(bytes) + "'";
  }
}
