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

package org.apache.iceberg.parquet;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;
import java.util.PrimitiveIterator;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.BoundaryOrder;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.ColumnIndexBuilder;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndexBuilder;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Assert;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.isNaN;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notNaN;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.notStartsWith;
import static org.apache.iceberg.expressions.Expressions.startsWith;
import static org.apache.parquet.internal.column.columnindex.BoundaryOrder.ASCENDING;
import static org.apache.parquet.internal.column.columnindex.BoundaryOrder.DESCENDING;
import static org.apache.parquet.internal.column.columnindex.BoundaryOrder.UNORDERED;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Types.optional;

public class TestColumnIndexFilter {
  /**
   * COPIED FROM org.apache.parquet.internal.filter2.columnindex.TestColumnIndexFilter
   **/
  private static final long TOTAL_ROW_COUNT = 30;
  private static final String INT_COL = "int_col";
  private static final String STR_COL = "str_col";
  private static final String NO_NANS = "no_nans";
  private static final String NO_CI = "no_ci";
  private static final String ALL_NULLS = "all_nulls";
  private static final String ALL_NANS = "all_nans";
  private static final String NOT_IN_FILE = "not_in_file";
  private static final ColumnIndex INT_COL_CI = new CIBuilder(optional(INT32).named(INT_COL), ASCENDING)
      .addPage(0, 1, 1)
      .addPage(1, 2, 6)
      .addPage(0, 7, 7)
      .addPage(1, 7, 10)
      .addPage(0, 11, 17)
      .addPage(0, 18, 23)
      .addPage(0, 24, 26)
      .build();
  private static final OffsetIndex INT_COL_OI = new OIBuilder()
      .addPage(1)
      .addPage(6)
      .addPage(2)
      .addPage(5)
      .addPage(7)
      .addPage(6)
      .addPage(3)
      .build();
  private static final ColumnIndex STR_COL_CI =
      new CIBuilder(optional(BINARY).as(stringType()).named(STR_COL), DESCENDING)
          .addPage(0, "Zulu", "Zulu")
          .addPage(0, "Whiskey", "Yankee")
          .addPage(1, "Tango", "Victor")
          .addNullPage(3)
          .addPage(0, "Oscar", "Sierra")
          .addPage(0, "Juliett", "November")
          .addPage(0, "Bravo", "India")
          .addPage(0, "Alfa", "Alfa")
          .build();
  private static final OffsetIndex STR_COL_OI = new OIBuilder()
      .addPage(1)
      .addPage(3)
      .addPage(4)
      .addPage(3)
      .addPage(5)
      .addPage(5)
      .addPage(8)
      .addPage(1)
      .build();
  private static final ColumnIndex NO_NANS_CI = new CIBuilder(optional(DOUBLE).named(NO_NANS), UNORDERED)
      .addPage(0, 2.03, 2.03)
      .addPage(0, 0.56, 8.71)
      .addPage(2, 3.14, 3.50)
      .addPage(0, 2.71, 9.99)
      .addPage(3, 0.36, 5.32)
      .addPage(0, 4.17, 7.95)
      .addNullPage(4)
      .build();
  private static final OffsetIndex NO_NANS_OI = new OIBuilder()
      .addPage(1)
      .addPage(5)
      .addPage(4)
      .addPage(6)
      .addPage(7)
      .addPage(3)
      .addPage(4)
      .build();
  private static final ColumnIndex NO_CI_CI = null;
  private static final OffsetIndex NO_CI_OI = new OIBuilder()
      .addPage(1)
      .addPage(3)
      .addPage(2)
      .addPage(1)
      .addPage(5)
      .addPage(4)
      .addPage(5)
      .addPage(7)
      .addPage(2)
      .build();
  private static final ColumnIndex ALL_NULLS_CI = new CIBuilder(optional(INT64).named(ALL_NULLS), ASCENDING)
      .addNullPage(1)
      .addNullPage(29)
      .build();
  private static final OffsetIndex ALL_NULLS_OI = new OIBuilder()
      .addPage(1)
      .addPage(29)
      .build();
  private static final ColumnIndex ALL_NANS_CI = new CIBuilder(optional(DOUBLE).named(ALL_NANS), UNORDERED)
      .addPage(1, Double.NaN, Double.NaN)
      .addPage(29, Double.NaN, Double.NaN)
      .build();
  private static final ColumnIndexStore STORE = new ColumnIndexStore() {
    @Override
    public ColumnIndex getColumnIndex(ColumnPath column) {
      switch (column.toDotString()) {
        case INT_COL:
          return INT_COL_CI;
        case STR_COL:
          return STR_COL_CI;
        case NO_NANS:
          return NO_NANS_CI;
        case NO_CI:
          return NO_CI_CI;
        case ALL_NULLS:
          return ALL_NULLS_CI;
        case ALL_NANS:
          return ALL_NANS_CI;
        default:
          return null;
      }
    }

    @Override
    public OffsetIndex getOffsetIndex(ColumnPath column) {
      switch (column.toDotString()) {
        case INT_COL:
          return INT_COL_OI;
        case STR_COL:
          return STR_COL_OI;
        case NO_NANS:
          return NO_NANS_OI;
        case NO_CI:
          return NO_CI_OI;
        case ALL_NULLS:
          return ALL_NULLS_OI;
        case ALL_NANS:
          return ALL_NANS_OI;
        default:
          throw new MissingOffsetIndexException(column);
      }
    }
  };
  private static final OffsetIndex ALL_NANS_OI = new OIBuilder()
      .addPage(1)
      .addPage(29)
      .build();
  /**
   * <pre>
   * row   int_col       str_col        no_nans        no_ci          all_nulls      all_nans
   *                                                 (no column index)
   *      ------0------  ------0------  ------0------  ------0------  ------0------  ------0------
   * 0.   1              Zulu           2.03                          null           NaN
   *      ------1------  ------1------  ------1------  ------1------  ------1------  ------1------
   * 1.   2              Yankee         4.67                          null           NaN
   * 2.   3              Xray           3.42                          null           NaN
   * 3.   4              Whiskey        8.71                          null           NaN
   *                     ------2------                 ------2------
   * 4.   5              Victor         0.56                          null           NaN
   * 5.   6              Uniform        4.30                          null           NaN
   *                                    ------2------  ------3------
   * 6.   null           null           null                          null           NaN
   *      ------2------                                ------4------
   * 7.   7              Tango          3.50                          null           NaN
   *                     ------3------
   * 8.   7              null           3.14                          null           NaN
   *      ------3------k
   * 9.   7              null           null                          null           NaN
   *                                    ------3------
   * 10.  null           null           9.99                          null           NaN
   *                     ------4------
   * 11.  8              Sierra         8.78                          null           NaN
   *                                                   ------5------
   * 12.  9              Romeo          9.56                          null           NaN
   * 13.  10             Quebec         2.71                          null           NaN
   *      ------4------
   * 14.  11             Papa           5.71                          null           NaN
   * 15.  12             Oscar          4.09                          null           NaN
   *                     ------5------  ------4------  ------6------
   * 16.  13             November       null                          null           NaN
   * 17.  14             Mike           null                          null           NaN
   * 18.  15             Lima           0.36                          null           NaN
   * 19.  16             Kilo           2.94                          null           NaN
   * 20.  17             Juliett        4.23                          null           NaN
   *      ------5------  ------6------                 ------7------
   * 21.  18             India          null                          null           NaN
   * 22.  19             Hotel          5.32                          null           NaN
   *                                    ------5------
   * 23.  20             Golf           4.17                          null           NaN
   * 24.  21             Foxtrot        7.92                          null           NaN
   * 25.  22             Echo           7.95                          null           NaN
   *                                   ------6------
   * 26.  23             Delta          null                          null           NaN
   *      ------6------
   * 27.  24             Charlie        null                          null           NaN
   *                                                   ------8------
   * 28.  25             Bravo          null                          null           NaN
   *                     ------7------
   * 29.  26             Alfa           null                          null           NaN
   * </pre>
   */

  private static final Schema SCHEMA = new Schema(
      Types.NestedField.optional(1, INT_COL, Types.IntegerType.get()),
      Types.NestedField.optional(2, STR_COL, Types.StringType.get()),
      Types.NestedField.optional(3, NO_NANS, Types.DoubleType.get()),
      Types.NestedField.optional(4, NO_CI, Types.IntegerType.get()),
      Types.NestedField.optional(5, ALL_NULLS, Types.LongType.get()),
      Types.NestedField.optional(6, ALL_NANS, Types.DoubleType.get()),
      Types.NestedField.optional(7, NOT_IN_FILE, Types.LongType.get())
  );
  private static final Schema SCHEMA_MISSING_COLUMN = new Schema(
      Types.NestedField.optional(1, INT_COL, Types.IntegerType.get()),
      Types.NestedField.optional(2, STR_COL, Types.StringType.get()),
      Types.NestedField.optional(3, NO_NANS, Types.DoubleType.get()),
      Types.NestedField.optional(4, NO_CI, Types.IntegerType.get()),
      Types.NestedField.optional(5, ALL_NULLS, Types.LongType.get()),
      Types.NestedField.optional(6, ALL_NANS, Types.DoubleType.get())
  );

  /**            END             **/

  private static final MessageType FILE_SCHEMA = ParquetSchemaUtil.convert(SCHEMA_MISSING_COLUMN, "table");
  private static final RowRanges ALL_ROWS = RowRanges.createSingle(TOTAL_ROW_COUNT);
  private static final RowRanges NO_ROWS = RowRanges.EMPTY;

  private static RowRanges createRowRanges(String path, Integer... pageIndexes) {
    return RowRanges.create(TOTAL_ROW_COUNT, new PrimitiveIterator.OfInt() {
      int index = -1;

      @Override
      public int nextInt() {
        return pageIndexes[index];
      }

      @Override
      public boolean hasNext() {
        index += 1;
        return index < pageIndexes.length;
      }
    }, STORE.getOffsetIndex(ColumnPath.fromDotString(path)));
  }

  private boolean rowRangesEquals(RowRanges r1, RowRanges r2) {
    if (r1 == r2) {
      return true;
    }

    if (r1 == null || r2 == null) {
      return false;
    }

    List<RowRanges.Range> ranges1 = r1.getRanges();
    List<RowRanges.Range> ranges2 = r2.getRanges();

    if (ranges1.size() != ranges2.size()) {
      return false;
    }

    for (int i = 0; i < ranges1.size(); i += 1) {
      RowRanges.Range range1 = ranges1.get(i);
      RowRanges.Range range2 = ranges2.get(i);
      if (range1.from != range2.from || range1.to != range2.to) {
        return false;
      }
    }

    return true;
  }

  private void assertRowRangesEquals(RowRanges expected, RowRanges actual) {
    if (!rowRangesEquals(expected, actual)) {
      throw new AssertionError(String.format("RowRanges are not equal, expected: %s, actual: %s",
          expected, actual));
    }
  }

  private RowRanges calculateRowRanges(Expression expr) {
    return calculateRowRanges(SCHEMA, expr, true);
  }

  private RowRanges calculateRowRanges(Expression expr, boolean caseSensitive) {
    return calculateRowRanges(SCHEMA, expr, caseSensitive);
  }

  private RowRanges calculateRowRanges(Schema schema, Expression expr, boolean caseSensitive) {
    return new ParquetColumnIndexFilter(schema, expr, caseSensitive)
        .calculateRowRanges(FILE_SCHEMA, STORE, TOTAL_ROW_COUNT);
  }

  private RowRanges calculateRowRanges(Schema schema, MessageType messageType, Expression expr, boolean caseSensitive) {
    return new ParquetColumnIndexFilter(schema, expr, caseSensitive)
            .calculateRowRanges(messageType, STORE, TOTAL_ROW_COUNT);
  }

  @Test
  public void testIsNulls() {
    RowRanges expected;

    expected = createRowRanges(INT_COL, 1, 3);
    assertRowRangesEquals(expected, calculateRowRanges(isNull(INT_COL)));

    expected = createRowRanges(STR_COL, 2, 3);
    assertRowRangesEquals(expected, calculateRowRanges(isNull(STR_COL)));

    expected = createRowRanges(NO_NANS, 2, 4, 6);
    assertRowRangesEquals(expected, calculateRowRanges(isNull(NO_NANS)));

    expected = ALL_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(isNull(ALL_NULLS)));
  }

  @Test
  public void testNotNulls() {
    RowRanges expected;

    expected = ALL_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(notNull(INT_COL)));

    expected = createRowRanges(STR_COL, 0, 1, 2, 4, 5, 6, 7);
    assertRowRangesEquals(expected, calculateRowRanges(notNull(STR_COL)));

    expected = createRowRanges(NO_NANS, 0, 1, 2, 3, 4, 5);
    assertRowRangesEquals(expected, calculateRowRanges(notNull(NO_NANS)));

    expected = NO_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(notNull(ALL_NULLS)));
  }

  @Test
  public void testIsNaN() {
    RowRanges expected;

    // column index exists, null page 6 should be filtered out
    expected = createRowRanges(NO_NANS, 0, 1, 2, 3, 4, 5);
    assertRowRangesEquals(expected, calculateRowRanges(isNaN(NO_NANS)));

    assertRowRangesEquals(ALL_ROWS, calculateRowRanges(isNaN(ALL_NANS)));
  }

  @Test
  public void testNotNaN() {
    RowRanges expected;

    expected = ALL_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(notNaN(NO_NANS)));

    assertRowRangesEquals(expected, calculateRowRanges(notNaN(ALL_NANS)));
  }

  @Test
  public void testMissingColumn() {
    Assert.assertThrows("Cannot find field 'missing'",
        ValidationException.class, () -> calculateRowRanges(equal("missing", 0)));
  }

  @Test
  public void testColumnNotInFile() {
    RowRanges expected;

    expected = NO_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(notNull(NOT_IN_FILE)));
  }

  @Test
  public void testMissingColumnIndex() {
    RowRanges expected = ALL_ROWS;

    assertRowRangesEquals(expected, calculateRowRanges(isNull(NO_CI)));
    assertRowRangesEquals(expected, calculateRowRanges(notNull(NO_CI)));
    assertRowRangesEquals(expected, calculateRowRanges(greaterThan(NO_CI, 9)));
    assertRowRangesEquals(expected, calculateRowRanges(lessThan(NO_CI, 9)));
    assertRowRangesEquals(expected, calculateRowRanges(equal(NO_CI, 9)));
    assertRowRangesEquals(expected, calculateRowRanges(notEqual(NO_CI, 9)));
  }

  @Test
  public void testNot() {
    // ColumnIndexEvalVisitor does not support evaluating NOT expression, but NOT should be rewritten
    RowRanges expected;

    expected = ALL_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(not(lessThan(INT_COL, 1))));

    expected = createRowRanges(INT_COL, 1, 2, 3, 4, 5, 6);
    assertRowRangesEquals(expected, calculateRowRanges(not(lessThanOrEqual(INT_COL, 1))));
  }

  @Test
  public void testAnd() {
    RowRanges expected;
    Expression expr;

    expected = NO_ROWS;
    expr = Expressions.and(equal(INT_COL, 1), equal(INT_COL, 2));
    assertRowRangesEquals(expected, calculateRowRanges(expr));

    expr = Expressions.and(equal(INT_COL, 1), equal(STR_COL, "Alfa"));
    assertRowRangesEquals(expected, calculateRowRanges(expr));

    expr = Expressions.and(equal(INT_COL, 2), equal(STR_COL, "Tango"));
    expected = RowRanges.intersection(createRowRanges(INT_COL, 1), createRowRanges(STR_COL, 2));
    assertRowRangesEquals(expected, calculateRowRanges(expr));
  }

  @Test
  public void testOr() {
    RowRanges expected;
    Expression expr;

    expected = createRowRanges(INT_COL, 0, 1);
    expr = Expressions.or(equal(INT_COL, 1), equal(INT_COL, 2));
    assertRowRangesEquals(expected, calculateRowRanges(expr));

    expected = RowRanges.union(createRowRanges(INT_COL, 0), createRowRanges(STR_COL, 7));
    expr = Expressions.or(equal(INT_COL, 1), equal(STR_COL, "Alfa"));
    assertRowRangesEquals(expected, calculateRowRanges(expr));

    expr = Expressions.or(equal(INT_COL, 2), equal(STR_COL, "Tango"));
    expected = RowRanges.union(createRowRanges(INT_COL, 1), createRowRanges(STR_COL, 2));
    assertRowRangesEquals(expected, calculateRowRanges(expr));
  }

  @Test
  public void testIntegerLt() {
    RowRanges expected;

    expected = NO_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(lessThan(INT_COL, 1)));

    expected = ALL_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(lessThan(INT_COL, 27)));

    expected = createRowRanges(INT_COL, 0, 1);
    assertRowRangesEquals(expected, calculateRowRanges(lessThan(INT_COL, 7)));

    expected = createRowRanges(INT_COL, 0, 1, 2, 3);
    assertRowRangesEquals(expected, calculateRowRanges(lessThan(INT_COL, 10)));
  }

  @Test
  public void testIntegerLtEq() {
    RowRanges expected;

    expected = NO_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(lessThanOrEqual(INT_COL, 0)));

    expected = ALL_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(lessThanOrEqual(INT_COL, 27)));

    expected = createRowRanges(INT_COL, 0, 1, 2, 3);
    assertRowRangesEquals(expected, calculateRowRanges(lessThanOrEqual(INT_COL, 7)));

    expected = createRowRanges(INT_COL, 0, 1, 2, 3, 4);
    assertRowRangesEquals(expected, calculateRowRanges(lessThanOrEqual(INT_COL, 11)));

    expected = createRowRanges(INT_COL, 0);

    assertRowRangesEquals(expected, calculateRowRanges(lessThanOrEqual(INT_COL, 1)));
  }

  @Test
  public void testIntegerGt() {
    RowRanges expected;

    expected = NO_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(greaterThan(INT_COL, 26)));

    expected = ALL_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(greaterThan(INT_COL, 0)));

    expected = createRowRanges(INT_COL, 3, 4, 5, 6);
    assertRowRangesEquals(expected, calculateRowRanges(greaterThan(INT_COL, 7)));
  }

  @Test
  public void testIntegerGtEq() {
    RowRanges expected;

    expected = NO_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(greaterThanOrEqual(INT_COL, 27)));

    expected = ALL_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(greaterThanOrEqual(INT_COL, 1)));

    expected = createRowRanges(INT_COL, 2, 3, 4, 5, 6);
    assertRowRangesEquals(expected, calculateRowRanges(greaterThanOrEqual(INT_COL, 7)));
  }

  @Test
  public void testIntegerEq() {
    RowRanges expected;

    expected = NO_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(equal(INT_COL, 0)));

    expected = createRowRanges(INT_COL, 2, 3);
    assertRowRangesEquals(expected, calculateRowRanges(equal(INT_COL, 7)));

    expected = createRowRanges(INT_COL, 0);
    assertRowRangesEquals(expected, calculateRowRanges(equal(INT_COL, 1)));
  }

  @Test
  public void testIntegerNotEq() {
    RowRanges expected;

    expected = ALL_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(notEqual(INT_COL, 0)));

    // TODO 如果是不会被截断的类型，可以用最大最小值做评估，跳过没有null值，且 min == max == value 的 pages
    assertRowRangesEquals(expected, calculateRowRanges(notEqual(INT_COL, 7)));
  }

  @Test
  public void testCaseInsensitive() {
    RowRanges expected;

    String intColAllCaps = INT_COL.toUpperCase(Locale.ROOT);

    expected = NO_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(equal(intColAllCaps, 0), false));

    expected = createRowRanges(INT_COL, 2, 3);
    assertRowRangesEquals(expected, calculateRowRanges(equal(intColAllCaps, 7), false));

    expected = createRowRanges(INT_COL, 0);
    assertRowRangesEquals(expected, calculateRowRanges(equal(intColAllCaps, 1), false));
  }

  @Test
  public void testStringStartsWith() {
    RowRanges expected;

    expected = NO_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(startsWith(STR_COL, "?")));

    expected = createRowRanges(STR_COL, 0);
    assertRowRangesEquals(expected, calculateRowRanges(startsWith(STR_COL, "Z")));
  }

  @Test
  public void testStringNotStartsWith() {
    RowRanges expected;

    expected = createRowRanges(STR_COL, 1, 2, 3, 4, 5, 6, 7);
    assertRowRangesEquals(expected, calculateRowRanges(notStartsWith(STR_COL, "Z")));

    expected = createRowRanges(STR_COL, 0, 1, 2, 3, 4, 5, 6);
    assertRowRangesEquals(expected, calculateRowRanges(notStartsWith(STR_COL, "A")));
  }

  @Test
  public void testIntegerIn() {
    RowRanges expected;
    Expression expr;

    expr = Expressions.in(INT_COL, 7, 13);
    expected = createRowRanges(INT_COL, 2, 3, 4);
    assertRowRangesEquals(expected, calculateRowRanges(expr));
  }

  @Test
  public void testIntegerNotIn() {
    RowRanges expected;
    Expression expr;

    expr = Expressions.notIn(INT_COL, 7, 13);
    expected = ALL_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(expr));
  }

  @Test
  public void testSomeNullsNotEq() {
    RowRanges expected;
    Expression expr;

    expr = Expressions.notEqual(STR_COL, "equal");
    expected = ALL_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(expr));
  }

  @Test
  public void testTypePromotion() {
    RowRanges expected;
    Schema promotedLong = new Schema(Types.NestedField.optional(1, INT_COL, Types.LongType.get()));

    expected = NO_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(promotedLong, equal(INT_COL, 0), true));

    expected = createRowRanges(INT_COL, 2, 3);
    assertRowRangesEquals(expected, calculateRowRanges(promotedLong, equal(INT_COL, 7), true));
  }

  @Test
  public void testMissingOffsetIndex() {
    RowRanges expected;

    PrimitiveType missingOI = org.apache.parquet.schema.Types.primitive(INT32, Type.Repetition.REQUIRED)
            .id(1)
            .named("missing_oi");
    MessageType messageType = new MessageType("test", missingOI);

    expected = ALL_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(SCHEMA, messageType, equal(INT_COL, 1), true));
  }

  private static class CIBuilder {
    private static final ByteBuffer EMPTY = ByteBuffer.wrap(new byte[0]);
    private final PrimitiveType type;
    private final BoundaryOrder order;
    boolean invalid = false;
    private List<Boolean> nullPages = Lists.newArrayList();
    private List<Long> nullCounts = Lists.newArrayList();
    private List<ByteBuffer> minValues = Lists.newArrayList();
    private List<ByteBuffer> maxValues = Lists.newArrayList();

    CIBuilder(PrimitiveType type, BoundaryOrder order) {
      this.type = type;
      this.order = order;
    }

    CIBuilder addNullPage(long nullCount) {
      nullPages.add(true);
      nullCounts.add(nullCount);
      minValues.add(EMPTY);
      maxValues.add(EMPTY);
      return this;
    }

    CIBuilder addPage(long nullCount, int min, int max) {
      nullPages.add(false);
      nullCounts.add(nullCount);
      minValues.add(ByteBuffer.wrap(BytesUtils.intToBytes(min)));
      maxValues.add(ByteBuffer.wrap(BytesUtils.intToBytes(max)));
      return this;
    }

    CIBuilder addPage(long nullCount, String min, String max) {
      nullPages.add(false);
      nullCounts.add(nullCount);
      minValues.add(ByteBuffer.wrap(min.getBytes(UTF_8)));
      maxValues.add(ByteBuffer.wrap(max.getBytes(UTF_8)));
      return this;
    }

    CIBuilder addPage(long nullCount, double min, double max) {
      if (Double.isNaN(min) || Double.isNaN(max)) {
        invalid = true;
        return this;
      }

      nullPages.add(false);
      nullCounts.add(nullCount);
      minValues.add(ByteBuffer.wrap(BytesUtils.longToBytes(Double.doubleToLongBits(min))));
      maxValues.add(ByteBuffer.wrap(BytesUtils.longToBytes(Double.doubleToLongBits(max))));
      return this;
    }

    ColumnIndex build() {
      return invalid ? null : ColumnIndexBuilder.build(type, order, nullPages, nullCounts, minValues, maxValues);
    }
  }

  private static class OIBuilder {
    private final OffsetIndexBuilder builder = OffsetIndexBuilder.getBuilder();

    OIBuilder addPage(long rowCount) {
      builder.add(1234, rowCount);
      return this;
    }

    OffsetIndex build() {
      return builder.build();
    }
  }
}
