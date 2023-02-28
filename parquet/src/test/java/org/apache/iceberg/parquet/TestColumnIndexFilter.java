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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Locale;
import java.util.PrimitiveIterator;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DecimalUtil;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.BoundaryOrder;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.ColumnIndexBuilder;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndexBuilder;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Assert;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNaN;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.expressions.Expressions.notNaN;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.notStartsWith;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.expressions.Expressions.startsWith;
import static org.apache.parquet.internal.column.columnindex.BoundaryOrder.ASCENDING;
import static org.apache.parquet.internal.column.columnindex.BoundaryOrder.DESCENDING;
import static org.apache.parquet.internal.column.columnindex.BoundaryOrder.UNORDERED;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Types.optional;

public class TestColumnIndexFilter {
  /**
   * COPIED FROM org.apache.parquet.internal.filter2.columnindex.TestColumnIndexFilter
   **/
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

    CIBuilder addPage(long nullCount, byte[] min, byte[] max) {
      nullPages.add(false);
      nullCounts.add(nullCount);
      minValues.add(ByteBuffer.wrap(min));
      maxValues.add(ByteBuffer.wrap(max));
      return this;
    }

    CIBuilder addPage(long nullCount, int min, int max) {
      nullPages.add(false);
      nullCounts.add(nullCount);
      minValues.add(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(0, min));
      maxValues.add(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(0, max));
      return this;
    }

    CIBuilder addPage(long nullCount, long min, long max) {
      nullPages.add(false);
      nullCounts.add(nullCount);
      minValues.add(ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(0, min));
      maxValues.add(ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(0, max));
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
      minValues.add(ByteBuffer.allocate(Double.BYTES).order(ByteOrder.LITTLE_ENDIAN).putDouble(0, min));
      maxValues.add(ByteBuffer.allocate(Double.BYTES).order(ByteOrder.LITTLE_ENDIAN).putDouble(0, max));
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

  private static final long TOTAL_ROW_COUNT = 30;
  private static final String INT_COL = "int_col";
  private static final String STR_COL = "str_col";
  private static final String NO_NANS = "no_nans";
  private static final String NO_CI = "no_ci";
  private static final String ALL_NULLS = "all_nulls";
  private static final String INT_DECIMAL_7_2 = "int_decimal_7_2";
  private static final String NOT_IN_FILE = "not_in_file";

  private static final Schema SCHEMA = new Schema(
      Types.NestedField.optional(1, INT_COL, Types.IntegerType.get()),
      Types.NestedField.optional(2, STR_COL, Types.StringType.get()),
      Types.NestedField.optional(3, NO_NANS, Types.DoubleType.get()),
      Types.NestedField.optional(4, NO_CI, Types.DoubleType.get()),
      Types.NestedField.optional(5, ALL_NULLS, Types.LongType.get()),
      Types.NestedField.optional(6, INT_DECIMAL_7_2, Types.DecimalType.of(7, 2)),
      Types.NestedField.optional(7, NOT_IN_FILE, Types.LongType.get())
  );

  private static final MessageType FILE_SCHEMA = org.apache.parquet.schema.Types.buildMessage()
      .addField(optional(INT32).id(1).named(INT_COL))
      .addField(optional(BINARY).id(2).as(LogicalTypeAnnotation.stringType()).id(2).named(STR_COL))
      .addField(optional(DOUBLE).id(3).named(NO_NANS))
      .addField(optional(DOUBLE).id(4).named(NO_CI))
      .addField(optional(INT64).id(5).named(ALL_NULLS))
      .addField(optional(INT32).id(6).as(LogicalTypeAnnotation.decimalType(2, 9)).named(INT_DECIMAL_7_2))
      .named("table");

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
  private static final ColumnIndex INT_DECIMAL_7_2_CI = new CIBuilder(optional(INT32).named(INT_DECIMAL_7_2), UNORDERED)
      .addPage(0, 99, 99)
      .addPage(0, 100, 100)
      .addPage(0, 101, 101)
      .addPage(0, 98, 98)
      .addPage(0, 99, 103)
      .addNullPage(4)
      .addPage(0, 100, 100)
      .addPage(2, 87, 109)
      .addNullPage(2)
      .build();
  private static final OffsetIndex INT_DECIMAL_7_2_OI = new OIBuilder()
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
        case INT_DECIMAL_7_2:
          return INT_DECIMAL_7_2_CI;
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
        case INT_DECIMAL_7_2:
          return INT_DECIMAL_7_2_OI;
        default:
          throw new MissingOffsetIndexException(column);
      }
    }
  };

  /**
   * <pre>
   * row   int_col       str_col        no_nans        no_ci          all_nulls      int_decimal_7_2
   *                                                 (no column index)
   *      ------0------  ------0------  ------0------  ------0------  ------0------  ------0------
   * 0.   1              Zulu           2.03                          null           99
   *      ------1------  ------1------  ------1------  ------1------  ------1------  ------1------
   * 1.   2              Yankee         4.67                          null           100
   * 2.   3              Xray           3.42                          null           100
   * 3.   4              Whiskey        8.71                          null           100
   *                     ------2------                 ------2------                 ------2------
   * 4.   5              Victor         0.56                          null           101
   * 5.   6              Uniform        4.30                          null           101
   *                                    ------2------  ------3------                 ------3------
   * 6.   null           null           null                          null           98
   *      ------2------                                ------4------                 ------4------
   * 7.   7              Tango          3.50                          null           102
   *                     ------3------
   * 8.   7              null           3.14                          null           103
   *      ------3------k
   * 9.   7              null           null                          null           99
   *                                    ------3------
   * 10.  null           null           9.99                          null           100
   *                     ------4------
   * 11.  8              Sierra         8.78                          null           99
   *                                                   ------5------                 ------5------
   * 12.  9              Romeo          9.56                          null           null
   * 13.  10             Quebec         2.71                          null           null
   *      ------4------
   * 14.  11             Papa           5.71                          null           null
   * 15.  12             Oscar          4.09                          null           null
   *                     ------5------  ------4------  ------6------                 ------6------
   * 16.  13             November       null                          null           100
   * 17.  14             Mike           null                          null           100
   * 18.  15             Lima           0.36                          null           100
   * 19.  16             Kilo           2.94                          null           100
   * 20.  17             Juliett        4.23                          null           100
   *      ------5------  ------6------                 ------7------                 ------7------
   * 21.  18             India          null                          null           109
   * 22.  19             Hotel          5.32                          null           108
   *                                    ------5------
   * 23.  20             Golf           4.17                          null           88
   * 24.  21             Foxtrot        7.92                          null           87
   * 25.  22             Echo           7.95                          null           88
   *                                   ------6------
   * 26.  23             Delta          null                          null           88
   *      ------6------
   * 27.  24             Charlie        null                          null           88
   *                                                   ------8------                 ------8------
   * 28.  25             Bravo          null                          null           null
   *                     ------7------
   * 29.  26             Alfa           null                          null           null
   * </pre>
   */

  private static final RowRanges ALL_ROWS = PageSkippingHelpers.allRows(TOTAL_ROW_COUNT);
  private static final RowRanges NO_ROWS = PageSkippingHelpers.empty();

  private static RowRanges selectRowRanges(String path, int... pageIndexes) {
    return selectRowRanges(path, STORE, TOTAL_ROW_COUNT, pageIndexes);
  }

  private static RowRanges selectRowRanges(String path, ColumnIndexStore store, long rowCount, int... pageIndexes) {
    return PageSkippingHelpers.createRowRanges(rowCount, new PrimitiveIterator.OfInt() {
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
    }, store.getOffsetIndex(ColumnPath.fromDotString(path)));
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
    return calculateRowRanges(expr, true);
  }

  private RowRanges calculateRowRanges(Expression expr, boolean caseSensitive) {
    return calculateRowRanges(SCHEMA, FILE_SCHEMA, expr, caseSensitive, STORE, TOTAL_ROW_COUNT);
  }

  private RowRanges calculateRowRanges(Schema schema, MessageType messageType, Expression expr,
                                       boolean caseSensitive, ColumnIndexStore store, long rowCount) {
    return new ParquetColumnIndexFilter(schema, expr, caseSensitive)
            .calculateRowRanges(messageType, store, rowCount);
  }

  @Test
  public void testIsNulls() {
    RowRanges expected;

    expected = selectRowRanges(INT_COL, 1, 3);
    assertRowRangesEquals(expected, calculateRowRanges(isNull(INT_COL)));

    expected = selectRowRanges(STR_COL, 2, 3);
    assertRowRangesEquals(expected, calculateRowRanges(isNull(STR_COL)));

    expected = selectRowRanges(NO_NANS, 2, 4, 6);
    assertRowRangesEquals(expected, calculateRowRanges(isNull(NO_NANS)));

    expected = ALL_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(isNull(ALL_NULLS)));
  }

  @Test
  public void testNotNulls() {
    RowRanges expected;

    expected = ALL_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(notNull(INT_COL)));

    expected = selectRowRanges(STR_COL, 0, 1, 2, 4, 5, 6, 7);
    assertRowRangesEquals(expected, calculateRowRanges(notNull(STR_COL)));

    expected = selectRowRanges(NO_NANS, 0, 1, 2, 3, 4, 5);
    assertRowRangesEquals(expected, calculateRowRanges(notNull(NO_NANS)));

    expected = NO_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(notNull(ALL_NULLS)));
  }

  @Test
  public void testIsNaN() {
    RowRanges expected;

    // column index exists, null page 6 should be filtered out
    expected = selectRowRanges(NO_NANS, 0, 1, 2, 3, 4, 5);
    assertRowRangesEquals(expected, calculateRowRanges(isNaN(NO_NANS)));

    assertRowRangesEquals(ALL_ROWS, calculateRowRanges(isNaN(NO_CI)));
  }

  @Test
  public void testNotNaN() {
    RowRanges expected;

    expected = ALL_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(notNaN(NO_NANS)));

    assertRowRangesEquals(expected, calculateRowRanges(notNaN(NO_CI)));
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

    expected = selectRowRanges(INT_COL, 1, 2, 3, 4, 5, 6);
    assertRowRangesEquals(expected, calculateRowRanges(not(lessThanOrEqual(INT_COL, 1))));
  }

  @Test
  public void testAnd() {
    RowRanges expected;
    Expression expr;

    List<ColumnDescriptor> columns = FILE_SCHEMA.getColumns();
    columns.forEach(System.out::println);

    expected = NO_ROWS;
    expr = and(equal(INT_COL, 1), equal(INT_COL, 2));
    assertRowRangesEquals(expected, calculateRowRanges(expr));

    expr = and(equal(INT_COL, 1), equal(STR_COL, "Alfa"));
    assertRowRangesEquals(expected, calculateRowRanges(expr));

    expr = and(equal(INT_COL, 2), equal(STR_COL, "Tango"));
    expected = PageSkippingHelpers.intersection(selectRowRanges(INT_COL, 1), selectRowRanges(STR_COL, 2));
    assertRowRangesEquals(expected, calculateRowRanges(expr));
  }

  @Test
  public void testOr() {
    RowRanges expected;
    Expression expr;

    expected = selectRowRanges(INT_COL, 0, 1);
    expr = or(equal(INT_COL, 1), equal(INT_COL, 2));
    assertRowRangesEquals(expected, calculateRowRanges(expr));

    expected = PageSkippingHelpers.union(selectRowRanges(INT_COL, 0), selectRowRanges(STR_COL, 7));
    expr = or(equal(INT_COL, 1), equal(STR_COL, "Alfa"));
    assertRowRangesEquals(expected, calculateRowRanges(expr));

    expr = or(equal(INT_COL, 2), equal(STR_COL, "Tango"));
    expected = PageSkippingHelpers.union(selectRowRanges(INT_COL, 1), selectRowRanges(STR_COL, 2));
    assertRowRangesEquals(expected, calculateRowRanges(expr));
  }

  @Test
  public void testIntegerLt() {
    RowRanges expected;

    expected = NO_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(lessThan(INT_COL, 1)));

    expected = ALL_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(lessThan(INT_COL, 27)));

    expected = selectRowRanges(INT_COL, 0, 1);
    assertRowRangesEquals(expected, calculateRowRanges(lessThan(INT_COL, 7)));

    expected = selectRowRanges(INT_COL, 0, 1, 2, 3);
    assertRowRangesEquals(expected, calculateRowRanges(lessThan(INT_COL, 10)));
  }

  @Test
  public void testIntegerLtEq() {
    RowRanges expected;

    expected = NO_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(lessThanOrEqual(INT_COL, 0)));

    expected = ALL_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(lessThanOrEqual(INT_COL, 27)));

    expected = selectRowRanges(INT_COL, 0, 1, 2, 3);
    assertRowRangesEquals(expected, calculateRowRanges(lessThanOrEqual(INT_COL, 7)));

    expected = selectRowRanges(INT_COL, 0, 1, 2, 3, 4);
    assertRowRangesEquals(expected, calculateRowRanges(lessThanOrEqual(INT_COL, 11)));

    expected = selectRowRanges(INT_COL, 0);
    assertRowRangesEquals(expected, calculateRowRanges(lessThanOrEqual(INT_COL, 1)));
  }

  @Test
  public void testIntegerGt() {
    RowRanges expected;

    expected = NO_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(greaterThan(INT_COL, 26)));

    expected = ALL_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(greaterThan(INT_COL, 0)));

    expected = selectRowRanges(INT_COL, 3, 4, 5, 6);
    assertRowRangesEquals(expected, calculateRowRanges(greaterThan(INT_COL, 7)));
  }

  @Test
  public void testIntegerGtEq() {
    RowRanges expected;

    expected = NO_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(greaterThanOrEqual(INT_COL, 27)));

    expected = ALL_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(greaterThanOrEqual(INT_COL, 1)));

    expected = selectRowRanges(INT_COL, 2, 3, 4, 5, 6);
    assertRowRangesEquals(expected, calculateRowRanges(greaterThanOrEqual(INT_COL, 7)));
  }

  @Test
  public void testIntegerEq() {
    RowRanges expected;

    expected = NO_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(equal(INT_COL, 0)));

    expected = selectRowRanges(INT_COL, 2, 3);
    assertRowRangesEquals(expected, calculateRowRanges(equal(INT_COL, 7)));

    expected = selectRowRanges(INT_COL, 0);
    assertRowRangesEquals(expected, calculateRowRanges(equal(INT_COL, 1)));
  }

  @Test
  public void testIntegerNotEq() {
    RowRanges expected;

    expected = ALL_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(notEqual(INT_COL, 0)));

    assertRowRangesEquals(expected, calculateRowRanges(notEqual(INT_COL, 7)));
  }

  @Test
  public void testCaseInsensitive() {
    RowRanges expected;

    String intColAllCaps = INT_COL.toUpperCase(Locale.ROOT);

    expected = NO_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(equal(intColAllCaps, 0), false));

    expected = selectRowRanges(INT_COL, 2, 3);
    assertRowRangesEquals(expected, calculateRowRanges(equal(intColAllCaps, 7), false));

    expected = selectRowRanges(INT_COL, 0);
    assertRowRangesEquals(expected, calculateRowRanges(equal(intColAllCaps, 1), false));
  }

  @Test
  public void testStringStartsWith() {
    RowRanges expected;

    expected = NO_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(startsWith(STR_COL, "?")));

    assertRowRangesEquals(expected, calculateRowRanges(startsWith(STR_COL, "s")));

    expected = selectRowRanges(STR_COL, 4);
    assertRowRangesEquals(expected, calculateRowRanges(startsWith(STR_COL, "S")));

    expected = selectRowRanges(STR_COL, 4, 6);
    assertRowRangesEquals(expected, calculateRowRanges(
        Expressions.or(startsWith(STR_COL, "Q"), startsWith(STR_COL, "G"))));

    expected = selectRowRanges(STR_COL, 0);
    assertRowRangesEquals(expected, calculateRowRanges(startsWith(STR_COL, "Z")));
  }

  @Test
  public void testStringNotStartsWith() {
    RowRanges expected;

    expected = selectRowRanges(STR_COL, 1, 2, 3, 4, 5, 6, 7);
    assertRowRangesEquals(expected, calculateRowRanges(notStartsWith(STR_COL, "Z")));

    expected = selectRowRanges(STR_COL, 0, 1, 2, 3, 4, 5, 6);
    assertRowRangesEquals(expected, calculateRowRanges(notStartsWith(STR_COL, "A")));

    expected = ALL_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(notStartsWith(STR_COL, "B")));
  }

  @Test
  public void testIntegerIn() {
    RowRanges expected;
    Expression expr;

    expr = in(INT_COL, 7, 13);
    expected = selectRowRanges(INT_COL, 2, 3, 4);
    assertRowRangesEquals(expected, calculateRowRanges(expr));
  }

  @Test
  public void testIntegerNotIn() {
    RowRanges expected;
    Expression expr;

    expr = notIn(INT_COL, 7, 13);
    expected = ALL_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(expr));
  }

  @Test
  public void testSomeNullsNotEq() {
    RowRanges expected;
    Expression expr;

    expr = notEqual(STR_COL, "equal");
    expected = ALL_ROWS;
    assertRowRangesEquals(expected, calculateRowRanges(expr));
  }

  @Test
  public void testIntTypePromotion() {
    RowRanges expected;
    Schema promotedLong = new Schema(Types.NestedField.optional(1, INT_COL, Types.LongType.get()));

    expected = NO_ROWS;
    RowRanges actual =
        calculateRowRanges(promotedLong, FILE_SCHEMA, equal(INT_COL, 0), true, STORE, TOTAL_ROW_COUNT);
    assertRowRangesEquals(expected, actual);

    expected = selectRowRanges(INT_COL, 2, 3);
    actual =
        calculateRowRanges(promotedLong, FILE_SCHEMA, equal(INT_COL, 7), true, STORE, TOTAL_ROW_COUNT);
    assertRowRangesEquals(expected, actual);
  }

  @Test
  public void testMissingOffsetIndex() {
    RowRanges expected;

    PrimitiveType missingOI = org.apache.parquet.schema.Types.primitive(INT32, Type.Repetition.REQUIRED)
            .id(1)
            .named("missing_oi");
    MessageType messageType = new MessageType("table", missingOI);

    expected = ALL_ROWS;
    RowRanges actual = calculateRowRanges(SCHEMA, messageType, equal(INT_COL, 1), true, STORE, TOTAL_ROW_COUNT);
    assertRowRangesEquals(expected, actual);
  }

  @Test
  public void testIntBackedDecimal() {
    RowRanges expected;

    Expression expr = equal(INT_DECIMAL_7_2, new BigDecimal("1.00"));
    expected = selectRowRanges(INT_DECIMAL_7_2, 1, 4, 6, 7);

    assertRowRangesEquals(expected, calculateRowRanges(expr));

    expr = or(lessThan(INT_DECIMAL_7_2, new BigDecimal("1.00")),
        greaterThan(INT_DECIMAL_7_2, new BigDecimal("1.01")));

    expected = selectRowRanges(INT_DECIMAL_7_2, 0, 3, 4, 7);
    assertRowRangesEquals(expected, calculateRowRanges(expr));
  }

  @Test
  public void testDecimalTypePromotion() {
    RowRanges expected;

    Schema promotedDecimal = new Schema(Types.NestedField.optional(6, INT_DECIMAL_7_2, Types.DecimalType.of(38,
        10)));

    Expression expr = equal(INT_DECIMAL_7_2, new BigDecimal("1.00"));
    expected = selectRowRanges(INT_DECIMAL_7_2, 1, 4, 6, 7);
    assertRowRangesEquals(expected, calculateRowRanges(expr));

    expr = or(lessThan(INT_DECIMAL_7_2, new BigDecimal("1.00")),
        greaterThan(INT_DECIMAL_7_2, new BigDecimal("1.01")));

    expected = selectRowRanges(INT_DECIMAL_7_2, 0, 3, 4, 7);
    assertRowRangesEquals(expected, calculateRowRanges(expr));
  }

  // 38 precision 10 scale decimal to bytes
  private byte[] decimalToBytes(String decimalStr) {
    BigDecimal decimal = new BigDecimal(decimalStr).setScale(10);
    int requiredBytes = TypeUtil.decimalRequiredBytes(38);
    byte[] bytes = new byte[requiredBytes];
    return DecimalUtil.toReusedFixLengthBytes(38, 10, decimal, bytes);
  }

  @Test
  public void testBinaryBackedDecimal() {
    String binaryDecimal = "decimal_38_10";
    long rowCount = 9;

    ColumnIndex binaryDecimalCI = new CIBuilder(optional(FIXED_LEN_BYTE_ARRAY)
            .length(TypeUtil.decimalRequiredBytes(38))
            .named(binaryDecimal), ASCENDING)
            .addPage(0, decimalToBytes("12.34"), decimalToBytes("12.35"))
            .addPage(0, decimalToBytes("123456789.87654321"), decimalToBytes("123456789.87654323"))
            .build();

    OffsetIndex binaryDecimalOI = new OIBuilder()
            .addPage(5)
            .addPage(4)
            .build();

    ColumnIndexStore columnIndexStore = new ColumnIndexStore() {
      @Override
      public ColumnIndex getColumnIndex(ColumnPath columnPath) {
        switch (columnPath.toDotString()) {
          case "decimal_38_10":
            return binaryDecimalCI;
          default:
            return null;
        }
      }

      @Override
      public OffsetIndex getOffsetIndex(ColumnPath columnPath) {
        switch (columnPath.toDotString()) {
          case "decimal_38_10":
            return binaryDecimalOI;
          default:
            throw new MissingOffsetIndexException(columnPath);
        }
      }
    };

    MessageType messageType = org.apache.parquet.schema.Types.buildMessage()
        .addField(optional(FIXED_LEN_BYTE_ARRAY).length(TypeUtil.decimalRequiredBytes(38)).id(1).as(LogicalTypeAnnotation.decimalType(10, 38)).named(binaryDecimal))
        .named("decimal");

    Schema schema = new Schema(
            Types.NestedField.optional(1, binaryDecimal, Types.DecimalType.of(38, 10)));

    Expression expr = or(
            lessThan(binaryDecimal, new BigDecimal("12.34")),
            greaterThanOrEqual(binaryDecimal, new BigDecimal("123456789.87654322"))
    );

    RowRanges expected = selectRowRanges(binaryDecimal, columnIndexStore, rowCount, 1);
    RowRanges actual = calculateRowRanges(schema, messageType, expr, true, columnIndexStore, rowCount);
    assertRowRangesEquals(expected, actual);

    expr = greaterThan(binaryDecimal, new BigDecimal("123456789.87654323"));
    expected = NO_ROWS;
    actual = calculateRowRanges(schema, messageType, expr, true, columnIndexStore, rowCount);
    assertRowRangesEquals(expected, actual);
  }
}
