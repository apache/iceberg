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

import static org.apache.iceberg.parquet.PageSkippingHelpers.allPageIndexes;
import static org.apache.iceberg.parquet.PageSkippingHelpers.allRows;
import static org.apache.iceberg.parquet.PageSkippingHelpers.filterPageIndexes;
import static org.apache.iceberg.parquet.PageSkippingHelpers.intersection;
import static org.apache.iceberg.parquet.PageSkippingHelpers.union;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntPredicate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.BinaryUtil;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.Pair;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetColumnIndexFilter {

  private static final Logger LOG = LoggerFactory.getLogger(ParquetColumnIndexFilter.class);

  private final Schema schema;
  private final Expression expr;

  public ParquetColumnIndexFilter(Schema schema, Expression unbound, boolean caseSensitive) {
    this.schema = schema;
    this.expr = Binder.bind(schema.asStruct(), Expressions.rewriteNot(unbound), caseSensitive);
  }

  /**
   * Calculates the row ranges containing the indexes of the rows might match the expression.
   *
   * @param fileSchema schema of file
   * @param columnIndexStore the store for providing column/offset indexes
   * @param rowCount the total number of rows in the row-group
   * @return the ranges of the possible matching row indexes; the returned ranges will contain all
   *     the rows if any of the required offset index is missing
   */
  public RowRanges calculateRowRanges(
      MessageType fileSchema, ColumnIndexStore columnIndexStore, long rowCount) {
    try {
      return new ColumnIndexEvalVisitor(fileSchema, columnIndexStore, rowCount).eval();
    } catch (ColumnIndexStore.MissingOffsetIndexException e) {
      LOG.info("Cannot get required offset index; Unable to filter on this row group", e);
      return allRows(rowCount);
    }
  }

  private static final boolean ROWS_MIGHT_MATCH = true;
  private static final boolean ROWS_CANNOT_MATCH = false;
  private static final RowRanges NO_ROWS = PageSkippingHelpers.empty();

  private class ColumnIndexEvalVisitor
      extends ExpressionVisitors.BoundExpressionVisitor<RowRanges> {

    private final Map<Integer, ColumnPath> idToColumn = Maps.newHashMap();
    private final Map<Integer, ParquetColumnIndex> idToColumnIndex = Maps.newHashMap();
    private final Map<Integer, OffsetIndex> idToOffsetIndex = Maps.newHashMap();
    private final Map<Integer, PrimitiveType> parquetTypes = Maps.newHashMap();
    private final Map<Integer, Type.PrimitiveType> icebergTypes = Maps.newHashMap();

    private final RowRanges allRows;
    private final ColumnIndexStore columnIndexStore;
    private final long rowCount;

    private ColumnIndexEvalVisitor(
        MessageType fileSchema, ColumnIndexStore columnIndexStore, long rowCount) {
      this.allRows = allRows(rowCount);
      this.columnIndexStore = columnIndexStore;
      this.rowCount = rowCount;

      for (ColumnDescriptor desc : fileSchema.getColumns()) {
        String[] path = desc.getPath();
        PrimitiveType colType = fileSchema.getType(path).asPrimitiveType();
        if (colType.getId() != null) {
          int id = colType.getId().intValue();
          parquetTypes.put(id, colType);
          Type type = schema.findType(id);
          if (type != null) {
            icebergTypes.put(id, type.asPrimitiveType());
          }

          idToColumn.put(id, ColumnPath.get(path));
        }
      }
    }

    private RowRanges eval() {
      return ExpressionVisitors.visit(expr, this);
    }

    @Override
    public RowRanges alwaysTrue() {
      return allRows;
    }

    @Override
    public RowRanges alwaysFalse() {
      return NO_ROWS;
    }

    @Override
    public RowRanges not(RowRanges result) {
      // The resulting row ranges for column index filter calculations is overestimated,
      // so evaluation of NOT expressions is not supported
      throw new UnsupportedOperationException("Cannot support evaluating NOT");
    }

    @Override
    public RowRanges and(RowRanges left, RowRanges right) {
      return intersection(left, right);
    }

    @Override
    public RowRanges or(RowRanges left, RowRanges right) {
      return union(left, right);
    }

    @Override
    public <T> RowRanges isNull(BoundReference<T> ref) {
      int id = ref.fieldId();

      Function<ParquetColumnIndex, PrimitiveIterator.OfInt> func =
          columnIndex -> {
            if (columnIndex.hasNullCounts()) {
              return filterPageIndexes(columnIndex.pageCount(), columnIndex::containsNull);
            } else {
              // Searching for nulls so if we don't have null related statistics we have to return
              // all pages
              return allPageIndexes(columnIndex.pageCount());
            }
          };

      return applyPredicate(id, func, ROWS_MIGHT_MATCH);
    }

    @Override
    public <T> RowRanges notNull(BoundReference<T> ref) {
      int id = ref.fieldId();

      // When filtering nested types notNull() is implicit filter passed even though complex
      // filters aren't pushed down in Parquet. Leave all nested column type filters to be
      // evaluated post scan.
      if (schema.findType(id) instanceof Type.NestedType) {
        return allRows;
      }

      Function<ParquetColumnIndex, PrimitiveIterator.OfInt> func =
          columnIndex -> filterPageIndexes(columnIndex.pageCount(), columnIndex::isNonNullPage);

      return applyPredicate(id, func, ROWS_CANNOT_MATCH);
    }

    @Override
    public <T> RowRanges isNaN(BoundReference<T> ref) {
      int id = ref.fieldId();

      Function<ParquetColumnIndex, PrimitiveIterator.OfInt> func =
          columnIndex -> filterPageIndexes(columnIndex.pageCount(), columnIndex::isNonNullPage);

      return applyPredicate(id, func, ROWS_CANNOT_MATCH);
    }

    @Override
    public <T> RowRanges notNaN(BoundReference<T> ref) {
      // Parquet column index does not contain statistics about NaN values, so cannot filter out any
      // pages.
      return allRows;
    }

    @Override
    public <T> RowRanges lt(BoundReference<T> ref, Literal<T> lit) {
      int id = ref.fieldId();

      Function<ParquetColumnIndex, PrimitiveIterator.OfInt> func =
          columnIndex -> {
            IntPredicate filter =
                pageIndex -> {
                  if (columnIndex.isNullPage(pageIndex)) {
                    return ROWS_CANNOT_MATCH;
                  }

                  T lower = columnIndex.min(pageIndex);
                  if (lit.comparator().compare(lower, lit.value()) >= 0) {
                    return ROWS_CANNOT_MATCH;
                  }

                  return ROWS_MIGHT_MATCH;
                };

            return filterPageIndexes(columnIndex.pageCount(), filter);
          };

      return applyPredicate(id, func, ROWS_CANNOT_MATCH);
    }

    @Override
    public <T> RowRanges ltEq(BoundReference<T> ref, Literal<T> lit) {
      int id = ref.fieldId();

      Function<ParquetColumnIndex, PrimitiveIterator.OfInt> func =
          columnIndex -> {
            IntPredicate filter =
                pageIndex -> {
                  if (columnIndex.isNullPage(pageIndex)) {
                    return ROWS_CANNOT_MATCH;
                  }

                  T lower = columnIndex.min(pageIndex);
                  if (lit.comparator().compare(lower, lit.value()) > 0) {
                    return ROWS_CANNOT_MATCH;
                  }

                  return ROWS_MIGHT_MATCH;
                };

            return filterPageIndexes(columnIndex.pageCount(), filter);
          };

      return applyPredicate(id, func, ROWS_CANNOT_MATCH);
    }

    @Override
    public <T> RowRanges gt(BoundReference<T> ref, Literal<T> lit) {
      int id = ref.fieldId();

      Function<ParquetColumnIndex, PrimitiveIterator.OfInt> func =
          columnIndex -> {
            IntPredicate filter =
                pageIndex -> {
                  if (columnIndex.isNullPage(pageIndex)) {
                    return ROWS_CANNOT_MATCH;
                  }

                  T upper = columnIndex.max(pageIndex);
                  if (lit.comparator().compare(upper, lit.value()) <= 0) {
                    return ROWS_CANNOT_MATCH;
                  }

                  return ROWS_MIGHT_MATCH;
                };
            return filterPageIndexes(columnIndex.pageCount(), filter);
          };

      return applyPredicate(id, func, ROWS_CANNOT_MATCH);
    }

    @Override
    public <T> RowRanges gtEq(BoundReference<T> ref, Literal<T> lit) {
      int id = ref.fieldId();

      Function<ParquetColumnIndex, PrimitiveIterator.OfInt> func =
          columnIndex -> {
            IntPredicate filter =
                pageIndex -> {
                  if (columnIndex.isNullPage(pageIndex)) {
                    return ROWS_CANNOT_MATCH;
                  }

                  T upper = columnIndex.max(pageIndex);
                  if (lit.comparator().compare(upper, lit.value()) < 0) {
                    return ROWS_CANNOT_MATCH;
                  }

                  return ROWS_MIGHT_MATCH;
                };
            return filterPageIndexes(columnIndex.pageCount(), filter);
          };

      return applyPredicate(id, func, ROWS_CANNOT_MATCH);
    }

    @Override
    public <T> RowRanges eq(BoundReference<T> ref, Literal<T> lit) {
      int id = ref.fieldId();

      Function<ParquetColumnIndex, PrimitiveIterator.OfInt> func =
          columnIndex -> {
            IntPredicate filter =
                pageIndex -> {
                  if (columnIndex.isNullPage(pageIndex)) {
                    return ROWS_CANNOT_MATCH;
                  }

                  T lower = columnIndex.min(pageIndex);
                  if (lit.comparator().compare(lower, lit.value()) > 0) {
                    return ROWS_CANNOT_MATCH;
                  }

                  T upper = columnIndex.max(pageIndex);
                  if (lit.comparator().compare(upper, lit.value()) < 0) {
                    return ROWS_CANNOT_MATCH;
                  }

                  return ROWS_MIGHT_MATCH;
                };

            return filterPageIndexes(columnIndex.pageCount(), filter);
          };

      return applyPredicate(id, func, ROWS_CANNOT_MATCH);
    }

    @Override
    public <T> RowRanges notEq(BoundReference<T> ref, Literal<T> lit) {
      return allRows;
    }

    @Override
    public <T> RowRanges in(BoundReference<T> ref, Set<T> literalSet) {
      int id = ref.fieldId();
      Pair<T, T> minMax = minMax(ref.comparator(), literalSet);

      Function<ParquetColumnIndex, PrimitiveIterator.OfInt> func =
          columnIndex -> {
            IntPredicate filter =
                pageIndex -> {
                  if (columnIndex.isNullPage(pageIndex)) {
                    return ROWS_CANNOT_MATCH;
                  }

                  T lower = columnIndex.min(pageIndex);
                  if (ref.comparator().compare(lower, minMax.second()) > 0) {
                    return ROWS_CANNOT_MATCH;
                  }

                  T upper = columnIndex.max(pageIndex);
                  if (ref.comparator().compare(upper, minMax.first()) < 0) {
                    return ROWS_CANNOT_MATCH;
                  }

                  return ROWS_MIGHT_MATCH;
                };

            return filterPageIndexes(columnIndex.pageCount(), filter);
          };

      return applyPredicate(id, func, ROWS_CANNOT_MATCH);
    }

    private <T> Pair<T, T> minMax(Comparator<T> comparator, Set<T> literalSet) {
      T min = null;
      T max = null;

      for (T item : literalSet) {
        if (min == null) {
          min = item;
          max = item;
        } else {
          if (comparator.compare(item, min) < 0) {
            min = item;
          } else if (comparator.compare(item, max) > 0) {
            max = item;
          }
        }
      }

      return Pair.of(min, max);
    }

    @Override
    public <T> RowRanges notIn(BoundReference<T> ref, Set<T> literalSet) {
      return allRows;
    }

    @Override
    public <T> RowRanges startsWith(BoundReference<T> ref, Literal<T> lit) {
      int id = ref.fieldId();

      Function<ParquetColumnIndex, PrimitiveIterator.OfInt> func =
          columnIndex -> {
            ByteBuffer prefixAsBytes = lit.toByteBuffer();
            Comparator<ByteBuffer> comparator = Comparators.unsignedBytes();

            IntPredicate filter =
                pageIndex -> {
                  if (columnIndex.isNullPage(pageIndex)) {
                    return ROWS_CANNOT_MATCH;
                  }

                  ByteBuffer lower = columnIndex.minBuffer(pageIndex);

                  // truncate lower bound so that its length in bytes is not greater than the length
                  // of prefix
                  int lowerLength = Math.min(prefixAsBytes.remaining(), lower.remaining());
                  int lowerCmp =
                      comparator.compare(
                          BinaryUtil.truncateBinary(lower, lowerLength), prefixAsBytes);
                  if (lowerCmp > 0) {
                    return ROWS_CANNOT_MATCH;
                  }

                  ByteBuffer upper = columnIndex.maxBuffer(pageIndex);
                  // truncate upper bound so that its length in bytes is not greater than the length
                  // of prefix
                  int upperLength = Math.min(prefixAsBytes.remaining(), upper.remaining());
                  int upperCmp =
                      comparator.compare(
                          BinaryUtil.truncateBinary(upper, upperLength), prefixAsBytes);
                  if (upperCmp < 0) {
                    return ROWS_CANNOT_MATCH;
                  }

                  return ROWS_MIGHT_MATCH;
                };

            return filterPageIndexes(columnIndex.pageCount(), filter);
          };

      return applyPredicate(id, func, ROWS_CANNOT_MATCH);
    }

    @Override
    public <T> RowRanges notStartsWith(BoundReference<T> ref, Literal<T> lit) {
      int id = ref.fieldId();

      Function<ParquetColumnIndex, PrimitiveIterator.OfInt> func =
          columnIndex -> {
            IntPredicate filter;
            if (columnIndex.hasNullCounts()) {
              ByteBuffer prefixAsBytes = lit.toByteBuffer();
              Comparator<ByteBuffer> comparator = Comparators.unsignedBytes();

              filter =
                  pageIndex -> {
                    if (columnIndex.containsNull(pageIndex)) {
                      return ROWS_MIGHT_MATCH;
                    }

                    ByteBuffer lower = columnIndex.minBuffer(pageIndex);
                    // if lower is shorter than the prefix, it can't start with the prefix
                    if (lower.remaining() < prefixAsBytes.remaining()) {
                      return ROWS_MIGHT_MATCH;
                    }

                    // truncate lower bound so that its length in bytes is not greater than the
                    // length of prefix
                    int cmp =
                        comparator.compare(
                            BinaryUtil.truncateBinary(lower, prefixAsBytes.remaining()),
                            prefixAsBytes);

                    if (cmp == 0) {
                      ByteBuffer upper = columnIndex.maxBuffer(pageIndex);
                      // the lower bound starts with the prefix; check the upper bound
                      // if upper is shorter than the prefix, it can't start with the prefix
                      if (upper.remaining() < prefixAsBytes.remaining()) {
                        return ROWS_MIGHT_MATCH;
                      }

                      // truncate upper bound so that its length in bytes is not greater than the
                      // length of prefix
                      cmp =
                          comparator.compare(
                              BinaryUtil.truncateBinary(upper, prefixAsBytes.remaining()),
                              prefixAsBytes);
                      if (cmp == 0) {
                        // both bounds match the prefix, so all rows must match the prefix and none
                        // do not match
                        return ROWS_CANNOT_MATCH;
                      }
                    }

                    return ROWS_MIGHT_MATCH;
                  };
            } else {
              // Return all pages if we don't have null counts statistics
              filter = pageIndex -> ROWS_MIGHT_MATCH;
            }

            return filterPageIndexes(columnIndex.pageCount(), filter);
          };

      return applyPredicate(id, func, ROWS_MIGHT_MATCH);
    }

    private RowRanges applyPredicate(
        int columnId,
        Function<ParquetColumnIndex, PrimitiveIterator.OfInt> func,
        boolean missingColumnMightMatch) {

      if (!idToColumn.containsKey(columnId)) {
        return missingColumnMightMatch ? allRows : NO_ROWS;
      }

      // If the column index of a column is not available, we cannot filter on this column.
      // If the offset index of a column is not available, we cannot filter on this row group.
      // Get the offset index first so that the MissingOffsetIndexException (if any) is thrown ASAP.
      OffsetIndex offsetIndex = offsetIndex(columnId);
      ParquetColumnIndex columnIndex = columnIndex(columnId);
      if (columnIndex == null) {
        LOG.info(
            "No column index for column {} is available; Unable to filter on this column",
            idToColumn.get(columnId));
        return allRows;
      }

      return PageSkippingHelpers.createRowRanges(rowCount, func.apply(columnIndex), offsetIndex);
    }

    // Assumes that the column corresponding to the id exists in the file.
    private OffsetIndex offsetIndex(int columnId) {
      return idToOffsetIndex.computeIfAbsent(
          columnId, k -> columnIndexStore.getOffsetIndex(idToColumn.get(k)));
    }

    // Assumes that the column corresponding to the id exists in the file.
    private ParquetColumnIndex columnIndex(int columnId) {
      ParquetColumnIndex wrapper = idToColumnIndex.get(columnId);

      if (wrapper == null) {
        ColumnIndex columnIndex = columnIndexStore.getColumnIndex(idToColumn.get(columnId));
        if (columnIndex != null) {
          wrapper =
              new ParquetColumnIndex(
                  columnIndex, parquetTypes.get(columnId), icebergTypes.get(columnId));
          idToColumnIndex.put(columnId, wrapper);
        }
      }

      return wrapper;
    }
  }

  /**
   * A wrapper for ColumnIndex, which will cache statistics data and convert min max buffers to
   * Iceberg type values.
   */
  private static class ParquetColumnIndex {
    private final ColumnIndex columnIndex;
    private final PrimitiveType primitiveType;
    private final Type.PrimitiveType icebergType;

    private List<Boolean> nullPages;
    private List<ByteBuffer> minBuffers;
    private List<ByteBuffer> maxBuffers;
    private List<Long> nullCounts; // optional field

    private ParquetColumnIndex(
        ColumnIndex columnIndex, PrimitiveType primitiveType, Type.PrimitiveType icebergType) {
      this.columnIndex = columnIndex;
      this.primitiveType = primitiveType;
      this.icebergType = icebergType;
    }

    private ByteBuffer minBuffer(int pageIndex) {
      if (minBuffers == null) {
        minBuffers = columnIndex.getMinValues();
      }

      return minBuffers.get(pageIndex);
    }

    private ByteBuffer maxBuffer(int pageIndex) {
      if (maxBuffers == null) {
        maxBuffers = columnIndex.getMaxValues();
      }

      return maxBuffers.get(pageIndex);
    }

    private List<Boolean> nullPages() {
      if (nullPages == null) {
        nullPages = columnIndex.getNullPages();
      }

      return nullPages;
    }

    private <T> T min(int pageIndex) {
      return fromBytes(minBuffer(pageIndex), primitiveType, icebergType);
    }

    private <T> T max(int pageIndex) {
      return fromBytes(maxBuffer(pageIndex), primitiveType, icebergType);
    }

    private Boolean isNullPage(int pageIndex) {
      return nullPages().get(pageIndex);
    }

    private Boolean isNonNullPage(int pageIndex) {
      return !nullPages().get(pageIndex);
    }

    private boolean hasNullCounts() {
      if (nullCounts == null) {
        nullCounts = columnIndex.getNullCounts();
      }

      return nullCounts != null;
    }

    private boolean containsNull(int pageIndex) {
      if (hasNullCounts()) {
        return nullCounts.get(pageIndex) > 0;
      }

      throw new UnsupportedOperationException("Has no null counts statistics");
    }

    private int pageCount() {
      return nullPages().size();
    }

    @SuppressWarnings("unchecked")
    private <T> T fromBytes(
        ByteBuffer bytes, PrimitiveType primitiveType, Type.PrimitiveType icebergType) {
      LogicalTypeAnnotation logicalTypeAnnotation = primitiveType.getLogicalTypeAnnotation();
      Optional<Object> converted =
          logicalTypeAnnotation == null
              ? Optional.empty()
              : logicalTypeAnnotation.accept(
                  new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Object>() {
                    @Override
                    public Optional<Object> visit(
                        LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
                      return Optional.of(StandardCharsets.UTF_8.decode(bytes));
                    }

                    @Override
                    public Optional<Object> visit(
                        LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumLogicalType) {
                      return Optional.of(StandardCharsets.UTF_8.decode(bytes));
                    }

                    @Override
                    public Optional<Object> visit(
                        LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType) {
                      switch (primitiveType.getPrimitiveTypeName()) {
                        case INT32:
                          return Optional.of(
                              new BigDecimal(
                                  BigInteger.valueOf(bytes.getInt(0)), decimalType.getScale()));
                        case INT64:
                          return Optional.of(
                              new BigDecimal(
                                  BigInteger.valueOf(bytes.getLong(0)), decimalType.getScale()));
                        case BINARY:
                        case FIXED_LEN_BYTE_ARRAY:
                          return Optional.of(
                              new BigDecimal(
                                  new BigInteger(ByteBuffers.toByteArray(bytes)),
                                  decimalType.getScale()));
                      }
                      return Optional.empty();
                    }

                    @Override
                    public Optional<Object> visit(
                        LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
                      switch (timeLogicalType.getUnit()) {
                        case MILLIS:
                          return Optional.of(((long) bytes.getInt(0)) * 1000L);
                        case MICROS:
                          return Optional.of(bytes.getLong(0));
                        case NANOS:
                          return Optional.of(Math.floorDiv(bytes.getLong(0), 1000));
                      }
                      return Optional.empty();
                    }

                    @Override
                    public Optional<Object> visit(
                        LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
                      switch (timestampLogicalType.getUnit()) {
                        case MILLIS:
                          return Optional.of(bytes.getLong(0) * 1000);
                        case MICROS:
                          return Optional.of(bytes.getLong(0));
                        case NANOS:
                          return Optional.of(Math.floorDiv(bytes.getLong(0), 1000));
                      }
                      return Optional.empty();
                    }

                    @Override
                    public Optional<Object> visit(
                        LogicalTypeAnnotation.JsonLogicalTypeAnnotation jsonLogicalType) {
                      return Optional.of(StandardCharsets.UTF_8.decode(bytes));
                    }

                    @Override
                    public Optional<Object> visit(
                        LogicalTypeAnnotation.UUIDLogicalTypeAnnotation uuidLogicalType) {
                      return LogicalTypeAnnotation.LogicalTypeAnnotationVisitor.super.visit(
                          uuidLogicalType);
                    }
                  });

      if (converted.isPresent()) {
        return (T) converted.get();
      }

      switch (primitiveType.getPrimitiveTypeName()) {
        case BOOLEAN:
          return (T) (Boolean) (bytes.get() != 0);
        case INT32:
          Integer intValue = bytes.getInt(0);
          if (icebergType.typeId() == Type.TypeID.LONG) {
            return (T) (Long) intValue.longValue();
          }
          return (T) intValue;
        case INT64:
          return (T) (Long) bytes.getLong(0);
        case FLOAT:
          Float floatValue = bytes.getFloat(0);
          if (icebergType.typeId() == Type.TypeID.DOUBLE) {
            return (T) (Double) floatValue.doubleValue();
          }
          return (T) floatValue;
        case DOUBLE:
          return (T) (Double) bytes.getDouble(0);
        case BINARY:
        case FIXED_LEN_BYTE_ARRAY:
          return (T) bytes;
        default:
          throw new UnsupportedOperationException("Unsupported Parquet type: " + primitiveType);
      }
    }
  }
}
