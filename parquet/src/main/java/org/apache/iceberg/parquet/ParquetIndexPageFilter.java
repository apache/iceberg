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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.compress.utils.Lists;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.apache.parquet.internal.filter2.columnindex.RowRanges.Range;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetIndexPageFilter {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetIndexPageFilter.class);

  private final Schema schema;
  private final MessageType fileSchema;
  private final Expression expr;

  public ParquetIndexPageFilter(Schema schema, MessageType fileSchema, Expression expr, boolean caseSensitive) {
    this.schema = schema;
    this.fileSchema = fileSchema;
    this.expr = Binder.bind(schema.asStruct(), Expressions.rewriteNot(expr), caseSensitive);
  }

  public long applyIndex(ParquetFileReader reader, int rowGroupIndex) {
    RowRanges ranges = new EvalVisitor(reader.getRowGroups().get(rowGroupIndex), reader).eval();
    ParquetRanges.setRanges(reader, rowGroupIndex, ranges);
    return ranges.rowCount();
  }

  private class EvalVisitor extends ExpressionVisitors.BoundExpressionVisitor<RowRanges> {
    private final ParquetFileReader reader;
    private final long totalRowCount;
    private final RowRanges allRows;
    private final Map<Integer, ColumnChunkMetaData> cols = Maps.newHashMap();
    private final Map<Integer, ColumnIndex> indexes = Maps.newHashMap();
    private final Map<Integer, OffsetIndex> offsets = Maps.newHashMap();
    private final Map<Integer, PrimitiveType> parquetTypes = Maps.newHashMap();
    private final Map<Integer, Type.PrimitiveType> icebergTypes = Maps.newHashMap();

    private EvalVisitor(BlockMetaData rowGroup, ParquetFileReader reader) {
      this.reader = reader;
      this.totalRowCount = rowGroup.getRowCount();
      this.allRows = ParquetRanges.of(0, totalRowCount - 1);

      for (ColumnDescriptor desc : fileSchema.getColumns()) {
        PrimitiveType colType = fileSchema.getType(desc.getPath()).asPrimitiveType();
        if (colType.getId() != null) {
          int id = colType.getId().intValue();
          parquetTypes.put(id, colType);
          icebergTypes.put(id, schema.findType(id).asPrimitiveType());
        }
      }

      for (ColumnChunkMetaData meta : rowGroup.getColumns()) {
        PrimitiveType colType = fileSchema.getType(meta.getPath().toArray()).asPrimitiveType();
        if (colType.getId() != null) {
          int id = colType.getId().intValue();
          cols.put(id, meta);
        }
      }
    }

    private RowRanges eval() {
      return ExpressionVisitors.visit(expr, this);
    }

    private Range asRange(int fieldId, int pageIndex) {
      OffsetIndex offsets = offsets(fieldId);
      if (offsets != null) {
        return ParquetRanges.rangeOf(
            offsets.getFirstRowIndex(pageIndex),
            offsets.getLastRowIndex(pageIndex, totalRowCount));
      } else {
        return ParquetRanges.rangeOf(0, totalRowCount - 1);
      }
    }

    @Override
    public <T> RowRanges isNull(BoundReference<T> ref) {
      int fieldId = ref.fieldId();
      ColumnIndex index = index(fieldId);
      List<Long> nullCounts = index.getNullCounts();

      List<Range> pageRanges = Lists.newArrayList();
      for (int i = 0; i < nullCounts.size(); i += 1) {
        if (nullCounts.get(i) >= 0) {
          // there is at least one null value in the page
          pageRanges.add(asRange(fieldId, i));
        }
      }

      return ParquetRanges.of(pageRanges);
    }

    @Override
    public <T> RowRanges notNull(BoundReference<T> ref) {
      int fieldId = ref.fieldId();
      ColumnIndex index = index(fieldId);
      List<Boolean> containsOnlyNull = index.getNullPages();

      List<Range> pageRanges = Lists.newArrayList();
      for (int i = 0; i < containsOnlyNull.size(); i += 1) {
        if (!containsOnlyNull.get(i)) {
          // there is at least one non-null value in the page
          pageRanges.add(asRange(fieldId, i));
        }
      }

      return ParquetRanges.of(pageRanges);
    }

    @Override
    public <T> RowRanges isNaN(BoundReference<T> ref) {
      return allRows;
    }

    @Override
    public <T> RowRanges notNaN(BoundReference<T> ref) {
      return allRows;
    }

    @Override
    public <T> RowRanges lt(BoundReference<T> ref, Literal<T> lit) {
      int fieldId = ref.fieldId();
      ColumnIndex index = index(fieldId);
      if (index == null) {
        return allRows;
      }

      List<Boolean> containsOnlyNull = index.getNullPages();
      List<ByteBuffer> lowerBounds = index.getMinValues();

      List<Range> pageRanges = Lists.newArrayList();
      for (int i = 0; i < containsOnlyNull.size(); i += 1) {
        if (containsOnlyNull.get(i)) {
          T lower = fromBytes(lowerBounds.get(i), parquetTypes.get(fieldId), icebergTypes.get(fieldId));
          if (lit.comparator().compare(lower, lit.value()) < 0) {
            pageRanges.add(asRange(fieldId, i));
          }
        }
      }

      return ParquetRanges.of(pageRanges);
    }

    @Override
    public <T> RowRanges ltEq(BoundReference<T> ref, Literal<T> lit) {
      int fieldId = ref.fieldId();
      ColumnIndex index = index(fieldId);
      if (index == null) {
        return allRows;
      }

      List<Boolean> containsOnlyNull = index.getNullPages();
      List<ByteBuffer> lowerBounds = index.getMinValues();

      List<Range> pageRanges = Lists.newArrayList();
      for (int i = 0; i < containsOnlyNull.size(); i += 1) {
        if (!containsOnlyNull.get(i)) {
          T lower = fromBytes(lowerBounds.get(i), parquetTypes.get(fieldId), icebergTypes.get(fieldId));
          if (lit.comparator().compare(lower, lit.value()) <= 0) {
            pageRanges.add(asRange(fieldId, i));
          }
        }
      }

      return ParquetRanges.of(pageRanges);
    }

    @Override
    public <T> RowRanges gt(BoundReference<T> ref, Literal<T> lit) {
      int fieldId = ref.fieldId();
      ColumnIndex index = index(fieldId);
      if (index == null) {
        return allRows;
      }

      List<Boolean> containsOnlyNull = index.getNullPages();
      List<ByteBuffer> upperBounds = index.getMaxValues();

      List<Range> pageRanges = Lists.newArrayList();
      for (int i = 0; i < containsOnlyNull.size(); i += 1) {
        if (!containsOnlyNull.get(i)) {
          T upper = fromBytes(upperBounds.get(i), parquetTypes.get(fieldId), icebergTypes.get(fieldId));
          if (lit.comparator().compare(upper, lit.value()) > 0) {
            pageRanges.add(asRange(fieldId, i));
          }
        }
      }

      return ParquetRanges.of(pageRanges);
    }

    @Override
    public <T> RowRanges gtEq(BoundReference<T> ref, Literal<T> lit) {
      int fieldId = ref.fieldId();
      ColumnIndex index = index(fieldId);
      if (index == null) {
        return allRows;
      }

      List<Boolean> containsOnlyNull = index.getNullPages();
      List<ByteBuffer> upperBounds = index.getMaxValues();

      List<Range> pageRanges = Lists.newArrayList();
      for (int i = 0; i < containsOnlyNull.size(); i += 1) {
        if (!containsOnlyNull.get(i)) {
          T upper = fromBytes(upperBounds.get(i), parquetTypes.get(fieldId), icebergTypes.get(fieldId));
          if (lit.comparator().compare(upper, lit.value()) >= 0) {
            pageRanges.add(asRange(fieldId, i));
          }
        }
      }

      return ParquetRanges.of(pageRanges);
    }

    @Override
    public <T> RowRanges eq(BoundReference<T> ref, Literal<T> lit) {
      int fieldId = ref.fieldId();
      ColumnIndex index = index(fieldId);
      if (index == null) {
        return allRows;
      }

      List<Boolean> containsOnlyNull = index.getNullPages();
      List<ByteBuffer> lowerBounds = index.getMinValues();
      List<ByteBuffer> upperBounds = index.getMaxValues();

      List<Range> pageRanges = Lists.newArrayList();
      for (int i = 0; i < containsOnlyNull.size(); i += 1) {
        if (!containsOnlyNull.get(i)) {
          Comparator<T> comparator = lit.comparator();
          T lower = fromBytes(lowerBounds.get(i), parquetTypes.get(fieldId), icebergTypes.get(fieldId));
          T upper = fromBytes(upperBounds.get(i), parquetTypes.get(fieldId), icebergTypes.get(fieldId));
          if (comparator.compare(lower, lit.value()) <= 0 && comparator.compare(lit.value(), upper) <= 0) {
            pageRanges.add(asRange(fieldId, i));
          }
        }
      }

      return ParquetRanges.of(pageRanges);
    }

    @Override
    public <T> RowRanges notEq(BoundReference<T> ref, Literal<T> lit) {
      int fieldId = ref.fieldId();
      ColumnIndex index = index(fieldId);
      if (index == null) {
        return allRows;
      }

      List<Long> nullCounts = index.getNullCounts();
      List<ByteBuffer> lowerBounds = index.getMinValues();
      List<ByteBuffer> upperBounds = index.getMaxValues();

      List<Range> pageRanges = Lists.newArrayList();
      for (int i = 0; i < nullCounts.size(); i += 1) {
        if (nullCounts.get(i) > 0) {
          pageRanges.add(asRange(fieldId, i));
          continue;
        }

        Comparator<T> comparator = lit.comparator();
        T lower = fromBytes(lowerBounds.get(i), parquetTypes.get(fieldId), icebergTypes.get(fieldId));
        T upper = fromBytes(upperBounds.get(i), parquetTypes.get(fieldId), icebergTypes.get(fieldId));
        if (comparator.compare(lower, lit.value()) != 0 || comparator.compare(lit.value(), upper) != 0) {
          pageRanges.add(asRange(fieldId, i));
        }
      }

      return ParquetRanges.of(pageRanges);
    }

    @Override
    public <T> RowRanges in(BoundReference<T> ref, Set<T> literalSet) {
      int fieldId = ref.fieldId();
      ColumnIndex index = index(fieldId);
      if (index == null) {
        return allRows;
      }

      List<Boolean> containsOnlyNull = index.getNullPages();
      List<ByteBuffer> lowerBounds = index.getMinValues();
      List<ByteBuffer> upperBounds = index.getMaxValues();

      List<Range> pageRanges = Lists.newArrayList();
      for (int i = 0; i < containsOnlyNull.size(); i += 1) {
        if (!containsOnlyNull.get(i)) {
          Comparator<T> comparator = ref.comparator();
          boolean overlapsRange = false;
          for (T value : literalSet) {
            T lower = fromBytes(lowerBounds.get(i), parquetTypes.get(fieldId), icebergTypes.get(fieldId));
            T upper = fromBytes(upperBounds.get(i), parquetTypes.get(fieldId), icebergTypes.get(fieldId));
            if (comparator.compare(lower, value) <= 0 && comparator.compare(value, upper) <= 0) {
              overlapsRange = true;
            }
          }

          if (overlapsRange) {
            pageRanges.add(asRange(fieldId, i));
          }
        }
      }

      return ParquetRanges.of(pageRanges);
    }

    @Override
    public <T> RowRanges notIn(BoundReference<T> ref, Set<T> literalSet) {
      return allRows;
    }

    @Override
    public <T> RowRanges startsWith(BoundReference<T> ref, Literal<T> lit) {
      return allRows;
    }

    @Override
    public <T> RowRanges notStartsWith(BoundReference<T> ref, Literal<T> lit) {
      return allRows;
    }

    @Override
    public RowRanges alwaysTrue() {
      return allRows;
    }

    @Override
    public RowRanges alwaysFalse() {
      return ParquetRanges.empty();
    }

    @Override
    public RowRanges and(RowRanges left, RowRanges right) {
      return ParquetRanges.intersection(left, right);
    }

    @Override
    public RowRanges or(RowRanges left, RowRanges right) {
      return ParquetRanges.union(left, right);
    }

    private ColumnIndex index(int id) {
      ColumnIndex index = indexes.get(id);
      if (index == null) {
        ColumnChunkMetaData col = cols.get(id);
        try {
          index = reader.readColumnIndex(col);
          indexes.put(id, index);
        } catch (IOException e) {
          LOG.warn("Failed to read column index for column: {} (skipping index)", id);
        }
      }

      return index;
    }

    private OffsetIndex offsets(int id) {
      OffsetIndex index = offsets.get(id);
      if (index == null) {
        ColumnChunkMetaData col = cols.get(id);
        try {
          index = reader.readOffsetIndex(col);
          offsets.put(id, index);
        } catch (IOException e) {
          LOG.warn("Failed to read column index for column: {} (skipping index)", id);
        }
      }

      return index;
    }
  }

  @SuppressWarnings("unchecked")
  private <T> T fromBytes(ByteBuffer bytes, PrimitiveType primitiveType, Type.PrimitiveType icebergType) {
    Optional<Object> converted = primitiveType.getLogicalTypeAnnotation()
        .accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Object>() {
          @Override
          public Optional<Object> visit(LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
            return Optional.of(StandardCharsets.UTF_8.decode(bytes));
          }

          @Override
          public Optional<Object> visit(LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumLogicalType) {
            return Optional.of(StandardCharsets.UTF_8.decode(bytes));
          }

          @Override
          public Optional<Object> visit(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType) {
            switch (primitiveType.getPrimitiveTypeName()) {
              case INT32:
                return Optional.of(new BigDecimal(BigInteger.valueOf(bytes.getInt(0)), decimalType.getScale()));
              case INT64:
                return Optional.of(new BigDecimal(BigInteger.valueOf(bytes.getLong(0)), decimalType.getScale()));
              case BINARY:
              case FIXED_LEN_BYTE_ARRAY:
                new BigDecimal(new BigInteger(ByteBuffers.toByteArray(bytes)), decimalType.getScale());
            }
            return Optional.empty();
          }

          @Override
          public Optional<Object> visit(LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
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
          public Optional<Object> visit(LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
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
          public Optional<Object> visit(LogicalTypeAnnotation.JsonLogicalTypeAnnotation jsonLogicalType) {
            return Optional.of(StandardCharsets.UTF_8.decode(bytes));
          }

          @Override
          public Optional<Object> visit(LogicalTypeAnnotation.UUIDLogicalTypeAnnotation uuidLogicalType) {
            return LogicalTypeAnnotation.LogicalTypeAnnotationVisitor.super.visit(uuidLogicalType);
          }
        });

    if (converted.isPresent()) {
      return (T) converted.get();
    }

    switch (primitiveType.getPrimitiveTypeName()) {
      case BOOLEAN:
        return (T) (Boolean) (bytes.get() != 0);
      case INT32:
        Integer intValue = bytes.getInt();
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
