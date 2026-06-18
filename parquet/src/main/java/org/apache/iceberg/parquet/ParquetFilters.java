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
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.ExpressionVisitors.ExpressionVisitor;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DecimalUtil;
import org.apache.iceberg.util.UUIDUtil;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

class ParquetFilters {

  private ParquetFilters() {}

  static FilterCompat.Filter convert(
      MessageType parquetSchema, Expression expr, boolean caseSensitive) {
    Schema schema = ParquetSchemaUtil.convert(parquetSchema);
    FilterPredicate pred =
        ExpressionVisitors.visit(
            Expressions.rewriteNot(expr),
            new ConvertFilterToParquet(
                schema, primitiveTypesById(parquetSchema, schema), caseSensitive));
    // TODO: handle AlwaysFalse.INSTANCE
    if (pred != null && pred != AlwaysTrue.INSTANCE) {
      // FilterCompat will apply LogicalInverseRewriter
      return FilterCompat.get(pred);
    } else {
      return FilterCompat.NOOP;
    }
  }

  private static Map<Integer, PrimitiveType> primitiveTypesById(
      MessageType parquetSchema, Schema schema) {
    Map<Integer, PrimitiveType> primitiveTypesById = Maps.newHashMap();

    for (ColumnDescriptor desc : parquetSchema.getColumns()) {
      PrimitiveType primitiveType = parquetSchema.getType(desc.getPath()).asPrimitiveType();
      Integer fieldId = schema.aliasToId(String.join(".", desc.getPath()));
      if (fieldId != null) {
        primitiveTypesById.put(fieldId, primitiveType);
      }
    }

    return primitiveTypesById;
  }

  private static class ConvertFilterToParquet extends ExpressionVisitor<FilterPredicate> {
    private final Schema schema;
    private final Map<Integer, PrimitiveType> primitiveTypesById;
    private final boolean caseSensitive;

    private ConvertFilterToParquet(
        Schema schema, Map<Integer, PrimitiveType> primitiveTypesById, boolean caseSensitive) {
      this.schema = schema;
      this.primitiveTypesById = primitiveTypesById;
      this.caseSensitive = caseSensitive;
    }

    @Override
    public FilterPredicate alwaysTrue() {
      return AlwaysTrue.INSTANCE;
    }

    @Override
    public FilterPredicate alwaysFalse() {
      return AlwaysFalse.INSTANCE;
    }

    @Override
    public FilterPredicate not(FilterPredicate child) {
      if (child == AlwaysTrue.INSTANCE) {
        return AlwaysFalse.INSTANCE;
      } else if (child == AlwaysFalse.INSTANCE) {
        return AlwaysTrue.INSTANCE;
      }
      return FilterApi.not(child);
    }

    @Override
    public FilterPredicate and(FilterPredicate left, FilterPredicate right) {
      if (left == AlwaysFalse.INSTANCE || right == AlwaysFalse.INSTANCE) {
        return AlwaysFalse.INSTANCE;
      } else if (left == AlwaysTrue.INSTANCE) {
        return right;
      } else if (right == AlwaysTrue.INSTANCE) {
        return left;
      }
      return FilterApi.and(left, right);
    }

    @Override
    public FilterPredicate or(FilterPredicate left, FilterPredicate right) {
      if (left == AlwaysTrue.INSTANCE || right == AlwaysTrue.INSTANCE) {
        return AlwaysTrue.INSTANCE;
      } else if (left == AlwaysFalse.INSTANCE) {
        return right;
      } else if (right == AlwaysFalse.INSTANCE) {
        return left;
      }
      return FilterApi.or(left, right);
    }

    protected Expression bind(UnboundPredicate<?> pred) {
      return pred.bind(schema.asStruct(), caseSensitive);
    }

    @Override
    public <T> FilterPredicate predicate(BoundPredicate<T> pred) {
      if (!(pred.term() instanceof BoundReference)) {
        throw new UnsupportedOperationException(
            "Cannot convert non-reference to Parquet filter: " + pred.term());
      }

      Operation op = pred.op();
      BoundReference<T> ref = (BoundReference<T>) pred.term();
      String path = schema.idToAlias(ref.fieldId());
      Literal<T> lit;
      if (pred.isUnaryPredicate()) {
        lit = null;
      } else if (pred.isLiteralPredicate()) {
        lit = pred.asLiteralPredicate().literal();
      } else {
        throw new UnsupportedOperationException("Cannot convert to Parquet filter: " + pred);
      }

      switch (ref.type().typeId()) {
        case BOOLEAN:
          Operators.BooleanColumn col = FilterApi.booleanColumn(path);
          switch (op) {
            case EQ:
              return FilterApi.eq(col, getParquetPrimitive(lit));
            case NOT_EQ:
              return FilterApi.notEq(col, getParquetPrimitive(lit));
          }
          break;
        case INTEGER:
        case DATE:
          return pred(op, FilterApi.intColumn(path), getParquetPrimitive(lit));
        case LONG:
        case TIME:
        case TIMESTAMP:
        case TIMESTAMP_NANO:
          return pred(op, FilterApi.longColumn(path), getParquetPrimitive(lit));
        case FLOAT:
          return pred(op, FilterApi.floatColumn(path), getParquetPrimitive(lit));
        case DOUBLE:
          return pred(op, FilterApi.doubleColumn(path), getParquetPrimitive(lit));
        case STRING:
        case FIXED:
        case BINARY:
          return pred(op, FilterApi.binaryColumn(path), getParquetPrimitive(lit));
        case UUID:
          return uuidPred(op, path, lit);
        case DECIMAL:
          return decimalPred(
              op,
              path,
              primitiveTypesById.get(ref.fieldId()),
              (Types.DecimalType) ref.type().asPrimitiveType(),
              lit);
      }

      throw new UnsupportedOperationException("Cannot convert to Parquet filter: " + pred);
    }

    @Override
    public <T> FilterPredicate predicate(UnboundPredicate<T> pred) {
      Expression bound = bind(pred);
      if (bound instanceof BoundPredicate) {
        return predicate((BoundPredicate<?>) bound);
      } else if (bound == Expressions.alwaysTrue()) {
        return AlwaysTrue.INSTANCE;
      } else if (bound == Expressions.alwaysFalse()) {
        return AlwaysFalse.INSTANCE;
      }
      throw new UnsupportedOperationException("Cannot convert to Parquet filter: " + pred);
    }
  }

  private static FilterPredicate uuidPred(Operation op, String path, Literal<?> lit) {
    switch (op) {
      case IS_NULL:
      case NOT_NULL:
      case EQ:
      case NOT_EQ:
        return pred(op, FilterApi.binaryColumn(path), getParquetUUID(lit));
      default:
        // Parquet UUID ordering is unsigned lexicographic, which does not match UUID.compareTo.
        return AlwaysTrue.INSTANCE;
    }
  }

  private static FilterPredicate decimalPred(
      Operation op,
      String path,
      PrimitiveType primitiveType,
      Types.DecimalType decimalType,
      Literal<?> lit) {
    if (primitiveType == null) {
      return AlwaysTrue.INSTANCE;
    }

    BigDecimal decimal = decimalValue(decimalType, lit);
    if (lit != null && decimal == null) {
      return AlwaysTrue.INSTANCE;
    }

    try {
      switch (primitiveType.getPrimitiveTypeName()) {
        case INT32:
          return pred(op, FilterApi.intColumn(path), getDecimalAsInt(decimal));
        case INT64:
          return pred(op, FilterApi.longColumn(path), getDecimalAsLong(decimal));
        case FIXED_LEN_BYTE_ARRAY:
          return pred(
              op,
              FilterApi.binaryColumn(path),
              getDecimalAsFixed(decimalType, primitiveType.getTypeLength(), decimal));
        case BINARY:
          return pred(op, FilterApi.binaryColumn(path), getDecimalAsBinary(decimal));
        default:
          return AlwaysTrue.INSTANCE;
      }
    } catch (ArithmeticException e) {
      return AlwaysTrue.INSTANCE;
    }
  }

  @SuppressWarnings("checkstyle:MethodTypeParameterName")
  private static <C extends Comparable<C>, COL extends Operators.Column<C> & Operators.SupportsLtGt>
      FilterPredicate pred(Operation op, COL col, C value) {
    switch (op) {
      case IS_NULL:
        return FilterApi.eq(col, null);
      case NOT_NULL:
        return FilterApi.notEq(col, null);
      case IS_NAN:
        if (col.getColumnType().equals(Double.class)) {
          return FilterApi.eq(col, (C) (Double) Double.NaN);
        } else if (col.getColumnType().equals(Float.class)) {
          return FilterApi.eq(col, (C) (Float) Float.NaN);
        } else {
          return AlwaysFalse.INSTANCE;
        }
      case NOT_NAN:
        if (col.getColumnType().equals(Double.class)) {
          return FilterApi.notEq(col, (C) (Double) Double.NaN);
        } else if (col.getColumnType().equals(Float.class)) {
          return FilterApi.notEq(col, (C) (Float) Float.NaN);
        } else {
          return AlwaysTrue.INSTANCE;
        }
      case EQ:
        return FilterApi.eq(col, value);
      case NOT_EQ:
        return FilterApi.notEq(col, value);
      case GT:
        return FilterApi.gt(col, value);
      case GT_EQ:
        return FilterApi.gtEq(col, value);
      case LT:
        return FilterApi.lt(col, value);
      case LT_EQ:
        return FilterApi.ltEq(col, value);
      default:
        throw new UnsupportedOperationException("Unsupported predicate operation: " + op);
    }
  }

  private static Integer getDecimalAsInt(BigDecimal decimal) {
    if (decimal == null) {
      return null;
    }

    return decimal.unscaledValue().intValueExact();
  }

  private static Long getDecimalAsLong(BigDecimal decimal) {
    if (decimal == null) {
      return null;
    }

    return decimal.unscaledValue().longValueExact();
  }

  private static Binary getDecimalAsFixed(Types.DecimalType type, int length, BigDecimal decimal) {
    if (decimal == null) {
      return null;
    }

    byte[] bytes =
        DecimalUtil.toReusedFixLengthBytes(
            type.precision(), type.scale(), decimal, new byte[length]);
    return Binary.fromConstantByteArray(bytes);
  }

  private static Binary getDecimalAsBinary(BigDecimal decimal) {
    if (decimal == null) {
      return null;
    }

    return Binary.fromConstantByteArray(decimal.unscaledValue().toByteArray());
  }

  private static BigDecimal decimalValue(Types.DecimalType type, Literal<?> lit) {
    if (lit == null) {
      return null;
    }

    BigDecimal decimal = (BigDecimal) lit.value();
    try {
      BigDecimal scaled = decimal.setScale(type.scale(), RoundingMode.UNNECESSARY);
      return scaled.precision() <= type.precision() ? scaled : null;
    } catch (ArithmeticException e) {
      return null;
    }
  }

  private static Binary getParquetUUID(Literal<?> lit) {
    if (lit == null) {
      return null;
    }

    return Binary.fromConstantByteArray(UUIDUtil.convert((UUID) lit.value()));
  }

  @SuppressWarnings("unchecked")
  private static <C extends Comparable<C>> C getParquetPrimitive(Literal<?> lit) {
    if (lit == null) {
      return null;
    }

    Object value = lit.value();
    if (value instanceof Number) {
      return (C) lit.value();
    } else if (value instanceof CharSequence) {
      return (C) Binary.fromString(value.toString());
    } else if (value instanceof ByteBuffer) {
      return (C) Binary.fromReusedByteBuffer((ByteBuffer) value);
    }
    throw new UnsupportedOperationException(
        "Type not supported yet: " + value.getClass().getName());
  }

  private static class AlwaysTrue implements FilterPredicate {
    static final AlwaysTrue INSTANCE = new AlwaysTrue();

    @Override
    public <R> R accept(Visitor<R> visitor) {
      throw new UnsupportedOperationException("AlwaysTrue is a placeholder only");
    }
  }

  private static class AlwaysFalse implements FilterPredicate {
    static final AlwaysFalse INSTANCE = new AlwaysFalse();

    @Override
    public <R> R accept(Visitor<R> visitor) {
      throw new UnsupportedOperationException("AlwaysTrue is a placeholder only");
    }
  }
}
