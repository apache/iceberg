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
import org.apache.iceberg.types.Type;
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
      MessageType mType, Schema schema, Expression expr, boolean caseSensitive) {
    FilterPredicate pred =
        ExpressionVisitors.visit(expr, new ConvertFilterToParquet(mType, schema, caseSensitive));
    // TODO: handle AlwaysFalse.INSTANCE
    if (pred != null && pred != AlwaysTrue.INSTANCE) {
      // FilterCompat will apply LogicalInverseRewriter
      return FilterCompat.get(pred);
    } else {
      return FilterCompat.NOOP;
    }
  }

  static FilterCompat.Filter convert(Schema schema, Expression expr, boolean caseSensitive) {
    return convert(null, schema, expr, caseSensitive);
  }

  private static class ConvertFilterToParquet extends ExpressionVisitor<FilterPredicate> {
    private final MessageType mType;
    private final Schema schema;
    private final boolean caseSensitive;

    private ConvertFilterToParquet(MessageType mType, Schema schema, boolean caseSensitive) {
      this.mType = mType;
      this.schema = schema;
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

      String errMsg = "Cannot convert to Parquet filter: " + pred;
      if (mType != null) {
        // We create a Parquet filter predicate and that predicate uses a Parquet column.
        // We need to ensure that the Parquet column type converted from the Iceberg type of
        // the column matches the Parquet type in the Parquet file. (If the filter is passed
        // to Parquet and used by Parquet to filter row groups, Parquet checks that the type
        // in the predicate matches the type in the file as a validation step before filtering.)
        // If the two do not match, we abort the conversion.
        org.apache.parquet.schema.Type pType = mType.getType(path);
        if (!(pType instanceof PrimitiveType)) {
          throw new UnsupportedOperationException(errMsg);
        } else {
          PrimitiveType.PrimitiveTypeName typeName = ((PrimitiveType) pType).getPrimitiveTypeName();
          String expected = predicateType(typeName);
          String actual = predicateType(ref.type().typeId());
          if (!actual.equals(expected)) {
            throw new UnsupportedOperationException(errMsg);
          }
        }
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
          return pred(op, FilterApi.longColumn(path), getParquetPrimitive(lit));
        case FLOAT:
          return pred(op, FilterApi.floatColumn(path), getParquetPrimitive(lit));
        case DOUBLE:
          return pred(op, FilterApi.doubleColumn(path), getParquetPrimitive(lit));
        case STRING:
        case UUID:
        case FIXED:
        case BINARY:
        case DECIMAL:
          return pred(op, FilterApi.binaryColumn(path), getParquetPrimitive(lit));
      }

      throw new UnsupportedOperationException(errMsg);
    }

    private String predicateType(PrimitiveType.PrimitiveTypeName typeName) {
      switch (typeName) {
        case BOOLEAN:
          return "boolean";
        case INT32:
          return "int";
        case INT64:
          return "long";
        case FLOAT:
          return "float";
        case DOUBLE:
          return "double";
        case INT96:
        case FIXED_LEN_BYTE_ARRAY:
        case BINARY:
          return "binary";
        default:
          return "unsupported";
      }
    }

    private String predicateType(Type.TypeID typeId) {
      switch (typeId) {
        case BOOLEAN:
          return "boolean";
        case INTEGER:
        case DATE:
          return "int";
        case LONG:
        case TIME:
        case TIMESTAMP:
          return "long";
        case FLOAT:
          return "float";
        case DOUBLE:
          return "double";
        case STRING:
        case UUID:
        case FIXED:
        case BINARY:
        case DECIMAL:
          return "binary";
        default:
          return "unsupported";
      }
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

  @SuppressWarnings("unchecked")
  private static <C extends Comparable<C>> C getParquetPrimitive(Literal<?> lit) {
    if (lit == null) {
      return null;
    }

    // TODO: this needs to convert to handle BigDecimal and UUID
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
