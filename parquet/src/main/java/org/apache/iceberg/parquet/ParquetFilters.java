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
import java.util.Collections;
import java.util.Set;
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
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;

class ParquetFilters {

  private ParquetFilters() {}

  static FilterCompat.Filter convert(Schema schema, Expression expr, boolean caseSensitive) {
    FilterPredicate pred =
        ExpressionVisitors.visit(expr, new ConvertFilterToParquet(schema, caseSensitive));
    // TODO: handle AlwaysFalse.INSTANCE
    if (pred != null && pred != AlwaysTrue.INSTANCE) {
      // FilterCompat will apply LogicalInverseRewriter
      return FilterCompat.get(pred);
    } else {
      return FilterCompat.NOOP;
    }
  }

  private static class ConvertFilterToParquet extends ExpressionVisitor<FilterPredicate> {
    private final Schema schema;
    private final boolean caseSensitive;

    private ConvertFilterToParquet(Schema schema, boolean caseSensitive) {
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
      Set<T> litSet;
      if (pred.isUnaryPredicate()) {
        lit = null;
        litSet = null;
      } else if (pred.isLiteralPredicate()) {
        lit = pred.asLiteralPredicate().literal();
        litSet = null;
      } else if (pred.isSetPredicate()) {
        lit = null;
        litSet = pred.asSetPredicate().literalSet();
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
          return pred(
              op,
              FilterApi.intColumn(path),
              getParquetPrimitive(lit),
              getParquetPrimitiveSet(litSet));
        case LONG:
        case TIME:
        case TIMESTAMP:
          return pred(
              op,
              FilterApi.longColumn(path),
              getParquetPrimitive(lit),
              getParquetPrimitiveSet(litSet));
        case FLOAT:
          return pred(
              op,
              FilterApi.floatColumn(path),
              getParquetPrimitive(lit),
              getParquetPrimitiveSet(litSet));
        case DOUBLE:
          return pred(
              op,
              FilterApi.doubleColumn(path),
              getParquetPrimitive(lit),
              getParquetPrimitiveSet(litSet));
        case STRING:
        case UUID:
        case FIXED:
        case BINARY:
        case DECIMAL:
          return pred(
              op,
              FilterApi.binaryColumn(path),
              getParquetPrimitive(lit),
              getParquetPrimitiveSet(litSet));
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

  @SuppressWarnings("checkstyle:MethodTypeParameterName")
  private static <C extends Comparable<C>, COL extends Operators.Column<C> & Operators.SupportsLtGt>
      FilterPredicate pred(Operation op, COL col, C value, Set<C> valueSet) {
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
      case IN:
        return FilterApi.in(col, valueSet);
      case NOT_IN:
        return FilterApi.notIn(col, valueSet);
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
      return (C) value;
    } else if (value instanceof CharSequence) {
      return (C) Binary.fromString(value.toString());
    } else if (value instanceof ByteBuffer) {
      return (C) Binary.fromReusedByteBuffer((ByteBuffer) value);
    }
    throw new UnsupportedOperationException(
        "Type not supported yet: " + value.getClass().getName());
  }

  @SuppressWarnings("unchecked")
  private static <C extends Comparable<C>> Set<C> getParquetPrimitiveSet(Set<?> litSet) {
    if (litSet == null) {
      return Collections.emptySet();
    }

    // TODO: this needs to convert to handle BigDecimal and UUID
    Set<C> convertedSet = Sets.newHashSet();
    for (Object value : litSet) {
      if (value instanceof Number) {
        convertedSet.add((C) value);
      } else if (value instanceof CharSequence) {
        convertedSet.add((C) Binary.fromString(value.toString()));
      } else if (value instanceof ByteBuffer) {
        convertedSet.add((C) Binary.fromReusedByteBuffer((ByteBuffer) value));
      } else {
        throw new UnsupportedOperationException(
            "Type not supported yet: " + value.getClass().getName());
      }
    }
    return convertedSet;
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
