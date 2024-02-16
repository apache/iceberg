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
import java.util.Iterator;
import java.util.NavigableSet;
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
import org.apache.iceberg.types.Type;
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
      Literal<T> lit = null;
      Set<T> litSet = null;
      if (pred.isUnaryPredicate()) {
        lit = null;
      } else if (pred.isLiteralPredicate()) {
        lit = pred.asLiteralPredicate().literal();
      } else if (pred.isSetPredicate()) {
        litSet = pred.asSetPredicate().asSetPredicate().literalSet();
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
          Operators.IntColumn intCol = FilterApi.intColumn(path);
          if (op == Operation.RANGE_IN) {
            return FilterApi.userDefined(
                intCol, new RangeInFilter((NavigableSet)litSet, ref.comparator()));
          } else {
            return pred(op, intCol, getParquetPrimitive(lit));
          }
        case LONG:
        case TIME:
        case TIMESTAMP:
          Operators.LongColumn longCol = FilterApi.longColumn(path);
          if (op == Operation.RANGE_IN) {
            return FilterApi.userDefined(
                longCol, new RangeInFilter((NavigableSet)litSet, ref.comparator()));
          } else {
            return pred(op, longCol, getParquetPrimitive(lit));
          }

        case FLOAT:
          Operators.FloatColumn floatCol = FilterApi.floatColumn(path);
          if (op == Operation.RANGE_IN) {
            return FilterApi.userDefined(
                floatCol, new RangeInFilter((NavigableSet)litSet, ref.comparator()));
          } else {
            return pred(op, floatCol, getParquetPrimitive(lit));
          }

        case DOUBLE:
          Operators.DoubleColumn doubleCol = FilterApi.doubleColumn(path);
          if (op == Operation.RANGE_IN) {
            return FilterApi.userDefined(
                doubleCol, new RangeInFilter((NavigableSet)litSet, ref.comparator()));
          } else {
            return pred(op, doubleCol, getParquetPrimitive(lit));
          }

        case STRING:
        case UUID:
        case FIXED:
        case BINARY:
        case DECIMAL:
          Operators.BinaryColumn binaryColumn = FilterApi.binaryColumn(path);
          if (op == Operation.RANGE_IN) {
            if (ref.type().typeId() == Type.TypeID.STRING) {
              return FilterApi.userDefined(
                      binaryColumn, new RangeInFilter(convertStringSetToParquet(
                              (Set<String>)litSet), ref.comparator()));
            } else {
              return FilterApi.userDefined(
                      binaryColumn, new RangeInFilter((NavigableSet)litSet, ref.comparator()));
            }

          } else {
            return pred(op, binaryColumn, getParquetPrimitive(lit));
          }
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

  /*
  @SuppressWarnings("checkstyle:MethodTypeParameterName")
  private static <
          C extends Comparable<C>, COL extends Operators.Column<C> & Operators.SupportsEqNotEq>
      FilterPredicate predIn(COL col, Set<Literal<?>> value) {
    return FilterApi.in(
        col, value.stream().map(x -> (C) getParquetPrimitive(x)).collect(Collectors.toSet()));
  }
  */

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

  @SuppressWarnings("unchecked")
  private static <C extends Comparable<C>> NavigableSet<C> convertStringSetToParquet(Set<C> set) {
    if (set.isEmpty()) {
      return (NavigableSet<C>) set;
    } else {
      Iterator<C> iter = set.iterator();
      NavigableSet<C> tempSet = Sets.newTreeSet();
      iter.forEachRemaining(x -> tempSet.add((C)Binary.fromString(x.toString())));
      return tempSet;
    }
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
