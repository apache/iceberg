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
package org.apache.iceberg.tornadovm;

import java.util.List;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import uk.ac.manchester.tornado.api.types.arrays.DoubleArray;
import uk.ac.manchester.tornado.api.types.arrays.IntArray;
import uk.ac.manchester.tornado.api.types.arrays.LongArray;
import uk.ac.manchester.tornado.cudf.enums.CudfType;

/**
 * A scan's residual row filter, compiled to a table of terms a TornadoVM kernel evaluates.
 *
 * <p>Iceberg prunes files with the filter while it plans; what it cannot decide from statistics is
 * the residual, applied here to every live row. The residual is a conjunction of terms, each a
 * column compared with constants -- {@code < <= > >= = !=}, {@code IN}, {@code IS NULL}, {@code IS
 * NOT NULL} -- with the semantics of Iceberg's own row evaluator ({@code Evaluator}), whose literal
 * comparators order nulls first: a null is below every value, so it satisfies {@code <}, {@code <=}
 * and {@code !=} and nothing else. (An engine that wants SQL's semantics adds {@code IS NOT NULL}
 * terms itself, as Spark does.) Anything else ({@code OR}, {@code NOT}, a transform, a string
 * column) is refused rather than evaluated wrongly.
 *
 * <p>Encoding: {@code terms[0]} is the term count, then {@value #TERM_INTS} ints a term: column
 * kind ({@link CudfType#code()}), operation, offset of the column in its typed buffer, its column
 * index in the validity bytes or -1, first constant, number of constants. Constants of INT and LONG
 * columns are in the long table, of DOUBLE columns in the double table.
 */
final class DevicePredicate {

  static final int TERM_INTS = 6;
  static final int MAX_TERMS = 32;
  static final int MAX_CONSTANTS = 4096;

  static final int OP_LT = 0;
  static final int OP_LT_EQ = 1;
  static final int OP_GT = 2;
  static final int OP_GT_EQ = 3;
  static final int OP_EQ = 4;
  static final int OP_NOT_EQ = 5;
  static final int OP_IN = 6;
  static final int OP_IS_NULL = 7;
  static final int OP_NOT_NULL = 8;
  static final int OP_FALSE = 9;

  /** One compiled term, before it is placed in the buffers of a particular layout. */
  static final class Term {
    private final int fieldId;
    private final CudfType kind;
    private final int op;
    private final long[] longs;
    private final double[] doubles;

    Term(int fieldId, CudfType kind, int op, long[] longs, double[] doubles) {
      this.fieldId = fieldId;
      this.kind = kind;
      this.op = op;
      this.longs = longs;
      this.doubles = doubles;
    }

    int fieldId() {
      return fieldId;
    }

    CudfType kind() {
      return kind;
    }

    int op() {
      return op;
    }

    long[] longs() {
      return longs;
    }

    double[] doubles() {
      return doubles;
    }
  }

  private final List<Term> terms;

  private DevicePredicate(List<Term> terms) {
    this.terms = terms;
  }

  static DevicePredicate alwaysTrue() {
    return new DevicePredicate(ImmutableList.of());
  }

  List<Term> terms() {
    return terms;
  }

  /** The field ids a filter reads, which the scan has to project. */
  static Set<Integer> fieldIds(Expression filter, Schema schema) {
    return Binder.boundReferences(schema.asStruct(), ImmutableList.of(filter), true);
  }

  /** Compiles a residual, bound or not, against the table schema. */
  static DevicePredicate compile(Expression residual, Schema schema) {
    Expression bound = residual;
    if (containsUnbound(residual)) {
      bound = Binder.bind(schema.asStruct(), residual, true);
    }
    List<Term> terms = ExpressionVisitors.visit(bound, new Compiler());
    if (terms.size() > MAX_TERMS) {
      throw new UnsupportedOperationException("A filter of more than " + MAX_TERMS + " terms");
    }
    int constants = terms.stream().mapToInt(t -> Math.max(t.longs.length, t.doubles.length)).sum();
    if (constants > MAX_CONSTANTS) {
      throw new UnsupportedOperationException(
          "A filter of more than " + MAX_CONSTANTS + " constants");
    }
    return new DevicePredicate(terms);
  }

  private static boolean containsUnbound(Expression expr) {
    return ExpressionVisitors.visit(
        expr,
        new ExpressionVisitors.ExpressionVisitor<Boolean>() {
          @Override
          public Boolean alwaysTrue() {
            return false;
          }

          @Override
          public Boolean alwaysFalse() {
            return false;
          }

          @Override
          public Boolean not(Boolean result) {
            return result;
          }

          @Override
          public Boolean and(Boolean left, Boolean right) {
            return left || right;
          }

          @Override
          public Boolean or(Boolean left, Boolean right) {
            return left || right;
          }

          @Override
          public <T> Boolean predicate(BoundPredicate<T> pred) {
            return false;
          }

          @Override
          public <T> Boolean predicate(UnboundPredicate<T> pred) {
            return true;
          }
        });
  }

  /** Writes the terms for one layout and stride into the kernel's buffers. */
  void encode(
      ScanLayout layout,
      Schema schema,
      int stride,
      IntArray encoded,
      LongArray longConstants,
      DoubleArray doubleConstants) {
    encoded.set(0, terms.size());
    int longCursor = 0;
    int doubleCursor = 0;
    for (int t = 0; t < terms.size(); t++) {
      Term term = terms.get(t);
      int base = 1 + t * TERM_INTS;
      encoded.set(base, term.kind == null ? 0 : term.kind.code());
      encoded.set(base + 1, term.op);
      if (term.op == OP_FALSE) {
        encoded.set(base + 2, 0);
        encoded.set(base + 3, -1);
        encoded.set(base + 4, 0);
        encoded.set(base + 5, 0);
        continue;
      }
      Types.NestedField field = schema.findField(term.fieldId);
      encoded.set(base + 2, layout.slot(term.fieldId) * stride);
      encoded.set(
          base + 3,
          layout.anyOptional() && field.isOptional() ? layout.readIndex(term.fieldId) : -1);
      if (term.kind == CudfType.FLOAT64) {
        encoded.set(base + 4, doubleCursor);
        encoded.set(base + 5, term.doubles.length);
        for (double value : term.doubles) {
          doubleConstants.set(doubleCursor++, value);
        }
      } else {
        encoded.set(base + 4, longCursor);
        encoded.set(base + 5, term.longs.length);
        for (long value : term.longs) {
          longConstants.set(longCursor++, value);
        }
      }
    }
  }

  /** Turns a bound conjunction into terms; refuses what the kernel cannot evaluate. */
  private static final class Compiler extends ExpressionVisitors.ExpressionVisitor<List<Term>> {
    @Override
    public List<Term> alwaysTrue() {
      return Lists.newArrayList();
    }

    @Override
    public List<Term> alwaysFalse() {
      List<Term> terms = Lists.newArrayList();
      terms.add(new Term(-1, null, OP_FALSE, new long[0], new double[0]));
      return terms;
    }

    @Override
    public List<Term> not(List<Term> result) {
      throw new UnsupportedOperationException("NOT is not supported in a device filter");
    }

    @Override
    public List<Term> and(List<Term> left, List<Term> right) {
      List<Term> terms = Lists.newArrayList(left);
      terms.addAll(right);
      return terms;
    }

    @Override
    public List<Term> or(List<Term> left, List<Term> right) {
      throw new UnsupportedOperationException("OR is not supported in a device filter");
    }

    @Override
    public <T> List<Term> predicate(UnboundPredicate<T> pred) {
      throw new IllegalStateException("Unbound predicate after binding: " + pred);
    }

    @Override
    public <T> List<Term> predicate(BoundPredicate<T> pred) {
      if (!(pred.term() instanceof BoundReference)) {
        throw new UnsupportedOperationException("Only plain column references: " + pred);
      }
      Types.NestedField field = pred.ref().field();
      CudfType kind = ScanLayout.cudfType(field);
      int op;
      List<Object> values = Lists.newArrayList();
      switch (pred.op()) {
        case LT -> op = OP_LT;
        case LT_EQ -> op = OP_LT_EQ;
        case GT -> op = OP_GT;
        case GT_EQ -> op = OP_GT_EQ;
        case EQ -> op = OP_EQ;
        case NOT_EQ -> op = OP_NOT_EQ;
        case IN -> op = OP_IN;
        case IS_NULL -> op = OP_IS_NULL;
        case NOT_NULL -> op = OP_NOT_NULL;
        default ->
            throw new UnsupportedOperationException(
                "Operation " + pred.op() + " is not supported in a device filter");
      }
      if (pred.isLiteralPredicate()) {
        values.add(pred.asLiteralPredicate().literal().value());
      } else if (pred.isSetPredicate()) {
        values.addAll(pred.asSetPredicate().literalSet());
      }
      long[] longs = new long[kind == CudfType.FLOAT64 ? 0 : values.size()];
      double[] doubles = new double[kind == CudfType.FLOAT64 ? values.size() : 0];
      for (int i = 0; i < values.size(); i++) {
        Object value = values.get(i);
        if (value instanceof CharSequence string) {
          longs[i] = ScanLayout.packString(string);
        } else if (kind == CudfType.FLOAT64) {
          doubles[i] = ((Number) value).doubleValue();
        } else {
          longs[i] = ScanLayout.deviceLong(field, value);
        }
      }
      List<Term> terms = Lists.newArrayList();
      terms.add(new Term(field.fieldId(), kind, op, longs, doubles));
      return terms;
    }
  }
}
