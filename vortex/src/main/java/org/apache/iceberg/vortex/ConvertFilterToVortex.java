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
package org.apache.iceberg.vortex;

import dev.vortex.api.Expression;
import dev.vortex.api.Expression.BinaryOp;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Type;

/**
 * Convert an Iceberg filter expression into a valid Vortex pruning predicate that can be pushed
 * into the scan node.
 *
 * <p>Filters that cannot be translated will default to {@code ALWAYS_TRUE} to be skipped.
 */
public final class ConvertFilterToVortex extends ExpressionVisitors.ExpressionVisitor<Expression> {
  static final Expression ALWAYS_TRUE = Expression.literal(true);
  static final Expression ALWAYS_FALSE = Expression.literal(false);
  // Sentinel distinct by reference from ALWAYS_TRUE/FALSE, used to mark
  // sub-expressions that
  // could not be translated to a Vortex expression.
  static final Expression UNCONVERTIBLE = Expression.literal(true);

  private static final int SET_PREDICATE_LIMIT = 200;

  private final Schema fileSchema;
  private final boolean caseSensitive;

  private ConvertFilterToVortex(Schema fileSchema, boolean caseSensitive) {
    this.fileSchema = fileSchema;
    this.caseSensitive = caseSensitive;
  }

  public static Expression convert(Schema schema, org.apache.iceberg.expressions.Expression expr) {
    return convert(schema, expr, true);
  }

  public static Expression convert(
      Schema schema, org.apache.iceberg.expressions.Expression expr, boolean caseSensitive) {
    org.apache.iceberg.expressions.Expression pushedNot = Expressions.rewriteNot(expr);
    Expression converted =
        ExpressionVisitors.visit(pushedNot, new ConvertFilterToVortex(schema, caseSensitive));
    if (converted == UNCONVERTIBLE) {
      return ALWAYS_TRUE;
    } else {
      return converted;
    }
  }

  @Override
  public Expression alwaysFalse() {
    return ALWAYS_FALSE;
  }

  @Override
  public Expression alwaysTrue() {
    return ALWAYS_TRUE;
  }

  @Override
  public Expression not(Expression child) {
    if (child == ALWAYS_TRUE) {
      return ALWAYS_FALSE;
    } else if (child == ALWAYS_FALSE) {
      return ALWAYS_TRUE;
    } else if (child == UNCONVERTIBLE) {
      return UNCONVERTIBLE;
    } else {
      return Expression.not(child);
    }
  }

  @Override
  public Expression and(Expression leftResult, Expression rightResult) {
    if (leftResult == UNCONVERTIBLE && rightResult == UNCONVERTIBLE) {
      return ALWAYS_TRUE;
    } else if (leftResult == UNCONVERTIBLE) {
      return rightResult;
    } else if (rightResult == UNCONVERTIBLE) {
      return leftResult;
    } else {
      return Expression.and(leftResult, rightResult);
    }
  }

  @Override
  public Expression or(Expression leftResult, Expression rightResult) {
    if (leftResult == UNCONVERTIBLE || rightResult == UNCONVERTIBLE) {
      return ALWAYS_TRUE;
    } else {
      return Expression.or(leftResult, rightResult);
    }
  }

  @Override
  public <T> Expression predicate(BoundPredicate<T> pred) {
    if (!(pred.term() instanceof BoundReference<T> term)) {
      throw new UnsupportedOperationException(
          "Cannot convert non-reference to Vortex filter: " + pred.term());
    }

    String name = term.ref().field().name();
    if (pred.isLiteralPredicate()) {
      org.apache.iceberg.expressions.Literal<T> icebergLit = pred.asLiteralPredicate().literal();
      Expression vortexLit = toVortexLiteral(icebergLit.value(), term.type());
      if (vortexLit == UNCONVERTIBLE) {
        return UNCONVERTIBLE;
      }
      Expression vortexTerm = Expression.column(name);
      return fromBinaryPredicate(pred.op(), vortexTerm, vortexLit);
    } else if (pred.isUnaryPredicate()) {
      Expression vortexTerm = Expression.column(name);
      return fromUnaryPredicate(pred.op(), vortexTerm);
    } else if (pred.isSetPredicate()) {
      Set<T> literalSet = pred.asSetPredicate().literalSet();
      if (literalSet.size() > SET_PREDICATE_LIMIT) {
        return UNCONVERTIBLE;
      }

      Expression vortexTerm = Expression.column(name);
      return fromSetPredicate(pred.op(), vortexTerm, literalSet, term.type());
    } else {
      return UNCONVERTIBLE;
    }
  }

  @Override
  public <T> Expression predicate(UnboundPredicate<T> pred) {
    org.apache.iceberg.expressions.Expression bound = bind(pred);
    if (bound instanceof BoundPredicate) {
      return predicate((BoundPredicate<?>) bound);
    } else if (bound == Expressions.alwaysTrue()) {
      return ALWAYS_TRUE;
    } else if (bound == Expressions.alwaysFalse()) {
      return ALWAYS_FALSE;
    }
    return UNCONVERTIBLE;
  }

  private org.apache.iceberg.expressions.Expression bind(UnboundPredicate<?> pred) {
    return pred.bind(fileSchema.asStruct(), caseSensitive);
  }

  /**
   * Convert an Iceberg value to a Vortex literal Expression. Returns {@link #UNCONVERTIBLE} if no
   * matching Vortex literal type exists (binary, decimal, date, time, timestamp, uuid).
   */
  private Expression toVortexLiteral(Object value, Type termType) {
    if (value == null) {
      return UNCONVERTIBLE;
    }
    return switch (termType.typeId()) {
      case BOOLEAN -> Expression.literal((Boolean) value);
      case INTEGER -> Expression.literal((Integer) value);
      case LONG -> Expression.literal((Long) value);
      case FLOAT -> Expression.literal((Float) value);
      case DOUBLE -> Expression.literal((Double) value);
      case STRING -> Expression.literal(((CharSequence) value).toString());
      default -> UNCONVERTIBLE;
    };
  }

  private Expression fromBinaryPredicate(
      org.apache.iceberg.expressions.Expression.Operation op, Expression left, Expression right) {
    return switch (op) {
      case TRUE -> ALWAYS_TRUE;
      case FALSE -> ALWAYS_FALSE;
      case LT -> Expression.binary(BinaryOp.LT, left, right);
      case LT_EQ -> Expression.binary(BinaryOp.LTE, left, right);
      case GT -> Expression.binary(BinaryOp.GT, left, right);
      case GT_EQ -> Expression.binary(BinaryOp.GTE, left, right);
      case EQ -> Expression.binary(BinaryOp.EQ, left, right);
      case NOT_EQ -> Expression.binary(BinaryOp.NOT_EQ, left, right);
      case AND -> Expression.binary(BinaryOp.AND, left, right);
      case OR -> Expression.binary(BinaryOp.OR, left, right);
      default -> UNCONVERTIBLE;
    };
  }

  private Expression fromUnaryPredicate(
      org.apache.iceberg.expressions.Expression.Operation op, Expression child) {
    return switch (op) {
      case TRUE -> ALWAYS_TRUE;
      case FALSE -> ALWAYS_FALSE;
      case IS_NULL -> Expression.isNull(child);
      case NOT_NULL -> Expression.not(Expression.isNull(child));
      case NOT -> {
        if (child == ALWAYS_TRUE) {
          yield ALWAYS_FALSE;
        } else if (child == ALWAYS_FALSE) {
          yield ALWAYS_TRUE;
        } else {
          yield Expression.not(child);
        }
      }
      default -> UNCONVERTIBLE;
    };
  }

  private <T> Expression fromSetPredicate(
      org.apache.iceberg.expressions.Expression.Operation op,
      Expression term,
      Set<T> literalSet,
      Type termType) {
    Expression[] eqExprs = new Expression[literalSet.size()];
    int idx = 0;
    for (T value : literalSet) {
      Expression vortexLit = toVortexLiteral(value, termType);
      if (vortexLit == UNCONVERTIBLE) {
        return UNCONVERTIBLE;
      }
      eqExprs[idx++] = Expression.binary(BinaryOp.EQ, term, vortexLit);
    }

    return switch (op) {
      case IN -> eqExprs.length == 1 ? eqExprs[0] : Expression.or(eqExprs);
      case NOT_IN -> Expression.not(eqExprs.length == 1 ? eqExprs[0] : Expression.or(eqExprs));
      default -> UNCONVERTIBLE;
    };
  }
}
