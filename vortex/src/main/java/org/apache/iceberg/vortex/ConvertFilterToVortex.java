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
import dev.vortex.api.expressions.Binary;
import dev.vortex.api.expressions.GetItem;
import dev.vortex.api.expressions.Literal;
import dev.vortex.api.expressions.Not;
import dev.vortex.api.expressions.Root;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Convert an Iceberg filter expression into a valid Vortex pruning predicate that can be pushed
 * into the scan node.
 *
 * <p>Filters that cannot be translated will default to {@code ALWAYS_TRUE} to be skipped.
 */
public final class ConvertFilterToVortex extends ExpressionVisitors.ExpressionVisitor<Expression> {
  private static final Expression ALWAYS_TRUE = Literal.bool(true);
  private static final Expression ALWAYS_FALSE = Literal.bool(false);

  private static final int SET_PREDICATE_LIMIT = 200;
  private static final boolean CASE_SENSITIVE = true;

  private final Schema fileSchema;

  private ConvertFilterToVortex(Schema fileSchema) {
    this.fileSchema = fileSchema;
  }

  public static Expression convert(Schema schema, org.apache.iceberg.expressions.Expression expr) {
    org.apache.iceberg.expressions.Expression pushedNot = Expressions.rewriteNot(expr);
    Expression converted = ExpressionVisitors.visit(pushedNot, new ConvertFilterToVortex(schema));
    if (converted == UnconvertibleExpr.INSTANCE) {
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
    // Simplify ALWAYS_TRUE and ALWAYS_FALSE directly so that
    // they avoid any compute in Vortex.
    if (child == ALWAYS_TRUE) {
      return ALWAYS_FALSE;
    } else if (child == ALWAYS_FALSE) {
      return ALWAYS_TRUE;
    } else if (child == UnconvertibleExpr.INSTANCE) {
      // propagate convertibility
      return UnconvertibleExpr.INSTANCE;
    } else {
      return Not.of(child);
    }
  }

  @Override
  public Expression and(Expression leftResult, Expression rightResult) {
    if (leftResult == UnconvertibleExpr.INSTANCE && rightResult == UnconvertibleExpr.INSTANCE) {
      return ALWAYS_TRUE;
    } else if (leftResult == UnconvertibleExpr.INSTANCE) {
      return rightResult;
    } else if (rightResult == UnconvertibleExpr.INSTANCE) {
      return leftResult;
    } else {
      return Binary.and(leftResult, rightResult);
    }
  }

  @Override
  public Expression or(Expression leftResult, Expression rightResult) {
    if (leftResult == UnconvertibleExpr.INSTANCE || rightResult == UnconvertibleExpr.INSTANCE) {
      return ALWAYS_TRUE;
    } else {
      return Binary.or(leftResult, rightResult);
    }
  }

  @Override
  public <T> Expression predicate(BoundPredicate<T> pred) {
    if (!(pred.term() instanceof BoundReference<T> term)) {
      throw new UnsupportedOperationException(
          "Cannot convert non-reference to Parquet filter: " + pred.term());
    }

    if (pred.isLiteralPredicate()) {
      org.apache.iceberg.expressions.Literal<T> icebergLit = pred.asLiteralPredicate().literal();
      Literal<?> vortexLit = toVortexLiteral(icebergLit, term.type());
      // Term translates into a GetItem(Identity), i.e. get a field from the batch
      GetItem vortexTerm = GetItem.of(Root.INSTANCE, term.ref().field().name());
      return fromBinaryPredicate(pred.op(), vortexTerm, vortexLit);
    } else if (pred.isUnaryPredicate()) {
      GetItem vortexTerm = GetItem.of(Root.INSTANCE, term.ref().field().name());
      return fromUnaryPredicate(pred.op(), vortexTerm);
    } else if (pred.isSetPredicate()) {
      Set<T> literalSet = pred.asSetPredicate().literalSet();
      if (literalSet.size() > SET_PREDICATE_LIMIT) {
        return UnconvertibleExpr.INSTANCE;
      }

      GetItem vortexTerm = GetItem.of(Root.INSTANCE, term.ref().field().name());
      return fromSetPredicate(pred.op(), vortexTerm, literalSet, term.type());
    } else {
      return UnconvertibleExpr.INSTANCE;
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
    return UnconvertibleExpr.INSTANCE;
  }

  private org.apache.iceberg.expressions.Expression bind(UnboundPredicate<?> pred) {
    return pred.bind(fileSchema.asStruct(), CASE_SENSITIVE);
  }

  Literal<?> toVortexLiteral(org.apache.iceberg.expressions.Literal<?> literal, Type termType) {
    switch (termType.typeId()) {
      case BOOLEAN -> {
        return Literal.bool((Boolean) literal.value());
      }
      case INTEGER -> {
        return Literal.int32((Integer) literal.value());
      }
      case LONG -> {
        return Literal.int64((Long) literal.value());
      }
      case FLOAT -> {
        return Literal.float32((Float) literal.value());
      }
      case DOUBLE -> {
        return Literal.float64((Double) literal.value());
      }
      case DECIMAL -> {
        Types.DecimalType decimalType = (Types.DecimalType) termType;
        return Literal.decimal(
            (BigDecimal) literal.value(), decimalType.precision(), decimalType.scale());
      }
      case STRING -> {
        CharSequence charSequence = (CharSequence) literal.value();
        if (Objects.isNull(charSequence)) {
          return Literal.string(null);
        } else {
          return Literal.string(charSequence.toString());
        }
      }
      case BINARY -> {
        ByteBuffer byteBuffer = (ByteBuffer) literal.value();
        if (Objects.isNull(byteBuffer)) {
          return Literal.bytes(null);
        } else {
          byte[] bytes = new byte[byteBuffer.remaining()];
          byteBuffer.get(bytes);
          return Literal.bytes(bytes);
        }
      }
      case UUID -> {
        UUID uuid = (UUID) literal.value();
        if (Objects.isNull(uuid)) {
          return Literal.string(null);
        } else {
          return Literal.string(uuid.toString());
        }
      }
      case TIME -> {
        return Literal.timeMicros((Long) literal.value());
      }
      case DATE -> {
        return Literal.dateDays((Integer) literal.value());
      }
      case TIMESTAMP -> {
        Types.TimestampType timestampType = (Types.TimestampType) termType;
        if (timestampType.shouldAdjustToUTC()) {
          throw new UnsupportedOperationException(
              "Handling of timestamps with timezones not yet supported");
        } else {
          // Iceberg always stores timestamp in microseconds.
          // TODO(aduffy): get the Vortex type so we know if we need to convert to different
          // precision.
          return Literal.timestampMicros((Long) literal.value(), Optional.empty());
        }
      }
      default -> throw new UnsupportedOperationException("Unsupported Literal type: " + termType);
    }
  }

  // Always true is not a real binary op...
  Expression fromBinaryPredicate(
      org.apache.iceberg.expressions.Expression.Operation op, Expression left, Expression right) {

    if (left == UnconvertibleExpr.INSTANCE && right == UnconvertibleExpr.INSTANCE) {
      return UnconvertibleExpr.INSTANCE;
    } else if (left == UnconvertibleExpr.INSTANCE) {
      return right;
    }

    return switch (op) {
      case TRUE -> ALWAYS_TRUE;
      case FALSE -> ALWAYS_FALSE;
      case LT -> Binary.lt(left, right);
      case LT_EQ -> Binary.ltEq(left, right);
      case GT -> Binary.gt(left, right);
      case GT_EQ -> Binary.gtEq(left, right);
      case EQ -> Binary.eq(left, right);
      case NOT_EQ -> Binary.notEq(left, right);
      case AND -> Binary.and(left, right);
      case OR -> Binary.or(left, right);
      default -> UnconvertibleExpr.INSTANCE;
    };
  }

  Expression fromUnaryPredicate(
      org.apache.iceberg.expressions.Expression.Operation op, Expression child) {
    switch (op) {
      case TRUE -> {
        return ALWAYS_TRUE;
      }
      case FALSE -> {
        return ALWAYS_FALSE;
      }
      case NOT -> {
        if (child == ALWAYS_TRUE) {
          return ALWAYS_FALSE;
        } else if (child == ALWAYS_FALSE) {
          return ALWAYS_TRUE;
        } else {
          return Not.of(child);
        }
      }
      default -> {
        return ALWAYS_TRUE;
      }
    }
  }

  <T> Expression fromSetPredicate(
      org.apache.iceberg.expressions.Expression.Operation op,
      GetItem term,
      Set<T> literalSet,
      Type termType) {
    Expression[] eqExprs =
        literalSet.stream()
            .map(value -> (Expression) Binary.eq(term, toVortexValue(value, termType)))
            .toArray(Expression[]::new);

    return switch (op) {
      case IN -> Binary.or(eqExprs[0], java.util.Arrays.copyOfRange(eqExprs, 1, eqExprs.length));
      case NOT_IN ->
          Not.of(Binary.or(eqExprs[0], java.util.Arrays.copyOfRange(eqExprs, 1, eqExprs.length)));
      default -> UnconvertibleExpr.INSTANCE;
    };
  }

  @SuppressWarnings("unchecked")
  private <T> Literal<?> toVortexValue(T value, Type termType) {
    return switch (termType.typeId()) {
      case BOOLEAN -> Literal.bool((Boolean) value);
      case INTEGER -> Literal.int32((Integer) value);
      case LONG -> Literal.int64((Long) value);
      case FLOAT -> Literal.float32((Float) value);
      case DOUBLE -> Literal.float64((Double) value);
      case DECIMAL -> {
        Types.DecimalType decimalType = (Types.DecimalType) termType;
        yield Literal.decimal((BigDecimal) value, decimalType.precision(), decimalType.scale());
      }
      case STRING -> {
        CharSequence charSequence = (CharSequence) value;
        yield Literal.string(charSequence.toString());
      }
      case DATE -> Literal.dateDays((Integer) value);
      case TIME -> Literal.timeMicros((Long) value);
      case TIMESTAMP -> {
        Types.TimestampType timestampType = (Types.TimestampType) termType;
        if (timestampType.shouldAdjustToUTC()) {
          throw new UnsupportedOperationException(
              "Handling of timestamps with timezones not yet supported");
        }
        yield Literal.timestampMicros((Long) value, Optional.empty());
      }
      default ->
          throw new UnsupportedOperationException(
              "Unsupported type for set predicate: " + termType);
    };
  }

  enum UnconvertibleExpr implements Expression {
    INSTANCE;

    @Override
    public String id() {
      return "unconvertible";
    }

    @Override
    public List<Expression> children() {
      return List.of();
    }

    @Override
    public Optional<byte[]> metadata() {
      return Optional.empty();
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
      return null;
    }
  }
}
