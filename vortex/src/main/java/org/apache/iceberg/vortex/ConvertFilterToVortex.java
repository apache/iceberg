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
import dev.vortex.api.expressions.Identity;
import dev.vortex.api.expressions.Literal;
import dev.vortex.api.expressions.Not;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
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

  private static final boolean CASE_SENSITIVE = true;

  private final Schema fileSchema;

  private ConvertFilterToVortex(Schema fileSchema) {
    this.fileSchema = fileSchema;
  }

  public static Expression convert(Schema schema, org.apache.iceberg.expressions.Expression expr) {
    return ExpressionVisitors.visit(expr, new ConvertFilterToVortex(schema));
  }

  @Override
  public Expression alwaysFalse() {
    return ALWAYS_FALSE;
  }

  @Override
  public Expression alwaysTrue() {
    return ALWAYS_FALSE;
  }

  @Override
  public Expression not(Expression child) {
    // Simplify ALWAYS_TRUE and ALWAYS_FALSE directly so that
    // they avoid any compute in Vortex.
    if (child == ALWAYS_TRUE) {
      return ALWAYS_FALSE;
    } else if (child == ALWAYS_FALSE) {
      return ALWAYS_TRUE;
    } else {
      return Not.of(child);
    }
  }

  @Override
  public Expression and(Expression leftResult, Expression rightResult) {
    return Binary.and(leftResult, rightResult);
  }

  @Override
  public Expression or(Expression leftResult, Expression rightResult) {
    return Binary.or(leftResult, rightResult);
  }

  @Override
  public <T> Expression predicate(BoundPredicate<T> pred) {
    if (!(pred.term() instanceof BoundReference)) {
      throw new UnsupportedOperationException(
          "Cannot convert non-reference to Parquet filter: " + pred.term());
    }

    BoundReference<T> term = (BoundReference<T>) pred.term();

    if (pred.isLiteralPredicate()) {
      org.apache.iceberg.expressions.Literal<T> icebergLit = pred.asLiteralPredicate().literal();
      Literal<?> vortexLit = toVortexLiteral(icebergLit, term.type());
      // Term translates into a GetItem(Identity), i.e. get a field from the batch
      GetItem vortexTerm = GetItem.of(Identity.INSTANCE, term.ref().field().name());
      return fromBinaryPredicate(pred.op(), vortexTerm, vortexLit);
    } else if (pred.isUnaryPredicate()) {
      GetItem vortexTerm = GetItem.of(Identity.INSTANCE, term.ref().field().name());
      return fromUnaryPredicate(pred.op(), vortexTerm);
    } else {
      // Set predicates are not supported currently.
      return ALWAYS_TRUE;
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
    throw new UnsupportedOperationException("Cannot convert to Vortex filter: " + pred);
  }

  private org.apache.iceberg.expressions.Expression bind(UnboundPredicate<?> pred) {
    return pred.bind(fileSchema.asStruct(), CASE_SENSITIVE);
  }

  Literal<?> toVortexLiteral(org.apache.iceberg.expressions.Literal<?> literal, Type termType) {
    switch (termType.typeId()) {
      case BOOLEAN:
        return Literal.bool((Boolean) literal.value());
      case INTEGER:
        return Literal.int32((Integer) literal.value());
      case LONG:
        return Literal.int64((Long) literal.value());
      case FLOAT:
        return Literal.float32((Float) literal.value());
      case DOUBLE:
        return Literal.float64((Double) literal.value());
      case STRING:
        {
          CharSequence charSequence = (CharSequence) literal.value();
          if (Objects.isNull(charSequence)) {
            return Literal.string(null);
          } else {
            return Literal.string(charSequence.toString());
          }
        }
      case BINARY:
        {
          ByteBuffer byteBuffer = (ByteBuffer) literal.value();
          if (Objects.isNull(byteBuffer)) {
            return Literal.bytes(null);
          } else {
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            return Literal.bytes(bytes);
          }
        }
      case UUID:
        {
          UUID uuid = (UUID) literal.value();
          if (Objects.isNull(uuid)) {
            return Literal.string(null);
          } else {
            return Literal.string(uuid.toString());
          }
        }
      case TIME:
        return Literal.timeMicros((Long) literal.value());
      case DATE:
        return Literal.dateDays((Integer) literal.value());
      case TIMESTAMP:
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
      default:
        throw new UnsupportedOperationException("Unsupported Literal type: " + termType);
    }
  }

  // Always true is not a real binary op...
  Expression fromBinaryPredicate(
      org.apache.iceberg.expressions.Expression.Operation op, Expression left, Expression right) {
    switch (op) {
      case TRUE:
        return ALWAYS_TRUE;
      case FALSE:
        return ALWAYS_FALSE;
      case LT:
        return Binary.lt(left, right);
      case LT_EQ:
        return Binary.ltEq(left, right);
      case GT:
        return Binary.gt(left, right);
      case GT_EQ:
        return Binary.gtEq(left, right);
      case EQ:
        return Binary.eq(left, right);
      case NOT_EQ:
        return Binary.notEq(left, right);
      case AND:
        return Binary.and(left, right);
      case OR:
        return Binary.or(left, right);

        // These filters cannot be translated into Vortex operations, so we do not use them for
        // pruning and instead return full results to the query engine for post-filtering.
      default:
        return ALWAYS_TRUE;
    }
  }

  Expression fromUnaryPredicate(
      org.apache.iceberg.expressions.Expression.Operation op, Expression child) {
    switch (op) {
      case TRUE:
        return ALWAYS_TRUE;
      case FALSE:
        return ALWAYS_FALSE;
      case NOT:
        if (child == ALWAYS_TRUE) {
          return ALWAYS_FALSE;
        } else if (child == ALWAYS_FALSE) {
          return ALWAYS_TRUE;
        } else {
          return Not.of(child);
        }
      default:
        return ALWAYS_TRUE;
    }
  }
}
