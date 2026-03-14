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
package org.apache.iceberg.expressions;

import java.util.Comparator;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;

public class BoundLiteralPredicate<T> extends BoundPredicate<T> {
  private static final Set<Type.TypeID> INTEGRAL_TYPES =
      Sets.newHashSet(
          Type.TypeID.INTEGER,
          Type.TypeID.LONG,
          Type.TypeID.DATE,
          Type.TypeID.TIME,
          Type.TypeID.TIMESTAMP_NANO,
          Type.TypeID.TIMESTAMP);

  private static long toLong(Literal<?> lit) {
    return ((Number) lit.value()).longValue();
  }

  private final Literal<T> literal;

  BoundLiteralPredicate(Operation op, BoundTerm<T> term, Literal<T> lit) {
    super(op, term);
    Preconditions.checkArgument(
        op != Operation.IN && op != Operation.NOT_IN,
        "Bound literal predicate does not support operation: %s",
        op);
    this.literal = lit;
  }

  @Override
  public Expression negate() {
    return new BoundLiteralPredicate<>(op().negate(), term(), literal);
  }

  public Literal<T> literal() {
    return literal;
  }

  @Override
  public boolean isLiteralPredicate() {
    return true;
  }

  @Override
  public BoundLiteralPredicate<T> asLiteralPredicate() {
    return this;
  }

  @Override
  public boolean test(T value) {
    Comparator<T> cmp = literal.comparator();
    switch (op()) {
      case LT:
        return cmp.compare(value, literal.value()) < 0;
      case LT_EQ:
        return cmp.compare(value, literal.value()) <= 0;
      case GT:
        return cmp.compare(value, literal.value()) > 0;
      case GT_EQ:
        return cmp.compare(value, literal.value()) >= 0;
      case EQ:
        return cmp.compare(value, literal.value()) == 0;
      case NOT_EQ:
        return cmp.compare(value, literal.value()) != 0;
      case STARTS_WITH:
        return String.valueOf(value).startsWith((String) literal.value());
      case NOT_STARTS_WITH:
        return !String.valueOf(value).startsWith((String) literal.value());
      default:
        throw new IllegalStateException("Invalid operation for BoundLiteralPredicate: " + op());
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean isEquivalentTo(Expression expr) {
    if (op() == expr.op()) {
      BoundLiteralPredicate<?> other = (BoundLiteralPredicate<?>) expr;
      if (term().isEquivalentTo(other.term())) {
        // because the term is equivalent, the literal must have the same type, T
        Literal<T> otherLiteral = (Literal<T>) other.literal();
        Comparator<T> cmp = literal.comparator();
        return cmp.compare(literal.value(), otherLiteral.value()) == 0;
      }

    } else if (expr instanceof BoundLiteralPredicate) {
      BoundLiteralPredicate<?> other = (BoundLiteralPredicate<?>) expr;
      if (INTEGRAL_TYPES.contains(term().type().typeId()) && term().isEquivalentTo(other.term())) {
        switch (op()) {
          case LT:
            if (other.op() == Operation.LT_EQ) {
              // < 6 is equivalent to <= 5
              return toLong(literal()) == toLong(other.literal()) + 1L;
            }
            break;
          case LT_EQ:
            if (other.op() == Operation.LT) {
              // <= 5 is equivalent to < 6
              return toLong(literal()) == toLong(other.literal()) - 1L;
            }
            break;
          case GT:
            if (other.op() == Operation.GT_EQ) {
              // > 5 is equivalent to >= 6
              return toLong(literal()) == toLong(other.literal()) - 1L;
            }
            break;
          case GT_EQ:
            if (other.op() == Operation.GT) {
              // >= 5 is equivalent to > 4
              return toLong(literal()) == toLong(other.literal()) + 1L;
            }
            break;
        }
      }
    }

    return false;
  }

  @Override
  public String toString() {
    switch (op()) {
      case LT:
        return term() + " < " + literal;
      case LT_EQ:
        return term() + " <= " + literal;
      case GT:
        return term() + " > " + literal;
      case GT_EQ:
        return term() + " >= " + literal;
      case EQ:
        return term() + " == " + literal;
      case NOT_EQ:
        return term() + " != " + literal;
      case STARTS_WITH:
        return term() + " startsWith \"" + literal + "\"";
      case NOT_STARTS_WITH:
        return term() + " notStartsWith \"" + literal + "\"";
      case IN:
        return term() + " in { " + literal + " }";
      case NOT_IN:
        return term() + " not in { " + literal + " }";
      default:
        return "Invalid literal predicate: operation = " + op();
    }
  }
}
