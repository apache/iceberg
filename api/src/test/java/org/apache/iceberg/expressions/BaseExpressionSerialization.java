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

import java.util.Collection;

public abstract class BaseExpressionSerialization {

  // You may be wondering why this isn't implemented as Expression.equals. The reason is that
  // expression equality implies equivalence, which is wider than structural equality. For example,
  // lessThan("a", 3) is equivalent to not(greaterThanOrEqual("a", 4)). To avoid confusion, equals
  // only guarantees object identity.

  protected static boolean equals(Expression left, Expression right) {
    if (left.op() != right.op()) {
      return false;
    }

    if (left instanceof Predicate) {
      if (!(left.getClass().isInstance(right))) {
        return false;
      }
      return equals((Predicate) left, (Predicate) right);
    }

    switch (left.op()) {
      case FALSE:
      case TRUE:
        return true;
      case NOT:
        return equals(((Not) left).child(), ((Not) right).child());
      case AND:
        return equals(((And) left).left(), ((And) right).left())
            && equals(((And) left).right(), ((And) right).right());
      case OR:
        return equals(((Or) left).left(), ((Or) right).left())
            && equals(((Or) left).right(), ((Or) right).right());
      default:
        return false;
    }
  }

  protected static boolean equals(Term left, Term right) {
    if (left instanceof Reference && right instanceof Reference) {
      return equals((Reference<?>) left, (Reference<?>) right);
    } else if (left instanceof UnboundTransform && right instanceof UnboundTransform) {
      UnboundTransform<?, ?> unboundLeft = (UnboundTransform<?, ?>) left;
      UnboundTransform<?, ?> unboundRight = (UnboundTransform<?, ?>) right;
      if (equals(unboundLeft.ref(), unboundRight.ref())
          && unboundLeft.transform().toString().equals(unboundRight.transform().toString())) {
        return true;
      }

    } else if (left instanceof BoundTransform && right instanceof BoundTransform) {
      BoundTransform<?, ?> boundLeft = (BoundTransform<?, ?>) left;
      BoundTransform<?, ?> boundRight = (BoundTransform<?, ?>) right;
      if (equals(boundLeft.ref(), boundRight.ref())
          && boundLeft.transform().toString().equals(boundRight.transform().toString())) {
        return true;
      }
    }

    return false;
  }

  @SuppressWarnings({"unchecked", "checkstyle:CyclomaticComplexity"})
  protected static boolean equals(Predicate left, Predicate right) {
    if (left.op() != right.op()) {
      return false;
    }

    if (!equals(left.term(), right.term())) {
      return false;
    }

    if (left.op() == Expression.Operation.IS_NULL
        || left.op() == Expression.Operation.NOT_NULL
        || left.op() == Expression.Operation.IS_NAN
        || left.op() == Expression.Operation.NOT_NAN) {
      return true;
    }

    if (left.getClass() != right.getClass()) {
      return false;
    }

    if (left instanceof UnboundPredicate) {
      UnboundPredicate lpred = (UnboundPredicate) left;
      UnboundPredicate rpred = (UnboundPredicate) right;
      if (left.op() == Expression.Operation.IN
          || left.op() == Expression.Operation.NOT_IN
          || left.op() == Expression.Operation.RANGE_IN) {
        return equals(lpred.literals(), rpred.literals());
      }
      return lpred.literal().comparator().compare(lpred.literal().value(), rpred.literal().value())
          == 0;

    } else if (left instanceof BoundPredicate) {
      BoundPredicate lpred = (BoundPredicate) left;
      BoundPredicate rpred = (BoundPredicate) right;
      if (lpred.isLiteralPredicate() && rpred.isLiteralPredicate()) {
        return lpred
                .asLiteralPredicate()
                .literal()
                .comparator()
                .compare(
                    lpred.asLiteralPredicate().literal().value(),
                    rpred.asLiteralPredicate().literal().value())
            == 0;
      } else if (lpred.isSetPredicate() && rpred.isSetPredicate()) {
        return equals(lpred.asSetPredicate().literalSet(), rpred.asSetPredicate().literalSet());
      } else {
        return lpred.isUnaryPredicate() && rpred.isUnaryPredicate();
      }

    } else {
      throw new UnsupportedOperationException(
          String.format("Predicate equality check for %s is not supported", left.getClass()));
    }
  }

  protected static boolean equals(Collection<Literal<?>> left, Collection<Literal<?>> right) {
    if (left.size() != right.size()) {
      return false;
    }
    return left.containsAll(right);
  }

  protected static boolean equals(Reference<?> left, Reference<?> right) {
    if (left instanceof NamedReference) {
      if (!(right instanceof NamedReference)) {
        return false;
      }

      NamedReference lref = (NamedReference) left;
      NamedReference rref = (NamedReference) right;

      return lref.name().equals(rref.name());
    } else if (left instanceof BoundReference) {
      if (!(right instanceof BoundReference)) {
        return false;
      }

      BoundReference lref = (BoundReference) left;
      BoundReference rref = (BoundReference) right;

      return lref.fieldId() == rref.fieldId() && lref.type().equals(rref.type());
    }

    return false;
  }
}
