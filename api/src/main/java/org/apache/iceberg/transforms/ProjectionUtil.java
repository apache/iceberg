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

package org.apache.iceberg.transforms;

import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.UnboundPredicate;

import static org.apache.iceberg.expressions.Expressions.predicate;

class ProjectionUtil {

  private ProjectionUtil() {}

  static <T> UnboundPredicate<T> truncateInteger(
      String name, BoundPredicate<Integer> pred, Transform<Integer, T> transform) {
    int boundary = pred.literal().value();
    switch (pred.op()) {
      case LT:
        // adjust closed and then transform ltEq
        return predicate(Expression.Operation.LT_EQ, name, transform.apply(boundary - 1));
      case LT_EQ:
        return predicate(Expression.Operation.LT_EQ, name, transform.apply(boundary));
      case GT:
        // adjust closed and then transform gtEq
        return predicate(Expression.Operation.GT_EQ, name, transform.apply(boundary + 1));
      case GT_EQ:
        return predicate(Expression.Operation.GT_EQ, name, transform.apply(boundary));
      case EQ:
        return predicate(pred.op(), name, transform.apply(boundary));
      default:
        return null;
    }
  }

  static <T> UnboundPredicate<T> truncateIntegerStrict(
      String name, BoundPredicate<Integer> pred, Transform<Integer, T> transform) {
    int boundary = pred.literal().value();
    switch (pred.op()) {
      case LT:
        return predicate(Expression.Operation.LT, name, transform.apply(boundary));
      case LT_EQ:
        return predicate(Expression.Operation.LT, name, transform.apply(boundary + 1));
      case GT:
        return predicate(Expression.Operation.GT, name, transform.apply(boundary));
      case GT_EQ:
        return predicate(Expression.Operation.GT, name, transform.apply(boundary - 1));
      case NOT_EQ:
        return predicate(Expression.Operation.NOT_EQ, name, transform.apply(boundary));
      case EQ:
        // there is no predicate that guarantees equality because adjacent ints transform to the same value
        return null;
      default:
        return null;
    }
  }

  static <T> UnboundPredicate<T> truncateLongStrict(
      String name, BoundPredicate<Long> pred, Transform<Long, T> transform) {
    long boundary = pred.literal().value();
    switch (pred.op()) {
      case LT:
        return predicate(Expression.Operation.LT, name, transform.apply(boundary));
      case LT_EQ:
        return predicate(Expression.Operation.LT, name, transform.apply(boundary + 1L));
      case GT:
        return predicate(Expression.Operation.GT, name, transform.apply(boundary));
      case GT_EQ:
        return predicate(Expression.Operation.GT, name, transform.apply(boundary - 1L));
      case NOT_EQ:
        return predicate(Expression.Operation.NOT_EQ, name, transform.apply(boundary));
      case EQ:
        // there is no predicate that guarantees equality because adjacent longs transform to the same value
        return null;
      default:
        return null;
    }
  }

  static <T> UnboundPredicate<T> truncateLong(
      String name, BoundPredicate<Long> pred, Transform<Long, T> transform) {
    long boundary = pred.literal().value();
    switch (pred.op()) {
      case LT:
        // adjust closed and then transform ltEq
        return predicate(Expression.Operation.LT_EQ, name, transform.apply(boundary - 1L));
      case LT_EQ:
        return predicate(Expression.Operation.LT_EQ, name, transform.apply(boundary));
      case GT:
        // adjust closed and then transform gtEq
        return predicate(Expression.Operation.GT_EQ, name, transform.apply(boundary + 1L));
      case GT_EQ:
        return predicate(Expression.Operation.GT_EQ, name, transform.apply(boundary));
      case EQ:
        return predicate(pred.op(), name, transform.apply(boundary));
      default:
        return null;
    }
  }

  static <T> UnboundPredicate<T> truncateDecimal(
      String name, BoundPredicate<BigDecimal> pred,
      Transform<BigDecimal, T> transform) {
    BigDecimal boundary = pred.literal().value();
    switch (pred.op()) {
      case LT:
        // adjust closed and then transform ltEq
        BigDecimal minusOne = new BigDecimal(
            boundary.unscaledValue().subtract(BigInteger.ONE),
            boundary.scale());
        return predicate(Expression.Operation.LT_EQ, name, transform.apply(minusOne));
      case LT_EQ:
        return predicate(Expression.Operation.LT_EQ, name, transform.apply(boundary));
      case GT:
        // adjust closed and then transform gtEq
        BigDecimal plusOne = new BigDecimal(
            boundary.unscaledValue().add(BigInteger.ONE),
            boundary.scale());
        return predicate(Expression.Operation.GT_EQ, name, transform.apply(plusOne));
      case GT_EQ:
        return predicate(Expression.Operation.GT_EQ, name, transform.apply(boundary));
      case EQ:
        return predicate(pred.op(), name, transform.apply(boundary));
      default:
        return null;
    }
  }

  static <T> UnboundPredicate<T> truncateDecimalStrict(
      String name, BoundPredicate<BigDecimal> pred,
      Transform<BigDecimal, T> transform) {
    BigDecimal boundary = pred.literal().value();

    BigDecimal minusOne = new BigDecimal(
        boundary.unscaledValue().subtract(BigInteger.ONE),
        boundary.scale());

    BigDecimal plusOne = new BigDecimal(
        boundary.unscaledValue().add(BigInteger.ONE),
        boundary.scale());

    switch (pred.op()) {
      case LT:
        return predicate(Expression.Operation.LT, name, transform.apply(boundary));
      case LT_EQ:
        return predicate(Expression.Operation.LT, name, transform.apply(plusOne));
      case GT:
        return predicate(Expression.Operation.GT, name, transform.apply(boundary));
      case GT_EQ:
        return predicate(Expression.Operation.GT, name, transform.apply(minusOne));
      case NOT_EQ:
        return predicate(Expression.Operation.NOT_EQ, name, transform.apply(boundary));
      case EQ:
        // there is no predicate that guarantees equality because adjacent decimals transform to the same value
        return null;
      default:
        return null;
    }
  }

  static <S, T> UnboundPredicate<T> truncateArray(
      String name, BoundPredicate<S> pred, Transform<S, T> transform) {
    S boundary = pred.literal().value();
    switch (pred.op()) {
      case LT:
      case LT_EQ:
        return predicate(Expression.Operation.LT_EQ, name, transform.apply(boundary));
      case GT:
      case GT_EQ:
        return predicate(Expression.Operation.GT_EQ, name, transform.apply(boundary));
      case EQ:
        return predicate(Expression.Operation.EQ, name, transform.apply(boundary));
      case STARTS_WITH:
        return predicate(Expression.Operation.STARTS_WITH, name, transform.apply(boundary));
//        case IN: // TODO
//          return Expressions.predicate(Operation.IN, name, transform.apply(boundary));
      default:
        return null;
    }
  }

  static <S, T> UnboundPredicate<T> truncateArrayStrict(
      String name, BoundPredicate<S> pred, Transform<S, T> transform) {
    S boundary = pred.literal().value();
    switch (pred.op()) {
      case LT:
      case LT_EQ:
        return predicate(Expression.Operation.LT, name, transform.apply(boundary));
      case GT:
      case GT_EQ:
        return predicate(Expression.Operation.GT, name, transform.apply(boundary));
      case NOT_EQ:
        return predicate(Expression.Operation.NOT_EQ, name, transform.apply(boundary));
      case EQ:
        // there is no predicate that guarantees equality because adjacent values transform to the same partition
        return null;
      default:
        return null;
    }
  }
}
