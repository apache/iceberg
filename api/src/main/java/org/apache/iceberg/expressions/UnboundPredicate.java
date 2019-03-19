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

package com.netflix.iceberg.expressions;

import com.netflix.iceberg.exceptions.ValidationException;
import com.netflix.iceberg.types.Types;

import static com.netflix.iceberg.expressions.Expression.Operation.IS_NULL;
import static com.netflix.iceberg.expressions.Expression.Operation.NOT_NULL;

public class UnboundPredicate<T> extends Predicate<T, NamedReference> {

  UnboundPredicate(Operation op, NamedReference namedRef, T value) {
    super(op, namedRef, Literals.from(value));
  }

  UnboundPredicate(Operation op, NamedReference namedRef) {
    super(op, namedRef, null);
  }

  UnboundPredicate(Operation op, NamedReference namedRef, Literal<T> lit) {
    super(op, namedRef, lit);
  }

  @Override
  public Expression negate() {
    return new UnboundPredicate<>(op().negate(), ref(), literal());
  }

  /**
   * Bind this UnboundPredicate, defaulting to case sensitive mode.
   *
   * Access modifier is package-private, to only allow use from existing tests.
   *
   * @param struct The {@link Types.StructType struct type} to resolve references by name.
   * @return an {@link Expression}
   * @throws ValidationException if literals do not match bound references, or if comparison on expression is invalid
   */
  Expression bind(Types.StructType struct) {
    return bind(struct, true);
  }

  /**
   * Bind this UnboundPredicate.
   *
   * @param struct The {@link Types.StructType struct type} to resolve references by name.
   * @param caseSensitive A boolean flag to control whether the bind should enforce case sensitivity.
   * @return an {@link Expression}
   * @throws ValidationException if literals do not match bound references, or if comparison on expression is invalid
   */
  public Expression bind(Types.StructType struct, boolean caseSensitive) {
    Types.NestedField field;
    if (caseSensitive) {
      field = struct.field(ref().name());
    } else {
      field = struct.caseInsensitiveField(ref().name());
    }

    ValidationException.check(field != null,
        "Cannot find field '%s' in struct: %s", ref().name(), struct);

    if (literal() == null) {
      switch (op()) {
        case IS_NULL:
          if (field.isRequired()) {
            return Expressions.alwaysFalse();
          }
          return new BoundPredicate<>(IS_NULL, new BoundReference<>(struct, field.fieldId()));
        case NOT_NULL:
          if (field.isRequired()) {
            return Expressions.alwaysTrue();
          }
          return new BoundPredicate<>(NOT_NULL, new BoundReference<>(struct, field.fieldId()));
        default:
          throw new ValidationException("Operation must be IS_NULL or NOT_NULL");
      }
    }

    Literal<T> lit = literal().to(field.type());
    if (lit == null) {
      throw new ValidationException(String.format(
          "Invalid value for comparison inclusive type %s: %s (%s)",
          field.type(), literal().value(), literal().value().getClass().getName()));

    } else if (lit == Literals.aboveMax()) {
      switch (op()) {
        case LT:
        case LT_EQ:
        case NOT_EQ:
          return Expressions.alwaysTrue();
        case GT:
        case GT_EQ:
        case EQ:
          return Expressions.alwaysFalse();
//        case IN:
//          break;
//        case NOT_IN:
//          break;
      }
    } else if (lit == Literals.belowMin()) {
      switch (op()) {
        case GT:
        case GT_EQ:
        case NOT_EQ:
          return Expressions.alwaysTrue();
        case LT:
        case LT_EQ:
        case EQ:
          return Expressions.alwaysFalse();
//        case IN:
//          break;
//        case NOT_IN:
//          break;
      }
    }
    return new BoundPredicate<>(op(), new BoundReference<>(struct, field.fieldId()), lit);
  }
}
