/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.expressions;

public abstract class Predicate<T, R extends Reference> implements Expression {
  private final Operation op;
  private final R ref;
  private final Literal<T> literal;

  Predicate(Operation op, R ref, Literal<T> lit) {
    this.op = op;
    this.ref = ref;
    this.literal = lit;
  }

  @Override
  public Operation op() {
    return op;
  }

  protected Operation negateOp() {
    switch (op) {
      case IS_NULL:
        return Operation.NOT_NULL;
      case NOT_NULL:
        return Operation.IS_NULL;
      case LT:
        return Operation.GT_EQ;
      case LT_EQ:
        return Operation.GT;
      case GT:
        return Operation.LT_EQ;
      case GT_EQ:
        return Operation.LT;
      case EQ:
        return Operation.NOT_EQ;
      case NOT_EQ:
        return Operation.EQ;
//      case IN:
//      return new Predicate<>(Operation.NOT_IN, ref, literal);
//      case NOT_IN:
//      return new Predicate<>(Operation.IN, ref, literal);
      default:
        throw new IllegalArgumentException("Invalid predicate: operation = " + op);
    }
  }

  public R ref() {
    return ref;
  }

  public Literal<T> literal() {
    return literal;
  }

  @Override
  public String toString() {
    switch (op) {
      case IS_NULL:
        return "is_null(" + ref() + ")";
      case NOT_NULL:
        return "not_null(" + ref() + ")";
      case LT:
        return String.valueOf(ref()) + " < " + literal();
      case LT_EQ:
        return String.valueOf(ref()) + " <= " + literal();
      case GT:
        return String.valueOf(ref()) + " > " + literal();
      case GT_EQ:
        return String.valueOf(ref()) + " >= " + literal();
      case EQ:
        return String.valueOf(ref()) + " == " + literal();
      case NOT_EQ:
        return String.valueOf(ref()) + " != " + literal();
//      case IN:
//        break;
//      case NOT_IN:
//        break;
      default:
        return "Invalid predicate: operation = " + op;
    }
  }
}
