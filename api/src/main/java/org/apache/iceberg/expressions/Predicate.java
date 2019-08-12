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

import com.google.common.base.Preconditions;
import java.util.Set;

public abstract class Predicate<T, R extends Reference> implements Expression {
  private final Operation op;
  private final R ref;
  private final Literal<T> literal;
  private final LiteralSet<T> literalSet;

  Predicate(Operation op, R ref) {
    Preconditions.checkArgument(op == Operation.IS_NULL || op == Operation.NOT_NULL,
        "Cannot create %s predicate without a value", op);
    this.op = op;
    this.ref = ref;
    this.literal = null;
    this.literalSet = null;
  }

  Predicate(Operation op, R ref, Literal<T> lit) {
    Preconditions.checkArgument(op != Operation.IN && op != Operation.NOT_IN,
        "%s predicate does not support a single literal", op);
    this.op = op;
    this.ref = ref;
    this.literal = lit;
    this.literalSet = null;
  }

  Predicate(Operation op, R ref, Set<Literal<T>> lits) {
    Preconditions.checkArgument(op == Operation.IN || op == Operation.NOT_IN,
        "%s predicate does not support a set of literals", op);
    this.op = op;
    this.ref = ref;
    this.literal = null;
    this.literalSet = new LiteralSet<>(lits);
  }

  Predicate(Operation op, R ref, LiteralSet<T> lits) {
    Preconditions.checkArgument(op == Operation.IN || op == Operation.NOT_IN,
        "%s predicate does not support a literal set", op);
    this.op = op;
    this.ref = ref;
    this.literal = null;
    this.literalSet = lits;
  }

  @Override
  public Operation op() {
    return op;
  }

  public R ref() {
    return ref;
  }

  Literal<T> literal() {
    return literal;
  }

  LiteralSet<T> literalSet() {
    return literalSet;
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
      case STARTS_WITH:
        return ref() + " startsWith \"" + literal() + "\"";
      case IN:
        return String.valueOf(ref()) + " in " + literalSet();
      case NOT_IN:
        return String.valueOf(ref()) + " not in " + literalSet();
      default:
        return "Invalid predicate: operation = " + op;
    }
  }
}
