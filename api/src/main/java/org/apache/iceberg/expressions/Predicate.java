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
import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class Predicate<T, R extends Reference> implements Expression {
  private final Operation op;
  private final R ref;
  private final Literal<T> literal;
  private final Set<Literal<T>> literalSet;

  Predicate(Operation op, R ref, Literal<T> lit) {
    this.op = op;
    this.ref = ref;
    this.literal = lit;
    if (lit == null || lit == Literals.aboveMax() || lit == Literals.belowMin() ||
            (op != Operation.IN && op != Operation.NOT_IN)) {
      this.literalSet = ImmutableSet.of();
    } else {
      this.literalSet = ImmutableSet.of(lit);
    }
  }

  Predicate(Operation op, R ref, Literal<T> lit, Collection<Literal<T>> lits) {
    Preconditions.checkArgument((op == Operation.IN || op == Operation.NOT_IN) && lit != null,
            "IN and NOT_IN predicate support a collection of not null literals");
    this.op = op;
    this.ref = ref;
    this.literal = lit;
    if (lits == null) {
      this.literalSet = ImmutableSet.of();
    } else {
      this.literalSet = Stream.concat(Stream.of(lit), lits.stream())
              .filter(l -> l != null && l != Literals.aboveMax() && l != Literals.belowMin())
              .collect(Collectors.toSet());
    }
  }

  @Override
  public Operation op() {
    return op;
  }

  public R ref() {
    return ref;
  }

  public Literal<T> literal() {
    return literal;
  }

  public Set<Literal<T>> literalSet() {
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
