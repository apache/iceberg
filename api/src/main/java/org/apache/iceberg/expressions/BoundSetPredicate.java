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

public class BoundSetPredicate<T> extends Predicate<BoundReference<T>> {
  private final LiteralSet<T> literalSet;

  BoundSetPredicate(Operation op, BoundReference<T> ref, Set<Literal<T>> lits) {
    super(op, ref);
    Preconditions.checkArgument(op == Operation.IN || op == Operation.NOT_IN,
        "%s predicate does not support a set of literals", op);
    this.literalSet = new LiteralSet<>(lits);
  }

  BoundSetPredicate(Operation op, BoundReference<T> ref, LiteralSet<T> lits) {
    super(op, ref);
    Preconditions.checkArgument(op == Operation.IN || op == Operation.NOT_IN,
        "%s predicate does not support a literal set", op);
    this.literalSet = lits;
  }

  @Override
  public Expression negate() {
    return new BoundSetPredicate<>(op().negate(), ref(), literalSet());
  }

  public LiteralSet<T> literalSet() {
    return literalSet;
  }

  @Override
  public String toString() {
    switch (op()) {
      case IN:
        return String.valueOf(ref()) + " in " + literalSet();
      case NOT_IN:
        return String.valueOf(ref()) + " not in " + literalSet();
      default:
        return "Invalid predicate: operation = " + op();
    }
  }
}
