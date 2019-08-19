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

public class BoundPredicate<T> extends Predicate<BoundReference<T>> {
  private final Literal<T> literal;

  BoundPredicate(Operation op, BoundReference<T> ref, Literal<T> lit) {
    super(op, ref);
    Preconditions.checkArgument(op != Operation.IN && op != Operation.NOT_IN,
        "Bound predicate does not support %s operation", op);
    this.literal = lit;
  }

  BoundPredicate(Operation op, BoundReference<T> ref) {
    super(op, ref);
    Preconditions.checkArgument(op == Operation.IS_NULL || op == Operation.NOT_NULL,
        "Cannot create %s predicate without a value", op);
    this.literal = null;
  }

  @Override
  public Expression negate() {
    return new BoundPredicate<>(op().negate(), ref(), literal());
  }

  public Literal<T> literal() {
    return literal;
  }

  @Override
  String literalString() {
    return literal.toString();
  }
}
