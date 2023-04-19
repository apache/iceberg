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

import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;

public class UnboundAggregate<T> extends Aggregate<UnboundTerm<T>>
    implements Unbound<T, Expression> {

  UnboundAggregate(Operation op, UnboundTerm<T> term) {
    super(op, term);
  }

  @Override
  public NamedReference<?> ref() {
    return term().ref();
  }

  /**
   * Bind this UnboundAggregate.
   *
   * @param struct The {@link Types.StructType struct type} to resolve references by name.
   * @param caseSensitive A boolean flag to control whether the bind should enforce case
   *     sensitivity.
   * @return an {@link Expression}
   * @throws ValidationException if literals do not match bound references, or if comparison on
   *     expression is invalid
   */
  @Override
  public Expression bind(Types.StructType struct, boolean caseSensitive) {
    switch (op()) {
      case COUNT_STAR:
        return new CountStar<>(null);
      case COUNT:
        return new CountNonNull<>(boundTerm(struct, caseSensitive));
      case MAX:
        return new MaxAggregate<>(boundTerm(struct, caseSensitive));
      case MIN:
        return new MinAggregate<>(boundTerm(struct, caseSensitive));
      default:
        throw new UnsupportedOperationException("Unsupported aggregate type: " + op());
    }
  }

  private BoundTerm<T> boundTerm(Types.StructType struct, boolean caseSensitive) {
    Preconditions.checkArgument(term() != null, "Invalid aggregate term: null");
    return term().bind(struct, caseSensitive);
  }
}
