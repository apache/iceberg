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

import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class BoundAggregate<T, C> extends Aggregate<BoundTerm<T>> implements Bound<C> {
  protected BoundAggregate(Operation op, BoundTerm<T> term) {
    super(op, term);
  }

  @Override
  public C eval(StructLike struct) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement eval.");
  }

  @Override
  public BoundReference<?> ref() {
    return term().ref();
  }

  public Types.NestedField nestedField(int index) {
    if (op() == Operation.COUNT_STAR) {
      return Types.NestedField.required(index, "COUNT(*)", Types.LongType.get());
    } else {
      if (term() instanceof BoundReference) {
        if (op() == Operation.COUNT) {
          return Types.NestedField.required(
              index, "COUNT(" + term().ref().name() + ")", Types.LongType.get());
        } else if (op() == Operation.MAX) {
          return Types.NestedField.required(
              index, "MAX(" + term().ref().name() + ")", term().type());
        } else if (op() == Operation.MIN) {
          return Types.NestedField.required(
              index, "MIN(" + term().ref().name() + ")", term().type());
        } else {
          throw new UnsupportedOperationException(op() + " is not supported.");
        }
      } else {
        throw new UnsupportedOperationException("Aggregate BoundTransform is not supported.");
      }
    }
  }

  public Type type() {
    if (op() == Operation.COUNT || op() == Operation.COUNT_STAR) {
      return Types.LongType.get();
    } else {
      return term().type();
    }
  }

  public String columnName() {
    if (op() == Operation.COUNT_STAR) {
      return "*";
    } else {
      return ref().name();
    }
  }
}
