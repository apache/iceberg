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

public abstract class BoundPredicate<T> extends Predicate<T, BoundTerm<T>> implements Bound<Boolean> {
  protected BoundPredicate(Operation op, BoundTerm<T> child) {
    super(op, child);
  }

  public boolean test(StructLike struct) {
    return test(child().eval(struct));
  }

  public abstract boolean test(T value);

  @Override
  public Boolean eval(StructLike struct) {
    return test(child().eval(struct));
  }

  @Override
  public BoundReference<?> ref() {
    return child().ref();
  }

  public boolean isUnaryPredicate() {
    return false;
  }

  public BoundUnaryPredicate<T> asUnaryPredicate() {
    throw new IllegalStateException("Not a unary predicate: " + this);
  }

  public boolean isLiteralPredicate() {
    return false;
  }

  public BoundLiteralPredicate<T> asLiteralPredicate() {
    throw new IllegalStateException("Not a literal predicate: " + this);
  }

  public boolean isSetPredicate() {
    return false;
  }

  public BoundSetPredicate<T> asSetPredicate() {
    throw new IllegalStateException("Not a set predicate: " + this);
  }
}
