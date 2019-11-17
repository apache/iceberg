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

public class BoundUnaryPredicate<T> extends BoundPredicate<T> {
  BoundUnaryPredicate(Operation op, BoundReference<T> ref) {
    super(op, ref);
  }

  @Override
  public boolean isUnaryPredicate() {
    return true;
  }

  @Override
  public BoundUnaryPredicate<T> asUnaryPredicate() {
    return this;
  }

  @Override
  public boolean test(T value) {
    switch (op()) {
      case IS_NULL:
        return value == null;
      case NOT_NULL:
        return value != null;
      default:
        throw new IllegalStateException("Invalid operation for BoundUnaryPredicate: " + op());
    }
  }

  @Override
  public String toString() {
    switch (op()) {
      case IS_NULL:
        return "is_null(" + ref() + ")";
      case NOT_NULL:
        return "not_null(" + ref() + ")";
      default:
        return "Invalid unary predicate: operation = " + op();
    }
  }
}
