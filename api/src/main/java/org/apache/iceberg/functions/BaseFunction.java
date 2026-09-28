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
package org.apache.iceberg.functions;

import java.util.Objects;

/** Base for all concrete {@link IcebergFunction} implementations; holds the field id. */
abstract class BaseFunction<S, T> implements IcebergFunction<S, T> {
  private final int fieldId;

  BaseFunction(int fieldId) {
    this.fieldId = fieldId;
  }

  @Override
  public final int fieldId() {
    return fieldId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IcebergFunction<?, ?> other = (IcebergFunction<?, ?>) o;
    // name() is a constant per class except in UnknownFunction, whose instances differ by name.
    return fieldId == other.fieldId() && name().equals(other.name());
  }

  @Override
  public int hashCode() {
    return Objects.hash(name(), fieldId);
  }

  @Override
  public String toString() {
    return name() + "(" + fieldId + ")";
  }
}
