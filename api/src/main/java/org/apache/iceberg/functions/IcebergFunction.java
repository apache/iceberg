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

import java.io.Serializable;
import java.util.Objects;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.SerializableFunction;

/**
 * A named, type-aware function that can be bound to an Iceberg {@link Type}.
 *
 * <p>{@link #bind(Type)} returns a {@link SerializableFunction} that applies this function's logic
 * to values of the bound type.
 *
 * @param <S> input value type
 * @param <T> output value type
 */
public interface IcebergFunction<S, T> extends Serializable {

  String MASK_ALPHANUM = "mask-alphanum";
  String MASK_TO_FIXED_VALUE = "mask-to-fixed-value";
  String REPLACE_WITH_NULL = "replace-with-null";
  String SHOW_FIRST_4 = "show-first-4";
  String SHOW_LAST_4 = "show-last-4";
  String TRUNCATE_TO_YEAR = "truncate-to-year";
  String TRUNCATE_TO_MONTH = "truncate-to-month";
  String SHA_256_GLOBAL = "sha-256-global";
  String SHA_256_QUERY_LOCAL = "sha-256-query-local";

  /** The function name as sent on the wire (REST action discriminator). */
  String name();

  /** The field id of the column this function applies to. */
  int fieldId();

  /**
   * Returns a function that applies this projection to values of the given {@link Type}.
   *
   * @throws IllegalArgumentException if the type is not supported by this function.
   */
  default SerializableFunction<S, T> bind(Type type) {
    throw new UnsupportedOperationException("bind is not implemented for " + getClass().getName());
  }

  /** Returns true if this function can be bound to the given {@link Type}. */
  boolean canBind(Type type);

  /** Base for all concrete functions; holds the field id. */
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
}
