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
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.SerializableFunction;

/**
 * A column projection action from the ReadRestrictions spec.
 *
 * <p>An {@code Action} is both a data carrier (wire-format discriminator + field id) and a factory
 * for the masking function that applies its semantics. {@link #bind(Type)} dispatches to the
 * type-specific {@link SerializableFunction} that implements the action. This mirrors the pattern
 * used by {@link org.apache.iceberg.transforms.Transform} in the partition-transform hierarchy.
 *
 * <p>All bound functions honor the spec invariant that null input produces null output.
 *
 * @param <S> source value type
 * @param <T> masked output type (usually equal to S; only {@link ApplyExpression} differs)
 */
public interface Action<S, T> extends Serializable {

  String MASK_ALPHANUM = "mask-alphanum";
  String MASK_TO_FIXED_VALUE = "mask-to-fixed-value";
  String REPLACE_WITH_NULL = "replace-with-null";
  String SHOW_FIRST_4 = "show-first-4";
  String SHOW_LAST_4 = "show-last-4";
  String TRUNCATE_TO_YEAR = "truncate-to-year";
  String TRUNCATE_TO_MONTH = "truncate-to-month";
  String SHA_256_GLOBAL = "sha-256-global";
  String SHA_256_QUERY_LOCAL = "sha-256-query-local";
  String APPLY_EXPRESSION = "apply-expression";

  /** The action discriminator string as sent on the wire. */
  String actionType();

  /** The field id of the column this action applies to. */
  int fieldId();

  /**
   * Returns a function that applies this action to values of the given {@link Type}.
   *
   * @throws IllegalArgumentException if the type is not supported by this action.
   */
  default SerializableFunction<S, T> bind(Type type) {
    throw new UnsupportedOperationException("bind is not implemented for " + getClass().getName());
  }

  /**
   * Variant that accepts a per-query salt. Only {@link Sha256QueryLocal} uses the salt; other
   * actions ignore it and delegate to {@link #bind(Type)}.
   */
  default SerializableFunction<S, T> bind(Type type, byte[] salt) {
    return bind(type);
  }

  /** Returns true if this action can be bound to the given {@link Type}. */
  boolean canBind(Type type);

  /**
   * Base for all concrete actions. Holds the field id; subclasses carry any action-specific state
   * (expression, salt, etc.) and implement {@link #bind(Type)}/{@link #canBind(Type)}.
   */
  abstract class BaseAction<S, T> implements Action<S, T> {
    private final int fieldId;

    BaseAction(int fieldId) {
      this.fieldId = fieldId;
    }

    @Override
    public final int fieldId() {
      return fieldId;
    }
  }
}
