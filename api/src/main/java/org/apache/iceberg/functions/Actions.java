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

import org.apache.iceberg.util.SerializableFunction;

/** Package-private helpers shared by {@link Action} implementations. */
final class Actions {

  private Actions() {}

  /**
   * Base for masking functions where null input must pass through as null unchanged (spec: "For all
   * actions, if the input column value is NULL, the output MUST be NULL."). Subclasses implement
   * {@link #applyNonNull(Object)} and don't have to repeat the guard.
   */
  abstract static class NullSafeFunction<S, T> implements SerializableFunction<S, T> {
    @Override
    public final T apply(S value) {
      return value == null ? null : applyNonNull(value);
    }

    protected abstract T applyNonNull(S value);
  }
}
