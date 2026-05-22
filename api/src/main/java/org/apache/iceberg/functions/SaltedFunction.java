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

import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.SerializableFunction;

/**
 * An {@link IcebergFunction} that requires a per-query salt for binding.
 *
 * @param <S> input column value type
 * @param <T> output column value type
 */
public interface SaltedFunction<S, T> extends IcebergFunction<S, T> {

  /**
   * Returns a function that applies this projection using the given salt.
   *
   * @throws IllegalArgumentException if the type is not supported or the salt is invalid.
   */
  SerializableFunction<S, T> bind(Type type, byte[] salt);
}
