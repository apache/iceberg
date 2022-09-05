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

import org.apache.iceberg.types.Types;

/**
 * Represents an unbound expression node.
 *
 * @param <T> the Java type of values produced by this node
 * @param <B> the Java type produced when this node is bound using {@link #bind(Types.StructType,
 *     boolean)}
 */
public interface Unbound<T, B> {
  /**
   * Bind this value expression to concrete types.
   *
   * @param struct input data types
   * @param caseSensitive whether binding should match columns using case sensitive resolution
   * @return a bound value expression
   */
  B bind(Types.StructType struct, boolean caseSensitive);

  /** Returns this expression's underlying reference. */
  NamedReference<?> ref();
}
