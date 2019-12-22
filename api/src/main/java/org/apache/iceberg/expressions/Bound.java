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

/**
 * Represents a bound value expression.
 *
 * @param <T> the Java type of values produced by this expression
 */
public interface Bound<T> {
  /**
   * @return the underlying reference
   */
  BoundReference<?> ref();

  /**
   * Produce a value from the struct for this expression.
   *
   * @param struct a struct of incoming data
   * @return the value of this expression when evaluated on the incoming struct
   */
  T eval(StructLike struct);
}
