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
package org.apache.iceberg.data;

/**
 * Transformer interface for transforming rows of data. Should be implemented by the engines to
 * handle type mapping when multiple engine types are mapped to a single Iceberg type.
 *
 * @param <T> Engine-specific row type that this transformer handles.
 */
public interface RowTransformer<T> {
  /**
   * Transforms a row of data into another type.
   *
   * @param row the input row to transform
   * @return the transformed row
   */
  T transform(T row);
}
