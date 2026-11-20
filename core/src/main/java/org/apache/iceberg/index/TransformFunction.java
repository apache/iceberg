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
package org.apache.iceberg.index;

import java.util.List;

/**
 * A SCALAR index transform function: derives a single {@code long} transform value from a row's key
 * column values, defining how index entries are organized (see {@code format/index.md}, "Index
 * Transform Function").
 *
 * <p>Single-column transforms (IDENTITY, HASH) take a one-element value list; composite transforms
 * (e.g. a Hilbert curve over multiple key columns) take one value per key column, in key-column
 * order.
 */
public interface TransformFunction {

  /** The transform name as stored in index metadata (e.g. {@code "IDENTITY"}, {@code "HASH"}). */
  String name();

  /**
   * Derives the transform value from a row's key column values, in key-column order.
   *
   * @param keyValues one value per key column, in key-column order
   * @return the transform value index entries are organized by
   */
  long transform(List<Object> keyValues);

  /**
   * Whether a range predicate on a key column maps to a contiguous transform-value range. True for
   * order-preserving transforms (IDENTITY, Hilbert); false for HASH, which scatters values across
   * buckets so a key range is not a contiguous bucket range.
   */
  boolean supportsRangePredicates();
}
