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
package org.apache.iceberg;

import org.apache.iceberg.types.Type;

interface FieldStats<T> extends StructLike {
  /** The field ID of the statistic */
  int fieldId();

  /**
   * The field type of the statistic.
   *
   * <p>For geo types (geometry/geography), this returns the bounding box struct type (geo_lower /
   * geo_upper) rather than the column's geometry or geography type, because the type is inferred
   * from the lower/upper bound schema fields.
   */
  Type type();

  /** The lower bound */
  T lowerBound();

  /** The upper bound */
  T upperBound();

  /**
   * Whether {@link #lowerBound()} and {@link #upperBound()} are equal to the min and max values for
   * the column.
   */
  boolean tightBounds();

  /** The total value count, including null and NaN */
  Long valueCount();

  /** The total null value count */
  Long nullValueCount();

  /** The total NaN value count */
  Long nanValueCount();

  /**
   * The avg value size in memory (uncompressed) in bytes for variable-length types (string, binary,
   * variant)
   */
  Integer avgValueSizeInBytes();
}
