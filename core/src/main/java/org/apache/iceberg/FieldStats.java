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

import org.apache.iceberg.types.Types;

interface FieldStats<T> {
  /** The field ID of the statistic */
  int fieldId();

  /**
   * Struct type describing the stats tracked for the field identified by {@link #fieldId()}.
   *
   * <p>Note: This type may be a projection of the stats stored in manifest files.
   */
  Types.StructType type();

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
  long valueCount();

  /** The total null value count */
  long nullValueCount();

  /** The total NaN value count */
  long nanValueCount();

  /**
   * The avg value size in memory (uncompressed) in bytes for variable-length types (string, binary,
   * variant)
   */
  Integer avgValueSizeInBytes();

  /** Returns a copy of this {@link FieldStats}. */
  FieldStats<T> copy();
}
