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
package org.apache.iceberg.stats;

import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;

public interface FieldStats<T> extends StructLike {
  /** The field ID of the statistic */
  int fieldId();

  /** The field type of the statistic */
  Type type();

  /** The total value count, including null and NaN */
  Long valueCount();

  /** The total null value count */
  Long nullValueCount();

  /** The total NaN value count */
  Long nanValueCount();

  /** The avg value size of variable-length types (String, Binary) */
  Integer avgValueSize();

  /** The max value size of variable-length types (String, Binary) */
  Integer maxValueSize();

  /** The lower bound */
  T lowerBound();

  /** The upper bound */
  T upperBound();

  /** Whether this statistic is exact or not. */
  boolean isExact();
}
