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

public enum FieldStatistic {
  VALUE_COUNT(0, "value_count"),
  NULL_VALUE_COUNT(1, "null_value_count"),
  NAN_VALUE_COUNT(2, "nan_value_count"),
  AVG_VALUE_SIZE(3, "avg_value_size"),
  MAX_VALUE_SIZE(4, "max_value_size"),
  LOWER_BOUND(5, "lower_bound"),
  UPPER_BOUND(6, "upper_bound"),
  IS_EXACT(7, "is_exact");

  private final int offset;
  private final String fieldName;

  FieldStatistic(int offset, String fieldName) {
    this.offset = offset;
    this.fieldName = fieldName;
  }

  public int offset() {
    return offset;
  }

  public String fieldName() {
    return fieldName;
  }

  public static FieldStatistic fromOffset(int offset) {
    switch (offset) {
      case 0:
        return VALUE_COUNT;
      case 1:
        return NULL_VALUE_COUNT;
      case 2:
        return NAN_VALUE_COUNT;
      case 3:
        return AVG_VALUE_SIZE;
      case 4:
        return MAX_VALUE_SIZE;
      case 5:
        return LOWER_BOUND;
      case 6:
        return UPPER_BOUND;
      case 7:
        return IS_EXACT;
      default:
        throw new IllegalArgumentException("Invalid statistic offset: " + offset);
    }
  }
}
