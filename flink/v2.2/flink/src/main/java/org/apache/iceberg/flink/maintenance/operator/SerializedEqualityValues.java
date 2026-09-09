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
package org.apache.iceberg.flink.maintenance.operator;

import java.util.Arrays;
import org.apache.flink.annotation.Internal;

/**
 * Serialized primary key used as a Flink keyed state key. Wraps the raw bytes with content-based
 * {@code equals}/{@code hashCode} so that Flink's keyBy partitions by key value, not by reference.
 *
 * <p>Using the full serialized key (instead of a hash) eliminates hash collisions: two distinct
 * primary keys always map to separate Flink state entries.
 */
@Internal
public record SerializedEqualityValues(byte[] data) {

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof SerializedEqualityValues other)) {
      return false;
    }

    return Arrays.equals(data, other.data);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(data);
  }
}
