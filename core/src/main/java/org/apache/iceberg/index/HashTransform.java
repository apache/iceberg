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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * HASH transform for the SCALAR index type.
 *
 * <p>Maps a key value to a hash bucket in [0, numBuckets). Uses the same
 * murmur3-based hash as Iceberg's bucket partition transform so that index
 * bucket assignments are consistent with partition pruning.
 *
 * <p>The transform value stored in leaf files is the bucket number (long).
 * The tracking file stores [bucketMin, bucketMax] per leaf file, enabling
 * the planner to identify which leaf files to scan for a given key.
 */
public class HashTransform {

  private final int numBuckets;

  public HashTransform(int numBuckets) {
    Preconditions.checkArgument(numBuckets > 0, "numBuckets must be positive, got: %s", numBuckets);
    this.numBuckets = numBuckets;
  }

  public int numBuckets() {
    return numBuckets;
  }

  /**
   * Compute the hash bucket for a String value.
   *
   * <p>Uses Java's {@code hashCode()} for Phase 1. A follow-up will switch to murmur3_x86_32
   * to align with Iceberg's bucket partition transform.
   */
  public long apply(String value) {
    if (value == null) {
      return 0;
    }
    return Math.floorMod(value.hashCode(), numBuckets);
  }

  /** Compute the hash bucket for a long value. */
  public long apply(long value) {
    return Math.floorMod(Long.hashCode(value), numBuckets);
  }

  /** Compute the hash bucket for an int value. */
  public long apply(int value) {
    return Math.floorMod(Integer.hashCode(value), numBuckets);
  }

  /**
   * Compute the hash bucket for a value of any supported type.
   *
   * @throws IllegalArgumentException if the type is not supported
   */
  public long apply(Object value) {
    if (value == null) return 0;
    if (value instanceof String) return apply((String) value);
    if (value instanceof Long) return apply((long) (Long) value);
    if (value instanceof Integer) return apply((int) (Integer) value);
    throw new IllegalArgumentException(
        "Unsupported type for HASH transform: " + value.getClass().getName());
  }
}
