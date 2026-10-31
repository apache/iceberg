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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.ZOrderByteUtils;

/**
 * ZORDER transform for composite (multi-column) SCALAR indexes: interleaves the key columns'
 * order-preserving byte representations (Z-order / Morton curve) into a single {@code long}
 * transform value, giving a total ordering that clusters nearby multi-column keys. Reuses Iceberg's
 * {@link ZOrderByteUtils} (the same encoding used by {@code rewrite_data_files} Z-order) for
 * correct, order-preserving per-type encoding of {@code int}/{@code long}/{@code string}.
 *
 * <p>Each key column contributes {@code floor(8 / N)} leading ordered bytes (N = number of key
 * columns); the interleaved result's top 63 bits form a non-negative transform value. Because the
 * value is a single {@code long}, only each column's leading bytes are retained — well-distributed
 * keys (e.g. UUIDs, wide ranges) spread across the space, while keys sharing a long common prefix
 * are distinguished more coarsely (the engine's residual predicate still returns correct results).
 */
public class ZorderTransform implements TransformFunction, Serializable {

  @Override
  public String name() {
    return "ZORDER";
  }

  @Override
  public long transform(List<Object> keyValues) {
    Preconditions.checkArgument(
        keyValues != null && !keyValues.isEmpty(), "at least one key value is required");
    int dimensions = keyValues.size();
    Preconditions.checkArgument(
        dimensions <= ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE,
        "ZORDER supports at most %s key columns, got %s",
        ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE,
        dimensions);
    int bytesPerColumn = Math.max(1, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE / dimensions);
    byte[][] columns = new byte[dimensions][];
    for (int i = 0; i < dimensions; i++) {
      columns[i] = orderedBytes(keyValues.get(i), bytesPerColumn);
    }
    byte[] interleaved =
        ZOrderByteUtils.interleaveBits(
            columns,
            ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE,
            ByteBuffer.allocate(ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE));
    // Interpret the interleaved Z-value's top 63 bits as a non-negative, order-preserving long.
    return ByteBuffer.wrap(interleaved).getLong() >>> 1;
  }

  @Override
  public boolean supportsRangePredicates() {
    // A range on a single key column does not map to one contiguous Z-order range; the composite
    // query path maps a multi-dimensional predicate to Z-order ranges separately.
    return false;
  }

  private static byte[] orderedBytes(Object value, int length) {
    Preconditions.checkArgument(value != null, "key value must not be null");
    ByteBuffer ordered;
    if (value instanceof Integer || value instanceof Long) {
      ordered =
          ZOrderByteUtils.longToOrderedBytes(
              ((Number) value).longValue(),
              ByteBuffer.allocate(ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE));
    } else if (value instanceof String) {
      ordered =
          ZOrderByteUtils.stringToOrderedBytes(
              (String) value, length, ByteBuffer.allocate(length), StandardCharsets.UTF_8.newEncoder());
    } else {
      throw new IllegalArgumentException(
          "Unsupported type for ZORDER transform: " + value.getClass().getName());
    }
    // Keep this column's leading `length` ordered bytes so every column contributes equally to the
    // interleave (required for a consistent Z-order).
    return Arrays.copyOf(ordered.array(), length);
  }
}
