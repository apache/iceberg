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
package org.apache.iceberg.util;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.iceberg.types.Type;

@FunctionalInterface
public interface BucketHash<T> extends Serializable {
  int hash(T value);

  @SuppressWarnings("unchecked")
  static <T> BucketHash<T> forType(Type type) {
    switch (type.typeId()) {
      case DATE:
      case INTEGER:
        return value -> BucketUtil.hash((int) value);
      case TIME:
      case TIMESTAMP:
      case LONG:
        return value -> BucketUtil.hash((long) value);
      case DECIMAL:
        return value -> BucketUtil.hash((BigDecimal) value);
      case STRING:
        return value -> BucketUtil.hash((CharSequence) value);
      case FIXED:
      case BINARY:
        return value -> BucketUtil.hash((ByteBuffer) value);
      case TIMESTAMP_NANO:
        return nanos -> BucketUtil.hash(DateTimeUtil.nanosToMicros((long) nanos));
      case UUID:
        return value -> BucketUtil.hash((UUID) value);
      case STRUCT:
        return (BucketHash<T>) BucketHashes.struct(type.asStructType());
      default:
        throw new IllegalArgumentException("Cannot bucket by type: " + type);
    }
  }
}
