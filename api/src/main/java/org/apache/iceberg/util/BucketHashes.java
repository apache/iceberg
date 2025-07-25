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

import java.util.function.IntFunction;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.hash.Hasher;
import org.apache.iceberg.types.Types;

public class BucketHashes {
  private BucketHashes() {}

  static BucketHash<StructLike> struct(Types.StructType struct) {
    return new StructHash(struct);
  }

  private static class StructHash implements BucketHash<StructLike> {
    private final BucketHash<Object>[] hashes;

    private StructHash(Types.StructType struct) {
      this.hashes =
          struct.fields().stream()
              .map(Types.NestedField::type)
              .map(BucketHash::forType)
              .toArray((IntFunction<BucketHash<Object>[]>) BucketHash[]::new);
    }

    @Override
    public int hash(StructLike value) {
      if (value == null) {
        return 0;
      }

      Hasher hasher = BucketUtil.hashFunction().newHasher();
      int len = hashes.length;
      hasher.putInt(len);
      for (int i = 0; i < len; i += 1) {
        hasher.putInt(hashes[i].hash(value.get(i, Object.class)));
      }
      return hasher.hash().asInt();
    }
  }
}
