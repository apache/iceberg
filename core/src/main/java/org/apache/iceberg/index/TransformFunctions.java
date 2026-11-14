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

import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Factory for {@link TransformFunction}s by name, resolving per-transform properties. */
public class TransformFunctions {

  public static final String HASH_NUM_BUCKETS = "hash.num-buckets";
  private static final int DEFAULT_HASH_NUM_BUCKETS = 256;

  private TransformFunctions() {}

  /**
   * Resolves a transform function by name (case-insensitive), reading any per-transform properties
   * (e.g. {@code hash.num-buckets}) from {@code properties}.
   */
  public static TransformFunction forName(String name, Map<String, String> properties) {
    Preconditions.checkNotNull(name, "transform function name is required");
    String upperName = name.toUpperCase(Locale.ROOT);
    switch (upperName) {
      case "IDENTITY":
        return new IdentityTransform();
      case "HASH":
        int numBuckets =
            Integer.parseInt(
                properties.getOrDefault(
                    HASH_NUM_BUCKETS, Integer.toString(DEFAULT_HASH_NUM_BUCKETS)));
        return new HashTransform(numBuckets);
      case "ZORDER":
        return new ZorderTransform();
      default:
        throw new IllegalArgumentException("Unsupported transform function: " + name);
    }
  }
}
