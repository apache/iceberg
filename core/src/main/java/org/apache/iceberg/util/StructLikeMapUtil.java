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

import java.nio.file.Paths;
import java.util.Map;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StructLikeMapUtil {

  private static final Logger LOG = LoggerFactory.getLogger(StructLikeMap.class);
  public static final String IMPL = "struct-like-map-impl";
  public static final String IN_MEMORY_MAP = "in-memory";
  public static final String ROCKSDB_MAP = "rocksdb";
  public static final String ROCKSDB_DIR = "rocksdb-dir";

  private StructLikeMapUtil() {
  }

  public static Map<StructLike, StructLike> load(Types.StructType keyType,
                                                 Types.StructType valType,
                                                 Map<String, String> properties) {
    String impl = properties.getOrDefault(IMPL, IN_MEMORY_MAP);

    LOG.info("Loading StructLikeMap implementation: {}", impl);

    switch (impl) {
      case IN_MEMORY_MAP:
        return StructLikeMap.create(keyType);
      case ROCKSDB_MAP:
        String defaultDir = Paths.get(System.getProperty("java.io.tmpdir"), "iceberg-rocksdb").toString();

        return RocksDBStructLikeMap.create(properties.getOrDefault(ROCKSDB_DIR, defaultDir), keyType, valType);
      default:
        throw new UnsupportedOperationException("Unknown StructLikeMap implementation: " + impl);
    }
  }
}
