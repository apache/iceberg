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
package org.apache.iceberg.flink.source.lookup;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Lookup join options for the Iceberg table source. */
@Internal
public class IcebergLookupOptions {

  public static final ConfigOption<LookupCacheType> CACHE_TYPE =
      ConfigOptions.key("lookup.cache.type")
          .enumType(LookupCacheType.class)
          .defaultValue(LookupCacheType.MEMORY)
          .withDescription(
              "Storage backend of the Iceberg full lookup cache. MEMORY keeps the cache on the "
                  + "TaskManager heap, ROCKSDB keeps it on the TaskManager local disk.");

  public static final ConfigOption<String> ROCKSDB_CACHE_DIR =
      ConfigOptions.key("lookup.cache.rocksdb.dir")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "Base directory used by the RocksDB lookup cache on TaskManagers. Each operator "
                  + "instance creates its own sub-directory. Required when lookup.cache.type is "
                  + "ROCKSDB.");

  private IcebergLookupOptions() {}
}
