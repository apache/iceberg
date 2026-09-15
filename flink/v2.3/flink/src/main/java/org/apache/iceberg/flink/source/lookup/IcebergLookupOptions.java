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

  public static final ConfigOption<LookupCacheBackend> FULL_CACHE_BACKEND =
      ConfigOptions.key("lookup.full-cache.backend")
          .enumType(LookupCacheBackend.class)
          .defaultValue(LookupCacheBackend.MEMORY)
          .withDescription(
              "Storage backend of the Iceberg full lookup cache. MEMORY keeps the cache on the "
                  + "TaskManager heap, ROCKSDB keeps it on the TaskManager local disk.");

  public static final ConfigOption<String> ROCKSDB_CACHE_DIR =
      ConfigOptions.key("lookup.full-cache.rocksdb.dir")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "Base directory used by the RocksDB lookup cache on TaskManagers. Required when "
                  + "lookup.full-cache.backend is ROCKSDB. Each operator instance creates its own "
                  + "sub-directory under it, so point it at a directory on the TaskManager data "
                  + "disk with enough space for the dimension table.");

  public static final ConfigOption<Boolean> FULL_CACHE_EAGER_LOAD =
      ConfigOptions.key("lookup.full-cache.eager-load")
          .booleanType()
          .defaultValue(false)
          .withDescription(
              "Whether to load the full lookup cache when the lookup function is opened, instead "
                  + "of on the first lookup. Eager loading avoids blocking the first probe row on "
                  + "the table load.");

  public static final ConfigOption<ReloadFailurePolicy> RELOAD_FAILURE_POLICY =
      ConfigOptions.key("lookup.full-cache.reload-failure-policy")
          .enumType(ReloadFailurePolicy.class)
          .defaultValue(ReloadFailurePolicy.KEEP_STALE)
          .withDescription(
              "What to do when a background reload of the full lookup cache fails. KEEP_STALE "
                  + "keeps serving the previous cache and only records the failure, FAIL fails the "
                  + "job on the next lookup so that a stale cache is not served silently.");

  private IcebergLookupOptions() {}
}
