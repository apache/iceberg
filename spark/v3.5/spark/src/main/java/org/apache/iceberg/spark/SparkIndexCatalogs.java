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
package org.apache.iceberg.spark;

import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.DurableIndexCatalog;
import org.apache.iceberg.index.IndexCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Session-scoped registry of {@link IndexCatalog} instances, one per table, shared between {@code
 * CALL system.build_scalar_index(...)} and query-time index lookups within the same Spark
 * session. Follows the same singleton-registry pattern as {@link ScanTaskSetManager}.
 *
 * <p>Backed by {@link DurableIndexCatalog}, so index registrations survive a JVM restart -- a
 * fresh {@link #catalogFor} call in a new process re-derives the same catalog (it persists its
 * pointer files under the table's own location, not in this class's map). What this class caches
 * is purely the {@link IndexCatalog} object itself, for reuse within one process's lifetime, not
 * the index metadata -- losing that cache costs nothing beyond re-constructing a cheap wrapper
 * object.
 */
public class SparkIndexCatalogs {

  private static final SparkIndexCatalogs INSTANCE = new SparkIndexCatalogs();

  private final Map<String, IndexCatalog> catalogsByTableUuid = Maps.newConcurrentMap();

  private SparkIndexCatalogs() {}

  public static SparkIndexCatalogs get() {
    return INSTANCE;
  }

  /** The {@link IndexCatalog} for {@code table}, created on first use. */
  public IndexCatalog catalogFor(Table table) {
    return catalogsByTableUuid.computeIfAbsent(
        Spark3Util.baseTableUUID(table),
        uuid -> new DurableIndexCatalog(table.io(), table.location()));
  }
}
