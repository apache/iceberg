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
package org.apache.iceberg.tornadovm.spark;

import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.tornadovm.DeviceMergeOnReadScan;

/**
 * One device scan a table and JVM: its plans, device buffers and compiled kernels are kept across
 * Spark tasks and queries, the way a warm Spark JVM keeps its generated code. Closed at exit.
 */
final class GpuScans {
  private static final Map<String, DeviceMergeOnReadScan> SCANS = Maps.newConcurrentMap();

  static {
    ExitCleanup.register(() -> SCANS.values().forEach(DeviceMergeOnReadScan::close));
  }

  private GpuScans() {}

  /**
   * The device scan that rewrites a table's file groups: one a table location and schema, kept like
   * the readers' (a new schema writes new columns).
   */
  static DeviceMergeOnReadScan rewriteScan(Table table) {
    return SCANS.computeIfAbsent(
        "rewrite:" + table.location() + "@" + table.schema().schemaId(),
        ignored -> DeviceMergeOnReadScan.of(table));
  }

  static DeviceMergeOnReadScan scan(Table table) {
    return SCANS.computeIfAbsent(table.location(), ignored -> DeviceMergeOnReadScan.of(table));
  }
}
