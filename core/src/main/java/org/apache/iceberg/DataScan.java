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
package org.apache.iceberg;

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

abstract class DataScan<ThisT, T extends ScanTask, G extends ScanTaskGroup<T>>
    extends SnapshotScan<ThisT, T, G> {

  protected DataScan(Table table, Schema schema, TableScanContext context) {
    super(table, schema, context);
  }

  @Override
  protected boolean useSnapshotSchema() {
    return true;
  }

  protected ManifestGroup newManifestGroup(
      List<ManifestFile> dataManifests, List<ManifestFile> deleteManifests) {
    return newManifestGroup(dataManifests, deleteManifests, context().returnColumnStats());
  }

  protected ManifestGroup newManifestGroup(
      List<ManifestFile> dataManifests, boolean withColumnStats) {
    return newManifestGroup(dataManifests, ImmutableList.of(), withColumnStats);
  }

  protected ManifestGroup newManifestGroup(
      List<ManifestFile> dataManifests,
      List<ManifestFile> deleteManifests,
      boolean withColumnStats) {

    ManifestGroup manifestGroup =
        new ManifestGroup(io(), dataManifests, deleteManifests)
            .caseSensitive(isCaseSensitive())
            .select(withColumnStats ? SCAN_WITH_STATS_COLUMNS : SCAN_COLUMNS)
            .filterData(filter())
            .specsById(table().specs())
            .scanMetrics(scanMetrics())
            .ignoreDeleted();

    if (shouldIgnoreResiduals()) {
      manifestGroup = manifestGroup.ignoreResiduals();
    }

    if (shouldPlanWithExecutor() && (dataManifests.size() > 1 || deleteManifests.size() > 1)) {
      manifestGroup = manifestGroup.planWith(planExecutor());
    }

    return manifestGroup;
  }
}
