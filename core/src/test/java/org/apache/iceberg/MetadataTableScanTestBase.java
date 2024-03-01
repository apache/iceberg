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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionUtil;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public abstract class MetadataTableScanTestBase extends TestBase {

  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2);
  }

  protected Set<String> actualManifestListPaths(TableScan allManifestsTableScan) {
    return StreamSupport.stream(allManifestsTableScan.planFiles().spliterator(), false)
        .map(t -> (AllManifestsTable.ManifestListReadTask) t)
        .map(t -> t.file().path().toString())
        .collect(Collectors.toSet());
  }

  protected Set<String> expectedManifestListPaths(
      Iterable<Snapshot> snapshots, Long... snapshotIds) {
    Set<Long> snapshotIdSet = Sets.newHashSet(snapshotIds);
    return StreamSupport.stream(snapshots.spliterator(), false)
        .filter(s -> snapshotIdSet.contains(s.snapshotId()))
        .map(Snapshot::manifestListLocation)
        .collect(Collectors.toSet());
  }

  protected void validateTaskScanResiduals(TableScan scan, boolean ignoreResiduals)
      throws IOException {
    try (CloseableIterable<CombinedScanTask> tasks = scan.planTasks()) {
      assertThat(tasks).as("Tasks should not be empty").hasSizeGreaterThan(0);
      for (CombinedScanTask combinedScanTask : tasks) {
        for (FileScanTask fileScanTask : combinedScanTask.files()) {
          if (ignoreResiduals) {
            assertThat(fileScanTask.residual())
                .as("Residuals must be ignored")
                .isEqualTo(Expressions.alwaysTrue());
          } else {
            assertThat(fileScanTask.residual())
                .as("Residuals must be preserved")
                .isEqualTo(Expressions.alwaysTrue());
          }
        }
      }
    }
  }

  protected void validateSingleFieldPartition(
      CloseableIterable<ManifestEntry<?>> files, int partitionValue) {
    validatePartition(files, 0, partitionValue);
  }

  protected void validatePartition(
      CloseableIterable<ManifestEntry<? extends ContentFile<?>>> entries,
      int position,
      int partitionValue) {
    assertThat(StreamSupport.stream(entries.spliterator(), false))
        .as("File scan tasks do not include correct file")
        .anyMatch(
            entry -> {
              StructLike partition = entry.file().partition();
              if (position >= partition.size()) {
                return false;
              }
              return Objects.equals(partitionValue, partition.get(position, Object.class));
            });
  }

  protected Map<Integer, ?> constantsMap(
      PositionDeletesScanTask task, Types.StructType partitionType) {
    return PartitionUtil.constantsMap(task, partitionType, (type, constant) -> constant);
  }
}
