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

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class MetadataTableScanTestBase extends TableTestBase {

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] {1, 2};
  }

  public MetadataTableScanTestBase(int formatVersion) {
    super(formatVersion);
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
      Assert.assertTrue("Tasks should not be empty", Iterables.size(tasks) > 0);
      for (CombinedScanTask combinedScanTask : tasks) {
        for (FileScanTask fileScanTask : combinedScanTask.files()) {
          if (ignoreResiduals) {
            Assert.assertEquals(
                "Residuals must be ignored", Expressions.alwaysTrue(), fileScanTask.residual());
          } else {
            Assert.assertNotEquals(
                "Residuals must be preserved", Expressions.alwaysTrue(), fileScanTask.residual());
          }
        }
      }
    }
  }

  protected void validateIncludesPartitionScan(
      CloseableIterable<FileScanTask> tasks, int partValue) {
    validateIncludesPartitionScan(tasks, 0, partValue);
  }

  protected void validateIncludesPartitionScan(
      CloseableIterable<FileScanTask> tasks, int position, int partValue) {
    Assert.assertTrue(
        "File scan tasks do not include correct file",
        StreamSupport.stream(tasks.spliterator(), false)
            .anyMatch(
                task -> {
                  StructLike partition = task.file().partition();
                  if (position >= partition.size()) {
                    return false;
                  }

                  return partition.get(position, Object.class).equals(partValue);
                }));
  }
}
