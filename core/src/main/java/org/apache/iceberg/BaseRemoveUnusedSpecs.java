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
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Tasks;

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;

public class BaseRemoveUnusedSpecs implements RemoveUnusedSpecs {
  private final TableOperations ops;
  private final Table table;

  public BaseRemoveUnusedSpecs(TableOperations ops, Table table) {
    this.ops = ops;
    this.table = table;
  }

  @Override
  public List<PartitionSpec> apply() {
    TableMetadata current = ops.refresh();
    TableMetadata newMetadata = removeUnusedSpecs(current);
    return newMetadata.specs();
  }

  @Override
  public void commit() {
    TableMetadata base = ops.refresh();
    Tasks.foreach(ops)
        .retry(base.propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
        .exponentialBackoff(
            base.propertyAsInt(COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
            base.propertyAsInt(COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
            base.propertyAsInt(COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
            2.0 /* exponential */)
        .onlyRetryOn(CommitFailedException.class)
        .run(taskOps -> {
          TableMetadata current = ops.refresh();
          TableMetadata newMetadata = removeUnusedSpecs(current);
          taskOps.commit(current, newMetadata);
        });
  }

  private TableMetadata removeUnusedSpecs(TableMetadata current) {
    List<PartitionSpec> specs = current.specs();
    int currentSpecId = current.defaultSpecId();

    // Read ManifestLists and get all specId's in use
    Set<Integer> specsInUse =
        Sets.newHashSet(
            CloseableIterable.transform(
                MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.ALL_ENTRIES)
                    .newScan()
                    .planFiles(),
                fileScanTask -> ((ManifestEntriesTable.ManifestReadTask) (fileScanTask)).partitionSpecId()
            ));

    List<PartitionSpec> remainingSpecs = specs.stream()
        .filter(spec -> spec.specId() == currentSpecId || specsInUse.contains(spec.specId()))
        .collect(Collectors.toList());

    return current.withSpecs(remainingSpecs);
  }
}
