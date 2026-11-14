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

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Tasks;

public class SetPartitionStatistics implements UpdatePartitionStatistics {

  private final TableOperations ops;
  private final Map<Long, PartitionStatisticsFile> statsToSet = Maps.newHashMap();
  private final Set<Long> statsToRemove = Sets.newHashSet();

  public SetPartitionStatistics(TableOperations ops) {
    this.ops = ops;
  }

  @Override
  public UpdatePartitionStatistics setPartitionStatistics(PartitionStatisticsFile file) {
    if (file == null) {
      return this;
    }

    statsToSet.put(file.snapshotId(), file);
    return this;
  }

  @Override
  public UpdatePartitionStatistics removePartitionStatistics(long snapshotId) {
    statsToRemove.add(snapshotId);
    return this;
  }

  @Override
  public List<PartitionStatisticsFile> apply() {
    return internalApply(ops.current()).partitionStatisticsFiles();
  }

  @Override
  public void commit() {
    Tasks.foreach(ops)
        .retry(ops.current().propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
        .exponentialBackoff(
            ops.current().propertyAsInt(COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
            ops.current().propertyAsInt(COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
            ops.current()
                .propertyAsInt(COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
            2.0 /* exponential */)
        .onlyRetryOn(CommitFailedException.class)
        .run(
            taskOps -> {
              TableMetadata base = taskOps.refresh();
              TableMetadata updated = internalApply(base);
              taskOps.commit(base, updated);
            });
  }

  private TableMetadata internalApply(TableMetadata base) {
    TableMetadata.Builder builder = TableMetadata.buildFrom(base);
    statsToSet.values().forEach(builder::setPartitionStatistics);
    statsToRemove.forEach(builder::removePartitionStatistics);
    return builder.build();
  }
}
