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
import java.util.Optional;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SetStatistics implements UpdateStatistics {
  private static final Logger LOG = LoggerFactory.getLogger(SetStatistics.class);
  private final TableOperations ops;
  private final Map<Long, Optional<StatisticsFile>> statisticsToSet = Maps.newHashMap();

  public SetStatistics(TableOperations ops) {
    this.ops = ops;
  }

  @Override
  public UpdateStatistics setStatistics(StatisticsFile statisticsFile) {
    statisticsToSet.put(statisticsFile.snapshotId(), Optional.of(statisticsFile));
    return this;
  }

  @Override
  public UpdateStatistics removeStatistics(long snapshotId) {
    statisticsToSet.put(snapshotId, Optional.empty());
    return this;
  }

  @Override
  public List<StatisticsFile> apply() {
    return internalApply(ops.current()).statisticsFiles();
  }

  @Override
  public void commit() {
    // Get current metadata for retry configuration
    TableMetadata currentMetadata = ops.current();

    // Retry loop with exponential backoff
    Tasks.foreach(ops)
        .retry(currentMetadata.propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
        .exponentialBackoff(
            currentMetadata.propertyAsInt(
                COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
            currentMetadata.propertyAsInt(
                COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
            currentMetadata.propertyAsInt(
                COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
            2.0)
        .onlyRetryOn(CommitFailedException.class)
        .run(
            taskOps -> {
              // Refresh metadata on each retry attempt
              TableMetadata base = taskOps.refresh();
              TableMetadata newMetadata = internalApply(base);

              // Skip if no changes
              if (base == newMetadata) {
                LOG.info("No statistics changes to commit, skipping");
                return;
              }

              // Attempt commit
              taskOps.commit(base, newMetadata);
            });
  }

  private TableMetadata internalApply(TableMetadata base) {
    TableMetadata.Builder builder = TableMetadata.buildFrom(base);
    statisticsToSet.forEach(
        (snapshotId, statistics) -> {
          if (statistics.isPresent()) {
            builder.setStatistics(statistics.get());
          } else {
            builder.removeStatistics(snapshotId);
          }
        });
    return builder.build();
  }
}
