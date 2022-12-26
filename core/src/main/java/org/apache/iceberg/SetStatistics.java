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
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class SetStatistics extends BasePendingUpdate<List<StatisticsFile>>
    implements UpdateStatistics {
  private final TableOperations ops;
  private final Map<Long, Optional<StatisticsFile>> statisticsToSet = Maps.newHashMap();

  public SetStatistics(TableOperations ops) {
    this.ops = ops;
  }

  @Override
  public UpdateStatistics setStatistics(long snapshotId, StatisticsFile statisticsFile) {
    Preconditions.checkArgument(snapshotId == statisticsFile.snapshotId());
    statisticsToSet.put(snapshotId, Optional.of(statisticsFile));
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
    TableMetadata base = ops.current();
    TableMetadata newMetadata = internalApply(base);
    validate(base);
    ops.commit(base, newMetadata);
  }

  private TableMetadata internalApply(TableMetadata base) {
    TableMetadata.Builder builder = TableMetadata.buildFrom(base);
    statisticsToSet.forEach(
        (snapshotId, statistics) -> {
          if (statistics.isPresent()) {
            builder.setStatistics(snapshotId, statistics.get());
          } else {
            builder.removeStatistics(snapshotId);
          }
        });
    return builder.build();
  }
}
