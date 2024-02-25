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
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

public class SetPartitionStatistics implements UpdatePartitionStatistics {
  private final TableOperations ops;
  private final Map<Long, PartitionStatisticsFile> statsToSet = Maps.newHashMap();
  private final Set<Long> statsToRemove = Sets.newHashSet();

  public SetPartitionStatistics(TableOperations ops) {
    this.ops = ops;
  }

  @Override
  public UpdatePartitionStatistics setPartitionStatistics(PartitionStatisticsFile file) {
    Preconditions.checkArgument(null != file, "partition statistics file must not be null");
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
    TableMetadata base = ops.current();
    TableMetadata newMetadata = internalApply(base);
    ops.commit(base, newMetadata);
  }

  private TableMetadata internalApply(TableMetadata base) {
    TableMetadata.Builder builder = TableMetadata.buildFrom(base);
    statsToSet.values().forEach(builder::setPartitionStatistics);
    statsToRemove.forEach(builder::removePartitionStatistics);
    return builder.build();
  }
}
