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

package org.apache.iceberg.flink.source.enumerator;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.source.FlinkSplitPlanner;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.assigner.SplitAssigner;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One-time split enumeration at the start-up
 */
public class StaticIcebergEnumerator extends AbstractIcebergEnumerator {
  private static final Logger LOG = LoggerFactory.getLogger(StaticIcebergEnumerator.class);

  private final SplitAssigner assigner;
  private final Table table;
  private final ScanContext scanContext;
  private final boolean shouldEnumerate;

  public StaticIcebergEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumeratorContext,
      SplitAssigner assigner,
      Table table,
      ScanContext scanContext,
      @Nullable IcebergEnumeratorState enumState) {
    super(enumeratorContext, assigner);
    this.assigner = assigner;
    this.table = table;
    this.scanContext = scanContext;
    // split enumeration is not needed during restore scenario
    this.shouldEnumerate = enumState == null;
  }

  @Override
  public void start() {
    super.start();
    if (shouldEnumerate) {
      List<IcebergSourceSplit> splits = FlinkSplitPlanner.planIcebergSourceSplits(table, scanContext);
      assigner.onDiscoveredSplits(splits);
      LOG.info("Discovered {} splits from table {} during job initialization",
            splits.size(), table.name());
    }
  }

  @Override
  protected boolean shouldWaitForMoreSplits() {
    return false;
  }

  @Override
  public IcebergEnumeratorState snapshotState(long checkpointId) {
    return new IcebergEnumeratorState(null, assigner.state());
  }
}
