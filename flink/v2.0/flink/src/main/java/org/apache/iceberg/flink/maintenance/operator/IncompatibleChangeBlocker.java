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
package org.apache.iceberg.flink.maintenance.operator;

import java.util.List;
import java.util.Locale;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provide a way to block incompatible changes for the table, like spec changes which could cause
 * issues with compaction.
 */
@Internal
public class IncompatibleChangeBlocker extends AbstractStreamOperator<Trigger>
    implements OneInputStreamOperator<Trigger, Trigger> {
  private static final Logger LOG = LoggerFactory.getLogger(IncompatibleChangeBlocker.class);

  private transient Counter errorCounter;
  private transient Counter incompatibleSpecChangeCounter;

  private final TableLoader tableLoader;
  private final boolean failOnError;
  private final String taskName;
  private final int taskIndex;

  private transient Table table;
  private transient ListState<Integer> specIdState;
  private transient Integer specId;

  public IncompatibleChangeBlocker(
      String taskName, int taskIndex, TableLoader tableLoader, boolean failOnError) {
    this.taskName = taskName;
    this.taskIndex = taskIndex;
    this.tableLoader = tableLoader;
    this.failOnError = failOnError;
  }

  @Override
  public void open() throws Exception {
    super.open();
    if (!tableLoader.isOpen()) {
      tableLoader.open();
    }

    this.table = tableLoader.loadTable();
    this.errorCounter =
        TableMaintenanceMetrics.groupFor(getRuntimeContext(), table.name(), taskName, taskIndex)
            .counter(TableMaintenanceMetrics.ERROR_COUNTER);
    this.incompatibleSpecChangeCounter =
        TableMaintenanceMetrics.groupFor(getRuntimeContext(), table.name(), taskName, taskIndex)
            .counter(TableMaintenanceMetrics.INCOMPATIBLE_SPEC_CHANGE);
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    this.specIdState =
        context
            .getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>("incompatibleChangeBlockerSpecIdState", Types.INT));

    if (context.isRestored()) {
      List<Integer> specIdStateList = Lists.newArrayList(specIdState.get().iterator());
      specId = !specIdStateList.isEmpty() ? specIdStateList.get(0) : null;
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    if (specId != null) {
      specIdState.clear();
      specIdState.add(specId);
    }
  }

  @Override
  public void processElement(StreamRecord<Trigger> element) throws Exception {
    if (element.isRecord()) {
      long timestamp = element.getTimestamp();

      table.refresh();
      int currentSpecId = table.spec().specId();
      if (null == specId) {
        specId = currentSpecId;
      }

      if (specId != currentSpecId) {
        LOG.warn(
            "Incompatible schema change for {} between schema: {} and {} at {}",
            table.name(),
            specId,
            currentSpecId,
            timestamp);

        incompatibleSpecChangeCounter.inc();
        errorCounter.inc();
        Exception error =
            new RuntimeException(
                String.format(
                    Locale.ROOT,
                    "Incompatible schema change for %s between schema: %d and %d at %d",
                    table.name(),
                    specId,
                    currentSpecId,
                    timestamp));

        if (failOnError) {
          throw new SuppressRestartsException(error);
        } else {
          output.collect(TaskResultAggregator.ERROR_STREAM, new StreamRecord<>(error, timestamp));
        }
      }
    }
  }
}
