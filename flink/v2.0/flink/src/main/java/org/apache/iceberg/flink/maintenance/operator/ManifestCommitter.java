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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Commits the manifest changes using {@link RewriteManifests}. */
@Internal
public class ManifestCommitter extends AbstractStreamOperator<Trigger>
    implements TwoInputStreamOperator<
        Tuple2<ManifestFile, ManifestCommitter.ManifestSource>, Exception, Trigger> {
  private static final Logger LOG = LoggerFactory.getLogger(ManifestCommitter.class);

  private transient Table table;
  private transient Counter errorCounter;
  private transient ListState<ManifestFile> newManifests;
  private transient ListState<ManifestFile> oldManifests;
  private transient ListState<Boolean> hasError;
  private boolean hasErrorFlag = false;

  private final TableLoader tableLoader;
  private final String taskName;
  private final int taskIndex;

  public ManifestCommitter(String taskName, int taskIndex, TableLoader tableLoader) {
    this.tableLoader = tableLoader;
    this.taskName = taskName;
    this.taskIndex = taskIndex;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    if (!tableLoader.isOpen()) {
      tableLoader.open();
    }

    this.table = tableLoader.loadTable();
    this.hasError =
        context
            .getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("blockOnErrorHasError", Types.BOOLEAN));

    if (!Iterables.isEmpty(hasError.get())) {
      hasErrorFlag = true;
    }

    this.newManifests =
        context
            .getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>(
                    "manifestUpdaterNewManifestFiles", TypeInformation.of(ManifestFile.class)));
    this.oldManifests =
        context
            .getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>(
                    "manifestUpdaterOldManifestFiles", TypeInformation.of(ManifestFile.class)));
    this.errorCounter =
        TableMaintenanceMetrics.groupFor(getRuntimeContext(), table.name(), taskName, taskIndex)
            .counter(TableMaintenanceMetrics.ERROR_COUNTER);
  }

  @Override
  public void processElement1(StreamRecord<Tuple2<ManifestFile, ManifestSource>> element)
      throws Exception {
    if (element.isRecord()) {
      Tuple2<ManifestFile, ManifestSource> value = element.getValue();
      if (ManifestSource.NEW.equals(value.f1)) {
        newManifests.add(value.f0);
      } else {
        oldManifests.add(value.f0);
      }
    }
  }

  @Override
  public void processElement2(StreamRecord<Exception> element) throws Exception {
    if (element.isRecord()) {
      if (!hasErrorFlag) {
        hasError.add(true);
        hasErrorFlag = true;
      }

      oldManifests.clear();
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    try {
      if (!hasErrorFlag) {
        table.refresh();
        RewriteManifests rewriteManifests = table.rewriteManifests();
        oldManifests.get().forEach(rewriteManifests::deleteManifest);
        newManifests.get().forEach(rewriteManifests::addManifest);
        rewriteManifests.commit();
      } else {
        LOG.info("Omitting commit on failure at {}", mark.getTimestamp());
      }
    } catch (Exception e) {
      try {
        newManifests.get().forEach(f -> table.io().deleteFile(f.path()));
      } catch (Exception ex) {
        LOG.error(
            "Exception removing non-committed manifest files for {} at {}",
            table,
            mark.getTimestamp(),
            ex);
      }
      output.collect(TaskResultAggregator.ERROR_STREAM, new StreamRecord<>(e, mark.getTimestamp()));
      errorCounter.inc();
    } finally {
      newManifests.clear();
      oldManifests.clear();
      hasError.clear();
      hasErrorFlag = false;
    }

    super.processWatermark(mark);
  }

  @Override
  public void close() throws Exception {
    super.close();

    if (tableLoader != null) {
      tableLoader.close();
      table = null;
    }
  }

  public enum ManifestSource {
    OLD,
    NEW
  }
}
