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

import java.util.Collection;
import java.util.List;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitSerializer;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Filters {@link FileScanTask}s based on a list of allowed file names. */
@Internal
public class FilterSplitsByFileName extends AbstractStreamOperator<MetadataTablePlanner.SplitInfo>
    implements TwoInputStreamOperator<
        ManifestFile, MetadataTablePlanner.SplitInfo, MetadataTablePlanner.SplitInfo> {

  private static final Logger LOG = LoggerFactory.getLogger(FilterSplitsByFileName.class);

  private transient ListState<MetadataTablePlanner.SplitInfo> splitInfoListState;
  private transient ListState<String> allowedFileNamesState;

  private IcebergSourceSplitSerializer splitSerializer;
  private final ScanContext scanContext;

  public FilterSplitsByFileName() {
    this.scanContext = ScanContext.builder().streaming(true).build();
  }

  @Override
  public void open() throws Exception {
    super.open();
    this.splitSerializer = new IcebergSourceSplitSerializer(scanContext.caseSensitive());
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    splitInfoListState =
        context
            .getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>(
                    "SplitInfoList", TypeInformation.of(MetadataTablePlanner.SplitInfo.class)));
    allowedFileNamesState =
        context
            .getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("AllowedFileNames", Types.STRING));
  }

  @Override
  public void processElement1(StreamRecord<ManifestFile> element) throws Exception {
    if (element.isRecord()) {
      allowedFileNamesState.add(element.getValue().path());
    }
  }

  @Override
  public void processElement2(StreamRecord<MetadataTablePlanner.SplitInfo> element)
      throws Exception {
    if (element.isRecord()) {
      splitInfoListState.add(element.getValue());
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    List<String> allowedFileNames = Lists.newArrayList(allowedFileNamesState.get().iterator());
    for (MetadataTablePlanner.SplitInfo splitInfo : splitInfoListState.get()) {
      IcebergSourceSplit split =
          splitSerializer.deserialize(splitInfo.version(), splitInfo.split());
      Collection<FileScanTask> tasks = split.task().tasks();
      List<FileScanTask> filteredTasks = Lists.newArrayListWithExpectedSize(tasks.size());
      for (FileScanTask task : tasks) {
        if (allowedFileNames.contains(task.file().location())) {
          filteredTasks.add(task);
        } else {
          LOG.debug(
              "Filtered out file {} as it is not in the allowed file list {} at {}",
              task.file().location(),
              allowedFileNames,
              mark.getTimestamp());
        }
      }

      if (filteredTasks.size() < tasks.size()) {
        if (!filteredTasks.isEmpty()) {
          IcebergSourceSplit icebergSourceSplit =
              IcebergSourceSplit.fromCombinedScanTask(new BaseCombinedScanTask(filteredTasks));
          output.collect(
              new StreamRecord<>(
                  new MetadataTablePlanner.SplitInfo(
                      splitSerializer.getVersion(), splitSerializer.serialize(icebergSourceSplit)),
                  mark.getTimestamp()));
        }
      } else {
        output.collect(new StreamRecord<>(splitInfo, mark.getTimestamp()));
      }
    }

    super.processWatermark(mark);
  }
}
