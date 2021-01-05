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

package org.apache.iceberg.flink.source;

import java.io.IOException;
import java.util.Queue;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.JavaSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.StreamSourceContexts;
import org.apache.flink.streaming.api.operators.YieldingOperatorFactory;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The operator that reads the {@link FlinkInputSplit splits} received from the preceding {@link
 * StreamingMonitorFunction}. Contrary to the {@link StreamingMonitorFunction} which has a parallelism of 1,
 * this operator can have multiple parallelism.
 *
 * <p>As soon as a split descriptor is received, it is put in a queue, and use {@link MailboxExecutor}
 * read the actual data of the split. This architecture allows the separation of the reading thread from the one split
 * processing the checkpoint barriers, thus removing any potential back-pressure.
 */
public class StreamingReaderOperator extends AbstractStreamOperator<RowData>
    implements OneInputStreamOperator<FlinkInputSplit, RowData> {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingReaderOperator.class);
  private final MailboxExecutorImpl executor;
  private FlinkInputFormat format;

  private transient SourceFunction.SourceContext<RowData> readerContext;

  private transient ListState<FlinkInputSplit> checkpointState;
  private transient Queue<FlinkInputSplit> splits;

  private StreamingReaderOperator(FlinkInputFormat format, ProcessingTimeService timeService,
                                  MailboxExecutor mailboxExecutor) {
    this.format = Preconditions.checkNotNull(format, "The InputFormat should not be null.");
    this.processingTimeService = timeService;
    this.executor = (MailboxExecutorImpl) Preconditions.checkNotNull(mailboxExecutor,
        "The mailboxExecutor should not be null.");
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);

    checkpointState = context.getOperatorStateStore().getListState(
        new ListStateDescriptor<>("splits", new JavaSerializer<>()));

    int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
    splits = Lists.newLinkedList();
    if (context.isRestored()) {
      LOG.info("Restoring state for the {} (taskIdx={}).", getClass().getSimpleName(), subtaskIdx);

      for (FlinkInputSplit split : checkpointState.get()) {
        splits.add(split);
      }
    }

    this.readerContext = StreamSourceContexts.getSourceContext(
        getOperatorConfig().getTimeCharacteristic(),
        getProcessingTimeService(),
        new Object(), // no actual locking needed
        getContainingTask().getStreamStatusMaintainer(),
        output,
        getRuntimeContext().getExecutionConfig().getAutoWatermarkInterval(),
        -1);

    if (!splits.isEmpty()) {
      enqueueProcessSplits();
    }
  }

  @Override
  public void processElement(StreamRecord<FlinkInputSplit> element) {
    splits.offer(element.getValue());
    enqueueProcessSplits();
  }

  private void enqueueProcessSplits() {
    executor.execute(this::processSplits, this.getClass().getSimpleName());
  }

  private void processSplits() throws IOException {
    do {
      FlinkInputSplit split = splits.poll();
      if (split == null) {
        return;
      }

      LOG.debug("load split: {}", split);
      format.open(split);

      RowData nextElement = null;
      while (!format.reachedEnd()) {
        nextElement = format.nextRecord(nextElement);
        if (nextElement != null) {
          readerContext.collect(nextElement);
        } else {
          break;
        }
      }

      format.close();
    } while (executor.isIdle());
    enqueueProcessSplits();
  }

  @Override
  public void processWatermark(Watermark mark) {
    // we do nothing because we emit our own watermarks if needed.
  }

  @Override
  public void dispose() throws Exception {
    super.dispose();

    if (format != null) {
      format.close();
      format.closeInputFormat();
      format = null;
    }

    readerContext = null;
  }

  @Override
  public void close() throws Exception {
    super.close();
    output.close();
    if (readerContext != null) {
      readerContext.emitWatermark(Watermark.MAX_WATERMARK);
      readerContext.close();
      readerContext = null;
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);

    checkpointState.clear();
    for (FlinkInputSplit split : splits) {
      checkpointState.add(split);
    }
  }

  public static OneInputStreamOperatorFactory<FlinkInputSplit, RowData> factory(FlinkInputFormat format) {
    return new OperatorFactory(format);
  }

  private static class OperatorFactory extends AbstractStreamOperatorFactory<RowData>
      implements YieldingOperatorFactory<RowData>, OneInputStreamOperatorFactory<FlinkInputSplit, RowData> {

    private final FlinkInputFormat format;

    private transient MailboxExecutor mailboxExecutor;

    private OperatorFactory(FlinkInputFormat format) {
      this.format = format;
    }

    @Override
    public void setMailboxExecutor(MailboxExecutor mailboxExecutor) {
      this.mailboxExecutor = mailboxExecutor;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <O extends StreamOperator<RowData>> O createStreamOperator(StreamOperatorParameters<RowData> parameters) {
      StreamingReaderOperator operator = new StreamingReaderOperator(format, processingTimeService, mailboxExecutor);
      operator.setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
      return (O) operator;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
      return StreamingReaderOperator.class;
    }
  }
}
