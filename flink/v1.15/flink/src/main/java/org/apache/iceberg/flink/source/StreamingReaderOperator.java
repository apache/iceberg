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
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.JavaSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
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
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The operator that reads the {@link FlinkInputSplit splits} received from the preceding {@link
 * StreamingMonitorFunction}. Contrary to the {@link StreamingMonitorFunction} which has a
 * parallelism of 1, this operator can have multiple parallelism.
 *
 * <p>As soon as a split descriptor is received, it is put in a queue, and use {@link
 * MailboxExecutor} read the actual data of the split. This architecture allows the separation of
 * the reading thread from the one split processing the checkpoint barriers, thus removing any
 * potential back-pressure.
 */
public class StreamingReaderOperator extends AbstractStreamOperator<RowData>
    implements OneInputStreamOperator<FlinkInputSplit, RowData> {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingReaderOperator.class);

  private enum SplitState {
    IDLE {
      @Override
      public boolean prepareToProcessRecord(StreamingReaderOperator op) {
        throw new IllegalStateException("not processing any records in IDLE state");
      }
    },
    /** A message is enqueued to process split, but no split is opened. */
    OPENING {
      @Override
      public boolean prepareToProcessRecord(StreamingReaderOperator op) throws IOException {
        if (op.splits.isEmpty()) {
          op.switchState(SplitState.IDLE);
          return false;
        } else {
          op.loadSplit(op.splits.poll());
          op.switchState(SplitState.READING);
          return true;
        }
      }
    },
    /** A message is enqueued to process split and its processing was started. */
    READING {
      @Override
      public boolean prepareToProcessRecord(StreamingReaderOperator op) {
        return true;
      }

      @Override
      public void onNoMoreData(StreamingReaderOperator op) {
        op.switchState(SplitState.IDLE);
      }
    },
    /**
     * No further processing can be done; only state disposal transition to {@link #FINISHED}
     * allowed.
     */
    FAILED {
      @Override
      public boolean prepareToProcessRecord(StreamingReaderOperator op) {
        throw new IllegalStateException("not processing any records in ERRORED state");
      }
    },
    /**
     * {@link #close()} was called but unprocessed data (records and splits) remains and needs to be
     * processed. {@link #close()} caller is blocked.
     */
    FINISHING {
      @Override
      public boolean prepareToProcessRecord(StreamingReaderOperator op) throws IOException {
        if (op.currentSplit == null && !op.splits.isEmpty()) {
          op.loadSplit(op.splits.poll());
        }
        return true;
      }

      @Override
      public void onNoMoreData(StreamingReaderOperator op) {
        // need one more mail to unblock possible yield() in close() method (todo: wait with timeout
        // in yield)
        op.enqueueProcessRecord();
        op.switchState(FINISHED);
      }
    },
    FINISHED {
      @Override
      public boolean prepareToProcessRecord(StreamingReaderOperator op) {
        LOG.warn("not processing any records while closed");
        return false;
      }
    };

    private static final Set<SplitState> ACCEPT_SPLITS = EnumSet.of(IDLE, OPENING, READING);
    /** Possible transition FROM each state. */
    private static final Map<SplitState, Set<SplitState>> VALID_TRANSITIONS;

    static {
      Map<SplitState, Set<SplitState>> tmpTransitions = Maps.newHashMap();
      tmpTransitions.put(IDLE, EnumSet.of(OPENING, FINISHED, FAILED));
      tmpTransitions.put(OPENING, EnumSet.of(READING, FINISHING, FAILED));
      tmpTransitions.put(READING, EnumSet.of(IDLE, OPENING, FINISHING, FAILED));
      tmpTransitions.put(FINISHING, EnumSet.of(FINISHED, FAILED));
      tmpTransitions.put(FAILED, EnumSet.of(FINISHED));
      tmpTransitions.put(FINISHED, EnumSet.noneOf(SplitState.class));
      VALID_TRANSITIONS = new EnumMap<>(tmpTransitions);
    }

    public boolean isAcceptingSplits() {
      return ACCEPT_SPLITS.contains(this);
    }

    public final boolean isTerminal() {
      return this == FINISHED;
    }

    public boolean canSwitchTo(SplitState next) {
      return VALID_TRANSITIONS.getOrDefault(this, EnumSet.noneOf(SplitState.class)).contains(next);
    }

    /**
     * Prepare to process new record OR split.
     *
     * @return true if should read the record
     */
    public abstract boolean prepareToProcessRecord(StreamingReaderOperator op) throws IOException;

    public void onNoMoreData(StreamingReaderOperator op) {}
  }

  // It's the same thread that is running this operator and checkpoint actions. we use this executor
  // to schedule only
  // one split for future reading, so that a new checkpoint could be triggered without blocking long
  // time for exhausting
  // all scheduled splits.
  private transient MailboxExecutorImpl executor;
  private transient FlinkInputFormat format;

  private transient SourceFunction.SourceContext<RowData> sourceContext;

  private transient ListState<FlinkInputSplit> checkpointedState;
  private transient SplitState state;

  private transient Queue<FlinkInputSplit> splits = Lists.newLinkedList();
  private transient FlinkInputSplit
      currentSplit; // can't work just on queue tail because it can change because it's PQ

  private final transient RunnableWithException processRecordAction =
      () -> {
        try {
          processRecord();
        } catch (Exception e) {
          switchState(SplitState.FAILED);
          throw e;
        }
      };

  private StreamingReaderOperator(
      FlinkInputFormat format, ProcessingTimeService timeService, MailboxExecutor mailboxExecutor) {
    this.format = Preconditions.checkNotNull(format, "The format should not be null.");
    this.processingTimeService =
        Preconditions.checkNotNull(timeService, "The timeService should not be null.");

    this.executor =
        (MailboxExecutorImpl)
            Preconditions.checkNotNull(mailboxExecutor, "The mailboxExecutor should not be null.");
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);

    Preconditions.checkArgument(
        checkpointedState == null, "The state has already been initialized.");

    // TODO Replace Java serialization with Avro approach to keep state compatibility.
    // See issue: https://github.com/apache/iceberg/issues/1698
    checkpointedState =
        context
            .getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("splits", new JavaSerializer<>()));

    int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
    if (!context.isRestored()) {
      LOG.info(
          "No state to restore for the {} (taskIdx={}).", getClass().getSimpleName(), subtaskIdx);
      return;
    }

    LOG.info("Restoring state for the {} (taskIdx={}).", getClass().getSimpleName(), subtaskIdx);

    splits = splits == null ? Lists.newLinkedList() : splits;

    for (FlinkInputSplit split : checkpointedState.get()) {
      splits.add(split);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("{} (taskIdx={}) restored {}.", getClass().getSimpleName(), subtaskIdx, splits);
    }
  }

  @Override
  public void open() throws Exception {
    super.open();

    // Initialize the current split state to IDLE.
    state = SplitState.IDLE;
    this.sourceContext =
        StreamSourceContexts.getSourceContext(
            getOperatorConfig().getTimeCharacteristic(),
            getProcessingTimeService(),
            new Object(), // no actual locking needed
            output,
            getRuntimeContext().getExecutionConfig().getAutoWatermarkInterval(),
            -1,
            true);

    this.splits = this.splits == null ? Lists.newLinkedList() : this.splits;

    // Enqueue to process the recovered input splits.
    if (!splits.isEmpty()) {
      enqueueProcessRecord();
    }
  }

  @Override
  public void processElement(StreamRecord<FlinkInputSplit> element) {
    Preconditions.checkState(state.isAcceptingSplits());
    splits.offer(element.getValue());
    if (state == SplitState.IDLE) {
      enqueueProcessRecord();
    }
  }

  private void enqueueProcessRecord() {
    Preconditions.checkState(!state.isTerminal(), "can't enqueue mail in terminal state %s", state);
    executor.execute(processRecordAction, "StreamingReaderOperator");
    if (state == SplitState.IDLE) {
      switchState(SplitState.OPENING);
    }
  }

  private void processRecord() throws IOException {
    do {
      if (!state.prepareToProcessRecord(this)) {
        return;
      }

      readAndCollectRecord();

      if (format.reachedEnd()) {
        onSplitProcessed();
        return;
      }
    } while (executor.isIdle());
    // todo: consider moving this loop into MailboxProcessor (return boolean "re-execute" from
    // enqueued action)

    enqueueProcessRecord();
  }

  private void onSplitProcessed() throws IOException {
    LOG.debug("split processed: {}", currentSplit);
    format.close();
    currentSplit = null;

    if (splits.isEmpty()) {
      state.onNoMoreData(this);
      return;
    }

    if (state == SplitState.READING) {
      switchState(SplitState.OPENING);
    }

    enqueueProcessRecord();
  }

  private void readAndCollectRecord() throws IOException {
    Preconditions.checkState(
        state == SplitState.READING || state == SplitState.FINISHING,
        "can't process record in state %s",
        state);

    if (format.reachedEnd()) {
      return;
    }

    RowData out = format.nextRecord(null);
    if (out != null) {
      sourceContext.collect(out);
    }
  }

  private void loadSplit(FlinkInputSplit split) throws IOException {
    Preconditions.checkState(
        state != SplitState.READING && state != SplitState.FINISHED,
        "can't load split in state %s",
        state);
    Preconditions.checkNotNull(split, "split is null");
    LOG.debug("load split: {}", split);
    currentSplit = split;
    format.open(currentSplit);
  }

  private void switchState(SplitState newState) {
    if (state != newState) {
      Preconditions.checkState(
          state.canSwitchTo(newState),
          "can't switch state from terminal state %s to %s",
          state,
          newState);
      LOG.debug("switch state: {} -> {}", state, newState);
      state = newState;
    }
  }

  @Override
  public void processWatermark(Watermark mark) {
    // we do nothing because we emit our own watermarks if needed.
  }

  @Override
  public void finish() throws Exception {
    LOG.debug("finishing");
    super.finish();

    switch (state) {
      case IDLE:
        switchState(SplitState.FINISHED);
        break;
      case FINISHED:
        LOG.warn("operator is already closed, doing nothing");
        return;
      default:
        switchState(SplitState.FINISHING);
        while (!state.isTerminal()) {
          executor.yield();
        }
    }

    try {
      sourceContext.emitWatermark(Watermark.MAX_WATERMARK);
    } catch (Exception e) {
      LOG.warn("unable to emit watermark while closing", e);
    }
  }

  @Override
  public void close() throws Exception {
    Exception exc = null;
    try {
      cleanUp();
    } catch (Exception ex) {
      exc = ex;
    }

    checkpointedState = null;
    currentSplit = null;
    executor = null;
    format = null;
    sourceContext = null;
    splits = null;

    try {
      super.close();
    } catch (Exception ex) {
      exc = ExceptionUtils.firstOrSuppressed(ex, exc);
    }

    if (exc != null) {
      throw exc;
    }
  }

  private void cleanUp() throws Exception {
    LOG.debug("cleanup, state={}", state);

    RunnableWithException[] runClose = {
      () -> sourceContext.close(),
      () -> format.close(),
      () -> {
        if (this.format instanceof RichInputFormat) {
          ((RichInputFormat<?, ?>) this.format).closeInputFormat();
        }
      }
    };

    Exception firstException = null;

    for (RunnableWithException r : runClose) {
      try {
        r.run();
      } catch (Exception e) {
        firstException = ExceptionUtils.firstOrSuppressed(e, firstException);
      }
    }

    currentSplit = null;
    if (firstException != null) {
      throw firstException;
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    Preconditions.checkState(
        checkpointedState != null, "The operator state has not been properly initialized.");

    int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();

    List<FlinkInputSplit> readerState = getReaderState();
    checkpointedState.clear();
    checkpointedState.addAll(readerState);

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "{} (taskIdx={}) checkpointed {} splits: {}.",
          getClass().getSimpleName(),
          subtaskIdx,
          readerState.size(),
          readerState);
    }
  }

  private List<FlinkInputSplit> getReaderState() throws IOException {
    List<FlinkInputSplit> snapshot = Lists.newArrayList();
    if (currentSplit != null) {
      snapshot.add(currentSplit);
    }

    snapshot.addAll(splits);
    return snapshot;
  }

  static OneInputStreamOperatorFactory<FlinkInputSplit, RowData> factory(FlinkInputFormat format) {
    return new OperatorFactory(format);
  }

  private static class OperatorFactory extends AbstractStreamOperatorFactory<RowData>
      implements YieldingOperatorFactory<RowData>,
          OneInputStreamOperatorFactory<FlinkInputSplit, RowData> {

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
    public <O extends StreamOperator<RowData>> O createStreamOperator(
        StreamOperatorParameters<RowData> parameters) {
      StreamingReaderOperator operator =
          new StreamingReaderOperator(format, processingTimeService, mailboxExecutor);
      operator.setup(
          parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
      return (O) operator;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
      return StreamingReaderOperator.class;
    }
  }
}
