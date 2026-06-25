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
package org.apache.iceberg.connect.channel;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.Offset;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.iceberg.connect.data.SinkWriterResult;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Worker extends Channel {
  private static final Logger LOG = LoggerFactory.getLogger(Worker.class);

  private final IcebergSinkConfig config;
  private final SinkTaskContext context;
  private final SinkWriter sinkWriter;

  // Under normal operation, the main thread drains the queue on every put() call, so the queue
  // depth is effectively bounded by the commit rate.
  private final ConcurrentLinkedQueue<Envelope> controlEventQueue;
  private final ExecutorService pollingExecutor;
  private final AtomicBoolean running;
  private final Duration pollInterval;
  private final String taskId;
  private final AtomicReference<Exception> errorRef = new AtomicReference<>(null);

  Worker(
      IcebergSinkConfig config,
      KafkaClientFactory clientFactory,
      SinkWriter sinkWriter,
      SinkTaskContext context) {
    // pass transient consumer group ID to which we never commit offsets
    super(
        "worker",
        config.controlGroupIdPrefix() + UUID.randomUUID(),
        config,
        clientFactory,
        context);

    this.config = config;
    this.context = context;
    this.sinkWriter = sinkWriter;

    this.taskId = config.connectorName() + "-" + config.taskId();
    this.controlEventQueue = new ConcurrentLinkedQueue<>();
    this.running = new AtomicBoolean(false);
    this.pollInterval = Duration.ofMillis(config.controlPollIntervalMs());
    this.pollingExecutor =
        Executors.newSingleThreadExecutor(
            r -> {
              Thread thread = new Thread(r, "worker-control-poller-" + taskId);
              thread.setDaemon(true);
              return thread;
            });
  }

  @Override
  void start() {
    // Do NOT call super.start() — all consumer access must happen on the background thread
    // to satisfy KafkaConsumer's single-thread requirement. The background thread calls
    // initializeConsumer() as its first action.
    running.set(true);

    try {
      pollingExecutor.execute(this::backgroundPoll);
    } catch (Exception ex) {
      LOG.error("Worker {} failed to execute the task.", taskId, ex);
      throw new ConnectException(
          String.format("Worker %s failed to execute the poll task", taskId));
    }
    LOG.info(
        "Worker {} started with async control event processing (poll interval: {}ms)",
        taskId,
        pollInterval.toMillis());
  }

  /**
   * Background polling task that subscribes to the control topic and continuously polls for events,
   * buffering them for processing by the main thread. All KafkaConsumer access is confined to this
   * thread.
   */
  private void backgroundPoll() {
    LOG.info("Background control topic polling thread started on {}", taskId);
    try {
      // Initialize consumer on this thread — KafkaConsumer is NOT thread-safe,
      // so subscribe + all poll calls must happen on the same thread.
      initializeConsumer();

      while (running.get() && !Thread.currentThread().isInterrupted()) {
        try {
          consumeAvailable(pollInterval);
        } catch (WakeupException e) {
          // Expected during shutdown — wakeupConsumer() was called
          LOG.debug("Consumer wakeup received during shutdown for {}", taskId, e);
          break;
        }
      }
    } catch (Exception e) {
      if (running.compareAndSet(true, false)) {
        LOG.error("Worker {} failed while polling control events", taskId, e);
        errorRef.compareAndSet(null, e);
      }
    } finally {
      LOG.info("Background control topic polling thread stopped on {}", taskId);
    }
  }

  /**
   * Process all available events from the queue (non-blocking). This is called from the main put()
   * path. Throws ConnectException if the background thread encountered an error — the caller is
   * responsible for stopping the worker.
   */
  void process() {
    Exception ex = errorRef.getAndSet(null);
    if (ex != null) {
      throw new ConnectException(
          String.format("Worker %s failed while processing async polling.", taskId), ex);
    }
    Envelope envelope;
    while ((envelope = controlEventQueue.poll()) != null) {
      handleStartCommit(((StartCommit) envelope.event().payload()).commitId());
    }
  }

  private void handleStartCommit(UUID commitId) {
    SinkWriterResult results = sinkWriter.completeWrite();

    // include all assigned topic partitions even if no messages were read
    // from a partition, as the coordinator will use that to determine
    // when all data for a commit has been received
    List<TopicPartitionOffset> assignments =
        context.assignment().stream()
            .map(
                tp -> {
                  Offset offset = results.sourceOffsets().get(tp);
                  if (offset == null) {
                    offset = Offset.NULL_OFFSET;
                  }
                  return new TopicPartitionOffset(
                      tp.topic(), tp.partition(), offset.offset(), offset.timestamp());
                })
            .collect(Collectors.toList());

    List<Event> events =
        results.writerResults().stream()
            .map(
                writeResult ->
                    new Event(
                        config.connectGroupId(),
                        new DataWritten(
                            writeResult.partitionStruct(),
                            commitId,
                            writeResult.tableReference(),
                            writeResult.dataFiles(),
                            writeResult.deleteFiles())))
            .collect(Collectors.toList());

    Event readyEvent = new Event(config.connectGroupId(), new DataComplete(commitId, assignments));
    events.add(readyEvent);

    send(events, results.sourceOffsets());
  }

  @Override
  protected boolean receive(Envelope envelope) {
    if (envelope.event().payload().type() == PayloadType.START_COMMIT) {
      controlEventQueue.offer(envelope);
      LOG.debug("Worker {} buffered START_COMMIT event", taskId);
      return true;
    }
    return false;
  }

  @Override
  void stop() {
    LOG.info("Worker {} stopping.", taskId);
    terminateBackGroundPolling();
    sinkWriter.close();
    super.stop();
    LOG.info("Worker {} stopped.", taskId);
  }

  private void terminateBackGroundPolling() {
    running.set(false);
    wakeupConsumer();
    pollingExecutor.shutdownNow();
    try {
      if (!pollingExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
        LOG.warn("Polling thread did not terminate in time on worker {}, forcing shutdown", taskId);
        throw new ConnectException(
            String.format(
                "Background polling thread of worker %s did not terminate gracefully.", taskId));
      }
    } catch (InterruptedException e) {
      LOG.warn("Worker {} got interrupted while waiting for polling thread shutdown", taskId, e);
      throw new ConnectException(
          String.format(
              "Background polling thread of worker %s interrupted while closing.", taskId),
          e);
    }
    controlEventQueue.clear();
    errorRef.set(null);
  }

  void save(Collection<SinkRecord> sinkRecords) {
    sinkWriter.save(sinkRecords);
  }

  @VisibleForTesting
  int pendingEventCount() {
    return controlEventQueue.size();
  }
}
