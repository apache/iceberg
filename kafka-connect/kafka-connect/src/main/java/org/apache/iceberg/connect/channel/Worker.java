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
import java.util.Objects;
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
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
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

  private final ConcurrentLinkedQueue<Envelope> controlEventQueue;
  private final ExecutorService pollingExecutor;
  private final AtomicBoolean running;
  private final Duration pollInterval;
  private final String workerIdentifier;
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

    this.workerIdentifier = config.connectorName() + "-" + config.taskId();
    // Initialize async processing components
    this.controlEventQueue = new ConcurrentLinkedQueue<>();
    this.running = new AtomicBoolean(false);
    this.pollInterval = Duration.ofMillis(config.controlPollIntervalMs());
    this.pollingExecutor =
        Executors.newSingleThreadExecutor(
            r -> {
              Thread thread =
                  new Thread(
                      r, "worker-control-poller-" + config.connectorName() + "-" + config.taskId());
              thread.setDaemon(true); // Not daemon to ensure graceful shutdown
              return thread;
            });
  }

  @Override
  void start() {
    super.start();
    running.set(true);

    try {
      // Start background polling thread
      pollingExecutor.execute(this::backgroundPoll);
    } catch (Exception ex) {
      LOG.error("Worker {} failed to execute the task.", workerIdentifier, ex);
      throw new ConnectException(
          String.format("Worker %s failed to execute the poll task", workerIdentifier));
    }
    LOG.info(
        "Worker {} started with async control event processing (poll interval: {}ms)",
        workerIdentifier,
        pollInterval.toMillis());
  }

  /**
   * Background polling task that continuously polls the control topic and buffers relevant events
   * for processing.
   */
  private void backgroundPoll() {
    LOG.info("Background control topic polling thread started on {}", workerIdentifier);
    try {
      while (running.get() && !Thread.currentThread().isInterrupted()) {
        try {
          // Poll control topic with configured interval
          consumeAvailable(pollInterval);
        } catch (Exception e) {
          if (running.compareAndSet(true, false)) {
            LOG.error("Worker {} failed while polling control events", workerIdentifier, e);
            errorRef.compareAndSet(null, e);
          }
          break;
        }
      }
    } finally {
      LOG.info("Background control topic polling thread stopped");
    }
  }

  /**
   * Process all available events from the queue (non-blocking). This is called from the main put()
   * path.
   */
  void process() {
    Exception ex = errorRef.getAndSet(null);
    if (ex != null) {
      stop();
      throw new ConnectException(
          String.format("Worker %s failed while processing async polling.", workerIdentifier), ex);
    }
    Envelope envelope;
    // Drain all available events from queue
    while ((envelope = controlEventQueue.poll()) != null) {
      processEnvelope(envelope);
    }
  }

  /** Process a single envelope (actual event handling logic). */
  private void processEnvelope(Envelope envelope) {
    Event event = envelope.event();
    if (Objects.requireNonNull(event.type()) == PayloadType.START_COMMIT) {
      handleStartCommit(((StartCommit) event.payload()).commitId());
    } else {
      LOG.warn(
          "Worker {} not responsible to process event of type {}.", workerIdentifier, event.type());
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
                            TableReference.of(config.catalogName(), writeResult.tableIdentifier()),
                            writeResult.dataFiles(),
                            writeResult.deleteFiles())))
            .collect(Collectors.toList());

    Event readyEvent = new Event(config.connectGroupId(), new DataComplete(commitId, assignments));
    events.add(readyEvent);

    send(events, results.sourceOffsets());
  }

  @Override
  protected boolean receive(Envelope envelope) {
    Event event = envelope.event();
    PayloadType type = event.payload().type();

    // Only buffer control events that need processing
    if (type == PayloadType.START_COMMIT
        || type == PayloadType.COMMIT_COMPLETE
        || type == PayloadType.COMMIT_TO_TABLE) {
      controlEventQueue.offer(envelope);
      LOG.info("Worker {} buffered control event: {}", workerIdentifier, type);
      return true;
    }

    return false;
  }

  @Override
  void stop() {
    LOG.info("Worker {} stopping.", workerIdentifier);
    terminateBackGroundPolling();
    sinkWriter.close();
    super.stop();
    LOG.info("Worker {} stopped.", workerIdentifier);
  }

  private void terminateBackGroundPolling() {
    running.set(false);
    pollingExecutor.shutdownNow();
    try {
      if (!pollingExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
        LOG.warn(
            "Polling thread did not terminate in time on worker {}, forcing shutdown",
            workerIdentifier);
        throw new ConnectException(
            String.format(
                "Background polling thread of worker %s did not terminated gracefully.",
                workerIdentifier));
      }
    } catch (InterruptedException e) {
      LOG.warn(
          "Worker {} got interrupted while waiting for polling thread shutdown",
          workerIdentifier,
          e);
      throw new ConnectException(
          String.format(
              "Background polling thread of worker %s interrupted while closing.",
              workerIdentifier),
          e);
    }
    controlEventQueue.clear();
    errorRef.set(null);
  }

  void save(Collection<SinkRecord> sinkRecords) {
    sinkWriter.save(sinkRecords);
  }
}
