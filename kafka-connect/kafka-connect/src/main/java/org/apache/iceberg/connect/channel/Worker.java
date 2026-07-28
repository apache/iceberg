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
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
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
import org.apache.iceberg.connect.events.Payload;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads the control topic on a dedicated thread and answers START_COMMIT requests from the Connect
 * task thread.
 *
 * <p>Threading contract:
 *
 * <ul>
 *   <li>the polling thread owns the {@link org.apache.kafka.clients.consumer.Consumer} for its
 *       entire lifecycle (subscribe, poll, close), since KafkaConsumer is not thread safe
 *   <li>the Connect task thread owns the {@link SinkWriter} and the producer, and is the only
 *       thread that writes data files or sends events
 *   <li>{@link #pendingCommits} and {@link #errorRef} are the only shared state between them
 * </ul>
 *
 * <p>Failure model: any failure on the polling thread is terminal for this worker. It is recorded
 * in {@link #errorRef} and rethrown from every subsequent {@link #process()} call, so the worker
 * can never keep buffering records once it is unable to answer a commit request. The worker is not
 * restartable in place; see {@link CommitterImpl} for why.
 */
class Worker extends Channel {
  private static final Logger LOG = LoggerFactory.getLogger(Worker.class);

  /**
   * How long {@link #start()} waits for the polling thread to subscribe and establish its consumer
   * position. Returning before that would leave a window where {@code process()} sees an empty
   * queue while a START_COMMIT is already on the broker, which is the race this class exists to
   * close.
   */
  private static final Duration SUBSCRIBE_TIMEOUT = Duration.ofSeconds(60);

  /** How long each phase of {@link #stop()} waits for the polling thread to exit. */
  private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);

  private final IcebergSinkConfig config;
  private final SinkTaskContext context;
  private final SinkWriter sinkWriter;

  // Buffered by the polling thread, drained by the Connect task thread. Under normal operation the
  // task thread drains the queue on every put() call, so the depth is bounded by the commit rate.
  private final ConcurrentLinkedQueue<StartCommit> pendingCommits = new ConcurrentLinkedQueue<>();
  private final ExecutorService pollingExecutor;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicBoolean stopping = new AtomicBoolean(false);
  private final AtomicReference<Exception> errorRef = new AtomicReference<>(null);
  private final CountDownLatch subscribed = new CountDownLatch(1);
  private final CountDownLatch pollingStopped = new CountDownLatch(1);
  private final Duration pollInterval;
  private final String taskId;

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
    this.pollInterval = Duration.ofMillis(config.controlPollIntervalMs());
    this.pollingExecutor =
        Executors.newSingleThreadExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "iceberg-worker-control-poller-" + taskId);
              thread.setDaemon(true);
              return thread;
            });
  }

  @Override
  void start() {
    // Do NOT call super.start() — subscribe and every subsequent poll must happen on the polling
    // thread to satisfy KafkaConsumer's single-thread requirement. The polling thread calls
    // initializeConsumer() as its first action and start() blocks until that completes.
    running.set(true);

    try {
      pollingExecutor.execute(this::backgroundPoll);
    } catch (Exception e) {
      running.set(false);
      throw new ConnectException(
          String.format("Worker %s failed to start the control topic polling thread", taskId), e);
    }

    awaitSubscription();

    LOG.info(
        "Worker {} started with async control event processing (poll interval: {}ms)",
        taskId,
        pollInterval.toMillis());
  }

  /**
   * Blocks until the polling thread has subscribed to the control topic and completed its initial
   * poll. The worker's consumer group is transient and resets to the latest offset, so any
   * START_COMMIT appended before the position is established is invisible to this worker;
   * establishing it synchronously in {@code start()} keeps that window as small as it was before
   * polling moved off the task thread.
   */
  private void awaitSubscription() {
    boolean initialized;
    try {
      initialized = subscribed.await(SUBSCRIBE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConnectException(
          String.format("Worker %s was interrupted while subscribing to the control topic", taskId),
          e);
    }

    if (!initialized) {
      throw new ConnectException(
          String.format(
              Locale.ROOT,
              "Worker %s timed out after %ds waiting to subscribe to the control topic",
              taskId,
              SUBSCRIBE_TIMEOUT.toSeconds()));
    }

    Exception failure = errorRef.get();
    if (failure != null) {
      throw new ConnectException(
          String.format("Worker %s failed to subscribe to the control topic", taskId), failure);
    }
  }

  /**
   * Subscribes to the control topic and then continuously polls it, buffering commit requests for
   * the Connect task thread. All consumer access, including {@code close()}, is confined to this
   * thread.
   */
  private void backgroundPoll() {
    LOG.info("Control topic polling thread started for worker {}", taskId);
    try {
      if (!initialize()) {
        return;
      }

      while (running.get() && !Thread.currentThread().isInterrupted()) {
        consumeAvailable(pollInterval);
      }
    } catch (Exception e) {
      handlePollingException(e);
    } finally {
      running.set(false);
      // The consumer belongs to this thread, so it must be closed here rather than from stop().
      closeConsumer();
      pollingStopped.countDown();
      LOG.info("Control topic polling thread stopped for worker {}", taskId);
    }
  }

  /**
   * Subscribes to the control topic and establishes the consumer position, then unblocks {@link
   * #start()}.
   *
   * @return true if the consumer is ready to be polled
   */
  private boolean initialize() {
    try {
      initializeConsumer();
      return true;
    } catch (Exception e) {
      handlePollingException(e);
      return false;
    } finally {
      // Counted down after the failure is recorded, so that start() -- which unblocks on this
      // latch -- reports the failure instead of returning as if subscription had succeeded.
      subscribed.countDown();
    }
  }

  private void handlePollingException(Exception error) {
    if (stopping.get()
        && (error instanceof WakeupException || error instanceof InterruptException)) {
      // Expected: stop() woke up or interrupted this thread. The "polling thread stopped" line is
      // the only trace needed, a stack trace here would just be shutdown noise.
      return;
    }
    recordFailure(error);
  }

  private void recordFailure(Exception error) {
    // Keep the first failure: it is the one that explains why polling stopped. It is never
    // cleared, so it remains available as a diagnostic and process() keeps failing on it.
    if (errorRef.compareAndSet(null, error)) {
      LOG.error("Worker {} failed while polling control events", taskId, error);
    } else {
      LOG.error("Worker {} hit a further failure while polling control events", taskId, error);
    }
  }

  /**
   * Handles every commit request buffered by the polling thread. Called from the Connect task
   * thread on the {@code put()} / {@code flush()} path.
   *
   * @throws ConnectException if control topic polling is no longer running. The failure is sticky:
   *     this worker holds data that has been written but not committed along with the source
   *     offsets for it, so it must stop accepting records rather than look healthy while being
   *     unable to answer a commit request.
   */
  void process() {
    Exception failure = errorRef.get();
    if (failure != null) {
      throw new ConnectException(
          String.format("Worker %s failed while polling the control topic", taskId), failure);
    }

    if (pollingStopped.getCount() == 0 && !stopping.get()) {
      throw new ConnectException(
          String.format(
              "Control topic polling thread for worker %s is no longer running, "
                  + "commit requests can no longer be answered",
              taskId));
    }

    StartCommit startCommit;
    while ((startCommit = pendingCommits.poll()) != null) {
      handleStartCommit(startCommit.commitId());
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
    Payload payload = envelope.event().payload();
    // narrow the type here, where it is checked, so pendingCommits cannot receive anything else
    if (payload instanceof StartCommit startCommit) {
      pendingCommits.offer(startCommit);
      LOG.debug("Worker {} buffered START_COMMIT event", taskId);
      return true;
    }
    return false;
  }

  @Override
  void stop() {
    if (!stopping.compareAndSet(false, true)) {
      LOG.debug("Worker {} is already stopped", taskId);
      return;
    }

    LOG.info("Worker {} stopping.", taskId);
    boolean interrupted = false;
    try {
      interrupted = terminateBackgroundPolling();
    } finally {
      // Cleanup must run even if the polling thread misbehaved, and stop() must not throw back
      // into the Connect framework. Otherwise the transactional producer leaks, possibly with an
      // open transaction that blocks the next task start, along with the buffered writer handles.
      try {
        sinkWriter.close();
      } catch (Exception e) {
        LOG.warn("Error closing sink writer of worker {}, ignoring...", taskId, e);
      }
      super.stop();
      LOG.info("Worker {} stopped.", taskId);
      if (interrupted) {
        // restored only now, so that it could not abort the cleanup above
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Stops the polling thread. Never throws, so that the rest of {@link #stop()} always runs.
   *
   * @return true if the calling thread was interrupted while waiting, so the caller can restore the
   *     interrupt flag once cleanup is done
   */
  private boolean terminateBackgroundPolling() {
    running.set(false);
    try {
      // wakeup() is the only thread-safe consumer method; it breaks out of a blocking poll()
      wakeupConsumer();
    } catch (Exception e) {
      LOG.warn("Error waking up the consumer of worker {}, ignoring...", taskId, e);
    }

    // shutdown() rather than shutdownNow(): interrupting the polling thread makes KafkaConsumer
    // throw InterruptException out of both poll() and close(), which would leak the consumer.
    pollingExecutor.shutdown();

    boolean interrupted = false;
    try {
      if (!awaitPollingThread()) {
        LOG.error(
            "Control topic polling thread of worker {} did not stop within {}s, interrupting it",
            taskId,
            SHUTDOWN_TIMEOUT.toSeconds());
        pollingExecutor.shutdownNow();
        if (!awaitPollingThread()) {
          LOG.error(
              "Control topic polling thread of worker {} is still running, "
                  + "its consumer cannot be closed safely and is left open",
              taskId);
        }
      }
    } catch (InterruptedException e) {
      LOG.warn(
          "Worker {} was interrupted while stopping the control topic polling thread", taskId, e);
      pollingExecutor.shutdownNow();
      interrupted = true;
    }

    int dropped = pendingCommits.size();
    pendingCommits.clear();
    if (dropped > 0) {
      LOG.warn(
          "Worker {} discarded {} unanswered commit request(s) while stopping, "
              + "the coordinator will time these commits out",
          taskId,
          dropped);
    }

    return interrupted;
  }

  private boolean awaitPollingThread() throws InterruptedException {
    return pollingExecutor.awaitTermination(SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
  }

  void save(Collection<SinkRecord> sinkRecords) {
    sinkWriter.save(sinkRecords);
  }

  @VisibleForTesting
  int pendingEventCount() {
    return pendingCommits.size();
  }

  @VisibleForTesting
  boolean isPolling() {
    return pollingStopped.getCount() > 0;
  }
}
