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
package org.apache.iceberg.util;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallelIterable<T> extends CloseableGroup implements CloseableIterable<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ParallelIterable.class);

  private static final int DEFAULT_MAX_QUEUE_SIZE = 30_000;

  private final Iterable<? extends Iterable<T>> iterables;
  private final ExecutorService workerPool;

  // Bound for number of items in the queue to limit memory consumption
  // even in the case when input iterables are large.
  private final int approximateMaxQueueSize;

  public ParallelIterable(Iterable<? extends Iterable<T>> iterables, ExecutorService workerPool) {
    this(iterables, workerPool, DEFAULT_MAX_QUEUE_SIZE);
  }

  public ParallelIterable(
      Iterable<? extends Iterable<T>> iterables,
      ExecutorService workerPool,
      int approximateMaxQueueSize) {
    this.iterables = Preconditions.checkNotNull(iterables, "Input iterables cannot be null");
    this.workerPool = Preconditions.checkNotNull(workerPool, "Worker pool cannot be null");
    this.approximateMaxQueueSize = approximateMaxQueueSize;
  }

  @Override
  public CloseableIterator<T> iterator() {
    ParallelIterator<T> iter =
        new ParallelIterator<>(iterables, workerPool, approximateMaxQueueSize);
    addCloseable(iter);
    return iter;
  }

  private static class ParallelIterator<T> implements CloseableIterator<T> {
    private final Iterator<Task<T>> tasks;
    private final Deque<Task<T>> yieldedTasks = new ArrayDeque<>();
    private final ExecutorService workerPool;
    private final CompletableFuture<Optional<Task<T>>>[] taskFutures;
    private final ConcurrentLinkedQueue<T> queue = new ConcurrentLinkedQueue<>();
    private final int maxQueueSize;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private ParallelIterator(
        Iterable<? extends Iterable<T>> iterables, ExecutorService workerPool, int maxQueueSize) {
      this.tasks =
          Iterables.transform(
                  iterables, iterable -> new Task<>(iterable, queue, closed, maxQueueSize))
              .iterator();
      this.workerPool = workerPool;
      this.maxQueueSize = maxQueueSize;
      // submit 2 tasks per worker at a time
      this.taskFutures = new CompletableFuture[2 * ThreadPools.WORKER_THREAD_POOL_SIZE];
    }

    @Override
    public void close() {
      // close first, avoid new task submit
      this.closed.set(true);

      try (Closer closer = Closer.create()) {
        synchronized (this) {
          yieldedTasks.forEach(closer::register);
          yieldedTasks.clear();
        }

        // cancel background tasks and close continuations if any
        for (CompletableFuture<Optional<Task<T>>> taskFuture : taskFutures) {
          if (taskFuture != null) {
            taskFuture.cancel(true);
            taskFuture.thenAccept(
                continuation -> {
                  if (continuation.isPresent()) {
                    try {
                      continuation.get().close();
                    } catch (IOException e) {
                      LOG.error("Task close failed", e);
                    }
                  }
                });
          }
        }

        // clean queue
        this.queue.clear();
      } catch (IOException e) {
        throw new UncheckedIOException("Close failed", e);
      }
    }

    /**
     * Checks on running tasks and submits new tasks if needed.
     *
     * <p>This should not be called after {@link #close()}.
     *
     * @return true if there are pending tasks, false otherwise
     */
    private synchronized boolean checkTasks() {
      Preconditions.checkState(!closed.get(), "Already closed");
      boolean hasRunningTask = false;

      for (int i = 0; i < taskFutures.length; i += 1) {
        if (taskFutures[i] == null || taskFutures[i].isDone()) {
          if (taskFutures[i] != null) {
            // check for task failure and re-throw any exception. Enqueue continuation if any.
            try {
              Optional<Task<T>> continuation = taskFutures[i].get();
              continuation.ifPresent(yieldedTasks::addLast);
            } catch (ExecutionException e) {
              if (e.getCause() instanceof RuntimeException) {
                // rethrow a runtime exception
                throw (RuntimeException) e.getCause();
              } else {
                throw new RuntimeException("Failed while running parallel task", e.getCause());
              }
            } catch (InterruptedException e) {
              throw new RuntimeException("Interrupted while running parallel task", e);
            }
          }

          taskFutures[i] = submitNextTask();
        }

        if (taskFutures[i] != null) {
          hasRunningTask = true;
        }
      }

      return !closed.get() && (tasks.hasNext() || hasRunningTask);
    }

    private CompletableFuture<Optional<Task<T>>> submitNextTask() {
      if (!closed.get()) {
        if (!yieldedTasks.isEmpty()) {
          return CompletableFuture.supplyAsync(yieldedTasks.removeFirst(), workerPool);
        } else if (tasks.hasNext()) {
          return CompletableFuture.supplyAsync(tasks.next(), workerPool);
        }
      }
      return null;
    }

    @Override
    public synchronized boolean hasNext() {
      Preconditions.checkState(!closed.get(), "Already closed");

      // If the consumer is processing records more slowly than the producers, the producers will
      // eventually fill the queue and yield, returning continuations. Continuations and new tasks
      // are started by checkTasks(). The check here prevents us from restarting continuations or
      // starting new tasks too early (when queue is almost full) or too late (when queue is already
      // emptied). Restarting too early would lead to tasks yielding very quickly (CPU waste on
      // scheduling). Restarting too late would mean the consumer may need to wait for the tasks
      // to produce new items. A consumer slower than producers shouldn't need to wait.
      int queueLowWaterMark = maxQueueSize / 2;
      if (queue.size() > queueLowWaterMark) {
        return true;
      }

      // this cannot conclude that there are no more records until tasks have finished. while some
      // are running, return true when there is at least one item to return.
      while (checkTasks()) {
        if (!queue.isEmpty()) {
          return true;
        }

        try {
          Thread.sleep(10);

        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }

      // when tasks are no longer running, return whether the queue has items
      return !queue.isEmpty();
    }

    @Override
    public synchronized T next() {
      // use hasNext to block until there is an available record
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return queue.poll();
    }
  }

  private static class Task<T> implements Supplier<Optional<Task<T>>>, Closeable {
    private final Iterable<T> input;
    private final ConcurrentLinkedQueue<T> queue;
    private final AtomicBoolean closed;
    private final int approximateMaxQueueSize;

    private Iterator<T> iterator = null;

    Task(
        Iterable<T> input,
        ConcurrentLinkedQueue<T> queue,
        AtomicBoolean closed,
        int approximateMaxQueueSize) {
      this.input = Preconditions.checkNotNull(input, "input cannot be null");
      this.queue = Preconditions.checkNotNull(queue, "queue cannot be null");
      this.closed = Preconditions.checkNotNull(closed, "closed cannot be null");
      this.approximateMaxQueueSize = approximateMaxQueueSize;
    }

    @Override
    public Optional<Task<T>> get() {
      try {
        if (iterator == null) {
          iterator = input.iterator();
        }

        while (iterator.hasNext()) {
          if (queue.size() >= approximateMaxQueueSize) {
            // Yield when queue is over the size limit. Task will be resubmitted later and continue the work.
            return Optional.of(this);
          }

          T next = iterator.next();
          if (closed.get()) {
            break;
          }

          queue.add(next);
        }
      } catch (Throwable e) {
        try {
          close();
        } catch (IOException closeException) {
          // self-suppression is not permitted
          // (e and closeException to be the same is unlikely, but possible)
          if (closeException != e) {
            e.addSuppressed(closeException);
          }
        }

        throw e;
      }

      try {
        close();
      } catch (IOException e) {
        throw new UncheckedIOException("Close failed", e);
      }

      // The task is complete. Returning empty means there is no continuation that should be
      // executed.
      return Optional.empty();
    }

    @Override
    public void close() throws IOException {
      iterator = null;
      if (input instanceof Closeable) {
        ((Closeable) input).close();
      }
    }
  }
}
