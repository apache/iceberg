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
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;

public class ParallelIterable<T> extends CloseableGroup implements CloseableIterable<T> {
  private final Iterable<? extends Iterable<T>> iterables;
  private final ExecutorService workerPool;

  public ParallelIterable(Iterable<? extends Iterable<T>> iterables, ExecutorService workerPool) {
    this.iterables = iterables;
    this.workerPool = workerPool;
  }

  @Override
  public CloseableIterator<T> iterator() {
    ParallelIterator<T> iter = new ParallelIterator<>(iterables, workerPool);
    addCloseable(iter);
    return iter;
  }

  private static class ParallelIterator<T> implements CloseableIterator<T> {
    private final Iterator<Runnable> tasks;
    private final ExecutorService workerPool;
    private final Future<?>[] taskFutures;
    private final ConcurrentLinkedQueue<T> queue = new ConcurrentLinkedQueue<>();
    private volatile boolean closed = false;

    private ParallelIterator(
        Iterable<? extends Iterable<T>> iterables, ExecutorService workerPool) {
      this.tasks =
          Iterables.transform(
                  iterables,
                  iterable ->
                      (Runnable)
                          () -> {
                            try (Closeable ignored =
                                (iterable instanceof Closeable) ? (Closeable) iterable : () -> {}) {
                              for (T item : iterable) {
                                queue.add(item);
                              }
                            } catch (IOException e) {
                              throw new RuntimeIOException(e, "Failed to close iterable");
                            }
                          })
              .iterator();
      this.workerPool = workerPool;
      // submit 2 tasks per worker at a time
      this.taskFutures = new Future[2 * ThreadPools.WORKER_THREAD_POOL_SIZE];
    }

    @Override
    public void close() {
      // close first, avoid new task submit
      this.closed = true;

      // cancel background tasks
      for (Future<?> taskFuture : taskFutures) {
        if (taskFuture != null && !taskFuture.isDone()) {
          taskFuture.cancel(true);
        }
      }
      // clean queue
      this.queue.clear();
    }

    /**
     * Checks on running tasks and submits new tasks if needed.
     *
     * <p>This should not be called after {@link #close()}.
     *
     * @return true if there are pending tasks, false otherwise
     */
    private boolean checkTasks() {
      boolean hasRunningTask = false;

      for (int i = 0; i < taskFutures.length; i += 1) {
        if (taskFutures[i] == null || taskFutures[i].isDone()) {
          if (taskFutures[i] != null) {
            // check for task failure and re-throw any exception
            try {
              taskFutures[i].get();
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

      return !closed && (tasks.hasNext() || hasRunningTask);
    }

    private Future<?> submitNextTask() {
      if (!closed && tasks.hasNext()) {
        return workerPool.submit(tasks.next());
      }
      return null;
    }

    @Override
    public synchronized boolean hasNext() {
      Preconditions.checkState(!closed, "Already closed");

      // if the consumer is processing records more slowly than the producers, then this check will
      // prevent tasks from being submitted. while the producers are running, this will always
      // return here before running checkTasks. when enough of the tasks are finished that the
      // consumer catches up, then lots of new tasks will be submitted at once. this behavior is
      // okay because it ensures that records are not stacking up waiting to be consumed and taking
      // up memory.
      //
      // consumers that process results quickly will periodically exhaust the queue and submit new
      // tasks when checkTasks runs. fast consumers should not be delayed.
      if (!queue.isEmpty()) {
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
}
