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
package org.apache.iceberg.spark.source;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AsyncTaskOpener<T, TaskT extends ScanTask> implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncTaskOpener.class);

  private final BlockingQueue<CloseableIterator<T>> queue;
  private final CloseableIterator<T> ALL_TASKS_COMPLETE = CloseableIterator.empty();
  private final ExecutorService executor;

  AsyncTaskOpener(
      List<TaskT> tasks,
      Function<TaskT, CloseableIterator<T>> open,
      int parallelism,
      int queueSize) {
    this.queue = new LinkedBlockingQueue<>(queueSize);
    this.executor =
        Executors.newFixedThreadPool(
            parallelism,
            runnable -> {
              Thread thread = new Thread(runnable);
              thread.setDaemon(true);
              thread.setName("iceberg-spark-readers-async-open-" + thread.getId());
              return thread;
            });
    startOpening(tasks, open);
  }

  private void startOpening(List<TaskT> tasks, Function<TaskT, CloseableIterator<T>> open) {
    Thread coordinatorThread =
        new Thread(
            () -> {
              try {
                Tasks.foreach(tasks)
                    .executeWith(executor)
                    .suppressFailureWhenFinished()
                    .onFailure(
                        (task, exception) -> {
                          LOG.error("Failed to open task asynchronously {} ", task, exception);
                        })
                    .run(
                        task -> {
                          CloseableIterator<T> iterator = open.apply(task);
                          try {
                            this.queue.put(iterator);
                          } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(
                                "Interrupted while adding opened task iterator to queue", e);
                          }
                        });
                this.queue.put(ALL_TASKS_COMPLETE);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                try {
                  this.queue.put(ALL_TASKS_COMPLETE);
                } catch (InterruptedException ie) {
                  this.queue.offer(ALL_TASKS_COMPLETE);
                }
                throw new RuntimeException("Interrupted while opening tasks in parallel", e);
              } finally {
                executor.shutdown();
              }
            },
            "iceberg-spark-readers-async-open-coordinator-thread");
    coordinatorThread.setDaemon(true);
    coordinatorThread.start();
  }

  CloseableIterator<T> getNext() throws InterruptedException {
    CloseableIterator<T> next = this.queue.take();
    if (next == ALL_TASKS_COMPLETE) {
      return null;
    }
    return next;
  }

  @Override
  public void close() throws IOException {
    this.executor.shutdownNow();
    CloseableIterator<T> iterator;
    while ((iterator = this.queue.poll()) != null) {
      if (iterator != ALL_TASKS_COMPLETE) {
        try {
          iterator.close();
        } catch (Exception e) {
          LOG.error("Error closing task iterator", e);
        }
      }
    }
  }
}
