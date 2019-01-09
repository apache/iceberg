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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ParallelIterable<T> implements Iterable<T> {
  private final Iterable<Iterable<T>> iterables;
  private final ExecutorService trackingPool;
  private final ExecutorService workerPool;

  public ParallelIterable(Iterable<Iterable<T>> iterables,
                          ExecutorService trackingPool,
                          ExecutorService workerPool) {
    this.iterables = iterables;
    this.trackingPool = trackingPool;
    this.workerPool = workerPool;
  }

  @Override
  public Iterator<T> iterator() {
    return new ParallelIterator<>(iterables, trackingPool, workerPool);
  }

  private static class ParallelIterator<T> implements Iterator<T> {
    private final ConcurrentLinkedQueue<T> queue = new ConcurrentLinkedQueue<>();
    private final Future<?> taskFuture;

    public ParallelIterator(Iterable<Iterable<T>> iterables,
                            ExecutorService trackingPool,
                            ExecutorService workerPool) {
      this.taskFuture = trackingPool.submit(() -> {
        Tasks.foreach(iterables)
            .noRetry().stopOnFailure().throwFailureWhenFinished()
            .executeWith(workerPool)
            .run(iterable -> {
              for (T item : iterable) {
                queue.add(item);
              }
            });
        return true;
      });
    }

    @Override
    public synchronized boolean hasNext() {
      // this cannot conclude that there are no more records until tasks have finished. while some
      // are running, return true when there is at least one item to return.
      while (!taskFuture.isDone()) {
        if (!queue.isEmpty()) {
          return true;
        }

        try {
          taskFuture.get(10, TimeUnit.MILLISECONDS);
          break;

        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        } catch (ExecutionException e) {
          ExceptionUtil.castAndThrow(e.getCause(), RuntimeException.class);
        } catch (TimeoutException e) {
          // continue looping to check the queue size and wait again
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
