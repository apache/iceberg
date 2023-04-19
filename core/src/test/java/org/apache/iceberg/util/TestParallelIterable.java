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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.awaitility.Awaitility;
import org.junit.Test;

public class TestParallelIterable {
  @Test
  public void closeParallelIteratorWithoutCompleteIteration()
      throws IOException, IllegalAccessException, NoSuchFieldException {
    ExecutorService executor = Executors.newFixedThreadPool(1);

    Iterable<CloseableIterable<Integer>> transform =
        Iterables.transform(
            Lists.newArrayList(1, 2, 3, 4, 5),
            item ->
                new CloseableIterable<Integer>() {
                  @Override
                  public void close() {}

                  @Override
                  public CloseableIterator<Integer> iterator() {
                    return CloseableIterator.withClose(Collections.singletonList(item).iterator());
                  }
                });

    ParallelIterable<Integer> parallelIterable = new ParallelIterable<>(transform, executor);
    CloseableIterator<Integer> iterator = parallelIterable.iterator();
    Field queueField = iterator.getClass().getDeclaredField("queue");
    queueField.setAccessible(true);
    ConcurrentLinkedQueue<?> queue = (ConcurrentLinkedQueue<?>) queueField.get(iterator);

    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isNotNull();
    Awaitility.await("Queue is populated")
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(() -> queueHasElements(iterator, queue));
    iterator.close();
    Awaitility.await("Queue is cleared")
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(queue).isEmpty());
  }

  private void queueHasElements(CloseableIterator<Integer> iterator, Queue queue) {
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isNotNull();
    assertThat(queue).isNotEmpty();
  }
}
