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
package org.apache.iceberg.metrics;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.withinPercentage;

import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

public class TestFixedReservoirHistogram {
  @Test
  public void emptyHistogram() {
    FixedReservoirHistogram histogram = new FixedReservoirHistogram(100);
    assertThat(histogram.count()).isEqualTo(0);
    Histogram.Statistics statistics = histogram.statistics();
    assertThat(statistics.size()).isEqualTo(0);
    assertThat(statistics.mean()).isEqualTo(0.0);
    assertThat(statistics.stdDev()).isEqualTo(0.0);
    assertThat(statistics.max()).isEqualTo(0L);
    assertThat(statistics.min()).isEqualTo(0L);
    assertThat(statistics.percentile(0.50)).isEqualTo(0L);
    assertThat(statistics.percentile(0.99)).isEqualTo(0L);
  }

  @Test
  public void singleObservation() {
    FixedReservoirHistogram histogram = new FixedReservoirHistogram(100);
    histogram.update(123L);
    assertThat(histogram.count()).isEqualTo(1);
    Histogram.Statistics statistics = histogram.statistics();
    assertThat(statistics.size()).isEqualTo(1);
    assertThat(statistics.mean()).isEqualTo(123.0);
    assertThat(statistics.stdDev()).isEqualTo(0.0);
    assertThat(statistics.max()).isEqualTo(123L);
    assertThat(statistics.min()).isEqualTo(123L);
    assertThat(statistics.percentile(0.50)).isEqualTo(123L);
    assertThat(statistics.percentile(0.99)).isEqualTo(123L);
  }

  @Test
  public void minMaxPercentilePoints() {
    int reservoirSize = 100;
    FixedReservoirHistogram histogram = new FixedReservoirHistogram(reservoirSize);
    for (int i = 0; i < reservoirSize; ++i) {
      histogram.update(i);
    }

    Histogram.Statistics statistics = histogram.statistics();
    assertThat(statistics.percentile(0.0)).isEqualTo(0L);
    assertThat(statistics.percentile(1.0)).isEqualTo(99L);
  }

  @Test
  public void invalidPercentilePoints() {
    int reservoirSize = 100;
    FixedReservoirHistogram histogram = new FixedReservoirHistogram(reservoirSize);
    for (int i = 0; i < reservoirSize; ++i) {
      histogram.update(i);
    }

    Histogram.Statistics statistics = histogram.statistics();

    assertThatThrownBy(() -> statistics.percentile(-0.1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Percentile point cannot be outside the range of [0.0 - 1.0]: " + -0.1);

    assertThatThrownBy(() -> statistics.percentile(1.1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Percentile point cannot be outside the range of [0.0 - 1.0]: " + 1.1);
  }

  @Test
  public void testMultipleThreadWriters() throws InterruptedException {
    int threads = 10;
    int samplesPerThread = 100;
    int totalSamples = threads * samplesPerThread;

    FixedReservoirHistogram histogram = new FixedReservoirHistogram(totalSamples);
    CyclicBarrier barrier = new CyclicBarrier(threads);
    ExecutorService executor = newFixedThreadPool(threads);

    List<Future<Integer>> futures =
        IntStream.range(0, threads)
            .mapToObj(
                threadIndex ->
                    executor.submit(
                        () -> {
                          try {
                            barrier.await(30, SECONDS);
                            for (int i = 1; i <= 100; ++i) {
                              histogram.update((long) threadIndex * samplesPerThread + i);
                            }
                            return threadIndex;
                          } catch (Exception e) {
                            throw new RuntimeException(e);
                          }
                        }))
            .collect(Collectors.toList());

    futures.stream()
        .map(
            f -> {
              try {
                return f.get(30, SECONDS);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            })
        .collect(Collectors.toList());

    executor.shutdownNow();
    executor.awaitTermination(5, SECONDS);
    Histogram.Statistics statistics = histogram.statistics();

    assertThat(histogram.count()).isEqualTo(totalSamples);
    assertThat(statistics.size()).isEqualTo(totalSamples);
    assertThat(statistics.mean()).isEqualTo(500.5);
    assertThat(statistics.stdDev()).isCloseTo(288.67499, withinPercentage(0.001));
    assertThat(statistics.max()).isEqualTo(1000L);
    assertThat(statistics.min()).isEqualTo(1L);
    assertThat(statistics.percentile(0.50)).isEqualTo(500);
    assertThat(statistics.percentile(0.75)).isEqualTo(750);
    assertThat(statistics.percentile(0.90)).isEqualTo(900);
    assertThat(statistics.percentile(0.95)).isEqualTo(950);
    assertThat(statistics.percentile(0.99)).isEqualTo(990);
    assertThat(statistics.percentile(0.999)).isEqualTo(999);
  }
}
