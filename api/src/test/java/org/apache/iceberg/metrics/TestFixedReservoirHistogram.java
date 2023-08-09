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
import static org.assertj.core.api.Assertions.withinPercentage;

import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFixedReservoirHistogram {
  @Test
  public void emptyHistogram() {
    FixedReservoirHistogram histogram = new FixedReservoirHistogram(100);
    Assertions.assertThat(histogram.count()).isEqualTo(0);
    Histogram.Statistics statistics = histogram.statistics();
    Assertions.assertThat(statistics.size()).isEqualTo(0);
    Assertions.assertThat(statistics.mean()).isEqualTo(0.0);
    Assertions.assertThat(statistics.stdDev()).isEqualTo(0.0);
    Assertions.assertThat(statistics.max()).isEqualTo(0L);
    Assertions.assertThat(statistics.min()).isEqualTo(0L);
    Assertions.assertThat(statistics.percentile(0.50)).isEqualTo(0L);
    Assertions.assertThat(statistics.percentile(0.99)).isEqualTo(0L);
  }

  @Test
  public void singleObservation() {
    FixedReservoirHistogram histogram = new FixedReservoirHistogram(100);
    histogram.update(123L);
    Assertions.assertThat(histogram.count()).isEqualTo(1);
    Histogram.Statistics statistics = histogram.statistics();
    Assertions.assertThat(statistics.size()).isEqualTo(1);
    Assertions.assertThat(statistics.mean()).isEqualTo(123.0);
    Assertions.assertThat(statistics.stdDev()).isEqualTo(0.0);
    Assertions.assertThat(statistics.max()).isEqualTo(123L);
    Assertions.assertThat(statistics.min()).isEqualTo(123L);
    Assertions.assertThat(statistics.percentile(0.50)).isEqualTo(123L);
    Assertions.assertThat(statistics.percentile(0.99)).isEqualTo(123L);
  }

  @Test
  public void minMaxPercentilePoints() {
    int reservoirSize = 100;
    FixedReservoirHistogram histogram = new FixedReservoirHistogram(reservoirSize);
    for (int i = 0; i < reservoirSize; ++i) {
      histogram.update(i);
    }

    Histogram.Statistics statistics = histogram.statistics();
    Assertions.assertThat(statistics.percentile(0.0)).isEqualTo(0L);
    Assertions.assertThat(statistics.percentile(1.0)).isEqualTo(99L);
  }

  @Test
  public void invalidPercentilePoints() {
    int reservoirSize = 100;
    FixedReservoirHistogram histogram = new FixedReservoirHistogram(reservoirSize);
    for (int i = 0; i < reservoirSize; ++i) {
      histogram.update(i);
    }

    Histogram.Statistics statistics = histogram.statistics();

    Assertions.assertThatThrownBy(() -> statistics.percentile(-0.1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Percentile point cannot be outside the range of [0.0 - 1.0]: " + -0.1);

    Assertions.assertThatThrownBy(() -> statistics.percentile(1.1))
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
                              histogram.update(threadIndex * samplesPerThread + i);
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

    Assertions.assertThat(histogram.count()).isEqualTo(totalSamples);
    Assertions.assertThat(statistics.size()).isEqualTo(totalSamples);
    Assertions.assertThat(statistics.mean()).isEqualTo(500.5);
    Assertions.assertThat(statistics.stdDev()).isCloseTo(288.67499, withinPercentage(0.001));
    Assertions.assertThat(statistics.max()).isEqualTo(1000L);
    Assertions.assertThat(statistics.min()).isEqualTo(1L);
    Assertions.assertThat(statistics.percentile(0.50)).isEqualTo(500);
    Assertions.assertThat(statistics.percentile(0.75)).isEqualTo(750);
    Assertions.assertThat(statistics.percentile(0.90)).isEqualTo(900);
    Assertions.assertThat(statistics.percentile(0.95)).isEqualTo(950);
    Assertions.assertThat(statistics.percentile(0.99)).isEqualTo(990);
    Assertions.assertThat(statistics.percentile(0.999)).isEqualTo(999);
  }
}
