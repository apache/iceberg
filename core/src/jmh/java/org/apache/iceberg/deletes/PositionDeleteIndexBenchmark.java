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
package org.apache.iceberg.deletes;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * A benchmark that compares probing {@link PositionDeleteIndex} once per position against
 * traversing a range once, which is how vectorized readers consume the index.
 *
 * <p>Readers process positions in contiguous ascending batches, so every probe in a batch resolves
 * the same underlying bitmap and container. The density determines which Roaring container is
 * chosen: up to 4096 deleted positions per 65536-position chunk are stored in a sorted array that
 * is binary searched, beyond that in a fixed bitmap.
 *
 * <p>To run this benchmark: <code>
 *   ./gradlew :iceberg-core:jmh
 *       -PjmhIncludeRegex=PositionDeleteIndexBenchmark
 *       -PjmhOutputPath=benchmark/position-delete-index-benchmark.txt
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Timeout(time = 5, timeUnit = TimeUnit.MINUTES)
public class PositionDeleteIndexBenchmark {

  private static final long TOTAL_POSITIONS = 5_000_000L;
  private static final int BATCH_SIZE = 5000;
  private static final long SEED = 1234L;

  /** Deleted positions per 65536-position chunk, as a percentage. */
  @Param({"0.5", "6.1", "12.0"})
  private double density;

  private PositionDeleteIndex index;

  @Setup
  public void setupBenchmark() {
    Random random = new Random(SEED);
    PositionDeleteIndex bitmapIndex = new BitmapPositionDeleteIndex();
    for (long pos = 0; pos < TOTAL_POSITIONS; pos++) {
      if (random.nextDouble() * 100 < density) {
        bitmapIndex.delete(pos);
      }
    }

    this.index = bitmapIndex;
  }

  @Benchmark
  @Threads(1)
  public void probePerPosition(Blackhole blackhole) {
    long deletedCount = 0;

    for (long batchStart = 0; batchStart < TOTAL_POSITIONS; batchStart += BATCH_SIZE) {
      for (int rowId = 0; rowId < BATCH_SIZE; rowId++) {
        if (index.isDeleted(batchStart + rowId)) {
          deletedCount++;
        }
      }
    }

    blackhole.consume(deletedCount);
  }

  @Benchmark
  @Threads(1)
  public void traverseRange(Blackhole blackhole) {
    PositionCounter counter = new PositionCounter();

    for (long batchStart = 0; batchStart < TOTAL_POSITIONS; batchStart += BATCH_SIZE) {
      index.forEachInRange(batchStart, batchStart + BATCH_SIZE, counter);
    }

    blackhole.consume(counter.count());
  }

  private static class PositionCounter implements LongConsumer {
    private long count = 0;

    @Override
    public void accept(long pos) {
      this.count++;
    }

    long count() {
      return count;
    }
  }
}
