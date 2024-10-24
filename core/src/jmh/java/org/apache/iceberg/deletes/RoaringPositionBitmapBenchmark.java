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
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.roaringbitmap.longlong.Roaring64Bitmap;

/**
 * A benchmark that evaluates the performance of {@link RoaringPositionBitmap}.
 *
 * <p>To run this benchmark: <code>
 *   ./gradlew :iceberg-core:jmh
 *       -PjmhIncludeRegex=RoaringPositionBitmapBenchmark
 *       -PjmhOutputPath=benchmark/roaring-position-bitmap-benchmark.txt
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
@Timeout(time = 5, timeUnit = TimeUnit.MINUTES)
public class RoaringPositionBitmapBenchmark {

  private static final int TOTAL_POSITIONS = 5_000_000;
  private static final long STEP = 5L;

  private long[] orderedPositions;
  private long[] shuffledPositions;

  @Setup
  public void setupBenchmark() {
    this.orderedPositions = generateOrderedPositions();
    this.shuffledPositions = generateShuffledPositions();
  }

  @Benchmark
  @Threads(1)
  public void addOrderedPositionsIcebergBitmap(Blackhole blackhole) {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();
    for (long position : orderedPositions) {
      bitmap.add(position);
    }
    blackhole.consume(bitmap);
  }

  @Benchmark
  @Threads(1)
  public void addOrderedPositionsLibraryBitmap(Blackhole blackhole) {
    Roaring64Bitmap bitmap = new Roaring64Bitmap();
    for (long position : orderedPositions) {
      bitmap.add(position);
    }
    blackhole.consume(bitmap);
  }

  @Benchmark
  @Threads(1)
  public void addShuffledPositionsIcebergBitmap(Blackhole blackhole) {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();
    for (long position : shuffledPositions) {
      bitmap.add(position);
    }
    blackhole.consume(bitmap);
  }

  @Benchmark
  @Threads(1)
  public void addShuffledPositionsLibraryBitmap(Blackhole blackhole) {
    Roaring64Bitmap bitmap = new Roaring64Bitmap();
    for (long position : shuffledPositions) {
      bitmap.add(position);
    }
    blackhole.consume(bitmap);
  }

  @Benchmark
  @Threads(1)
  public void addAndCheckPositionsIcebergBitmap(Blackhole blackhole) {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();

    for (long position : shuffledPositions) {
      bitmap.add(position);
    }

    for (long position = 0; position <= TOTAL_POSITIONS * STEP; position++) {
      bitmap.contains(position);
    }

    blackhole.consume(bitmap);
  }

  @Benchmark
  @Threads(1)
  public void addAndCheckPositionsLibraryBitmap(Blackhole blackhole) {
    Roaring64Bitmap bitmap = new Roaring64Bitmap();

    for (long position : shuffledPositions) {
      bitmap.add(position);
    }

    for (long position = 0; position <= TOTAL_POSITIONS * STEP; position++) {
      bitmap.contains(position);
    }

    blackhole.consume(bitmap);
  }

  private static long[] generateOrderedPositions() {
    long[] positions = new long[TOTAL_POSITIONS];

    for (int index = 0; index < TOTAL_POSITIONS; index++) {
      positions[index] = index * STEP;
    }

    return positions;
  }

  private static long[] generateShuffledPositions() {
    long[] positions = generateOrderedPositions();
    shuffle(positions);
    return positions;
  }

  private static void shuffle(long[] array) {
    Random rand = new Random();

    for (int i = array.length - 1; i > 0; i--) {
      // generate a random index between 0 and i
      int j = rand.nextInt(i + 1);

      // swap array[i] with array[j]
      long temp = array[i];
      array[i] = array[j];
      array[j] = temp;
    }
  }
}
