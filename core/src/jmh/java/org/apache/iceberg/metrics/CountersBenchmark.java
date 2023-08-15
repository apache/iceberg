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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.infra.Blackhole;

@Fork(1)
@State(Scope.Benchmark)
@Measurement(iterations = 25)
@BenchmarkMode(Mode.SingleShotTime)
@Timeout(time = 10, timeUnit = TimeUnit.MINUTES)
public class CountersBenchmark {

  private static final int NUM_OPERATIONS = 10_000_000;
  private static final int WORKER_POOL_SIZE = 16;
  private static final int INCREMENT_AMOUNT = 10_000;

  @Benchmark
  @Threads(1)
  public void defaultCounterMultipleThreads(Blackhole blackhole) {
    Counter counter = new DefaultCounter(Unit.BYTES);

    ExecutorService workerPool = ThreadPools.newWorkerPool("bench-pool", WORKER_POOL_SIZE);

    try {
      Tasks.range(WORKER_POOL_SIZE)
          .executeWith(workerPool)
          .run(
              (id) -> {
                for (int operation = 0; operation < NUM_OPERATIONS; operation++) {
                  counter.increment(INCREMENT_AMOUNT);
                }
              });
    } finally {
      workerPool.shutdown();
    }

    blackhole.consume(counter);
  }

  @Benchmark
  @Threads(1)
  public void defaultCounterSingleThread(Blackhole blackhole) {
    Counter counter = new DefaultCounter(Unit.BYTES);

    for (int operation = 0; operation < WORKER_POOL_SIZE * NUM_OPERATIONS; operation++) {
      counter.increment(INCREMENT_AMOUNT);
    }

    blackhole.consume(counter);
  }
}
