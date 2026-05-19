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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.util.BackoffStrategies;
import org.apache.iceberg.util.BackoffStrategy;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestBackoffStrategyCommit extends TestBase {

  @TestTemplate
  public void commitUsesConfiguredBackoffStrategy() {
    CountingBackoffStrategy.reset();

    table
        .updateProperties()
        .set(BackoffStrategies.STRATEGY_IMPL, CountingBackoffStrategy.class.getName())
        .commit();

    TestTables.TestTableOperations ops = table.ops();
    ops.failCommits(2);

    table.newFastAppend().appendFile(FILE_B).commit();

    assertThat(CountingBackoffStrategy.initialized).isTrue();
    // computeBackoff is invoked once before each of the two retried commit attempts
    assertThat(CountingBackoffStrategy.COMPUTE_CALLS.get()).isEqualTo(2);
  }

  @TestTemplate
  public void commitWithoutPropertyDoesNotUseCustomStrategy() {
    CountingBackoffStrategy.reset();

    // keep the retry waits tiny so the default exponential backoff does not slow the test
    table
        .updateProperties()
        .set(TableProperties.COMMIT_MIN_RETRY_WAIT_MS, "0")
        .set(TableProperties.COMMIT_MAX_RETRY_WAIT_MS, "0")
        .commit();

    TestTables.TestTableOperations ops = table.ops();
    ops.failCommits(2);

    table.newFastAppend().appendFile(FILE_B).commit();

    assertThat(CountingBackoffStrategy.COMPUTE_CALLS.get()).isZero();
  }

  /** Records how many times it is consulted; loaded reflectively via a no-arg constructor. */
  public static class CountingBackoffStrategy implements BackoffStrategy {
    static final AtomicInteger COMPUTE_CALLS = new AtomicInteger(0);
    static volatile boolean initialized = false;

    static void reset() {
      COMPUTE_CALLS.set(0);
      initialized = false;
    }

    @Override
    public void initialize(Map<String, String> properties) {
      initialized = true;
    }

    @Override
    public long computeBackoff(int attempt) {
      COMPUTE_CALLS.incrementAndGet();
      return 0L;
    }
  }
}
