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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import org.apache.iceberg.spark.WriteDurations;
import org.junit.jupiter.api.Test;

public class TestWriteTimer {

  @Test
  public void testAccumulatesWriteAndCloseSeparately() {
    WriteTimer timer = new WriteTimer();

    timer.timeWrite(() -> busyWait());
    long afterFirstWrite = timer.writeNanos();
    assertThat(afterFirstWrite).isGreaterThan(0);
    assertThat(timer.closeNanos()).isEqualTo(0);

    timer.timeWrite(() -> busyWait());
    assertThat(timer.writeNanos()).isGreaterThan(afterFirstWrite);
    assertThat(timer.closeNanos()).isEqualTo(0);

    timer.timeClose(() -> busyWait());
    assertThat(timer.closeNanos()).isGreaterThan(0);
  }

  @Test
  public void testRecordsTimeWhenOperationThrows() {
    WriteTimer timer = new WriteTimer();

    assertThatThrownBy(
            () ->
                timer.timeWrite(
                    () -> {
                      busyWait();
                      throw new IOException("write failed");
                    }))
        .isInstanceOf(IOException.class)
        .hasMessage("write failed");

    // a task that fails mid-write still reports what it wrote
    assertThat(timer.writeNanos()).isGreaterThan(0);
  }

  @Test
  public void testWriteDurations() {
    WriteTimer timer = new WriteTimer();
    timer.timeWrite(() -> busyWait());
    timer.timeClose(() -> busyWait());

    WriteDurations durations = timer.writeDurations();
    assertThat(durations.writeNanos()).isEqualTo(timer.writeNanos());
    assertThat(durations.closeNanos()).isEqualTo(timer.closeNanos());
  }

  @Test
  public void testCombine() {
    WriteDurations combined =
        WriteDurations.of(10L, 20L)
            .combine(WriteDurations.of(3L, 4L))
            .combine(WriteDurations.EMPTY);

    assertThat(combined.writeNanos()).isEqualTo(13L);
    assertThat(combined.closeNanos()).isEqualTo(24L);
  }

  private static void busyWait() {
    long start = System.nanoTime();
    while (System.nanoTime() - start < 1_000L) {
      // spin so the timer observes a non-zero duration
    }
  }
}
