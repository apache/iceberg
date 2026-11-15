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
import org.junit.jupiter.api.Test;

class TestWriteTimer {

  @Test
  void recordsCloseTimeWhenCloseThrows() {
    WriteTimer timer = new WriteTimer(0, 0L);

    assertThatThrownBy(
            () ->
                timer.timeClose(
                    () -> {
                      busyWait();
                      throw new IOException("close failed");
                    }))
        .isInstanceOf(IOException.class)
        .hasMessage("close failed");

    assertThat(timer.closeNanos()).isGreaterThan(0L);
    assertThat(timer.storageWriteNanos()).isEqualTo(0L);
  }

  private static void busyWait() {
    long start = System.nanoTime();
    while (System.nanoTime() - start < 1_000L) {
      // spin so the timer observes a non-zero duration
    }
  }
}
