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
package org.apache.iceberg.connect.channel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public class TestCoordinatorThread {

  @Test
  public void testRun() {
    Coordinator coordinator = mock(Coordinator.class);
    CoordinatorThread coordinatorThread = new CoordinatorThread(coordinator);

    coordinatorThread.start();

    verify(coordinator, timeout(1000)).start();
    verify(coordinator, timeout(1000).atLeast(1)).process();
    verify(coordinator, times(0)).stop();
    assertThat(coordinatorThread.isTerminated()).isFalse();

    coordinatorThread.terminate();

    verify(coordinator, timeout(1000)).stop();
    assertThat(coordinatorThread.isTerminated()).isTrue();
  }

  @Test
  public void testTerminateWaitsForCoordinatorStop() throws Exception {
    Coordinator coordinator = mock(Coordinator.class);
    CountDownLatch stopStarted = new CountDownLatch(1);
    CountDownLatch stopCanFinish = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              stopStarted.countDown();
              assertThat(stopCanFinish.await(5, TimeUnit.SECONDS)).isTrue();
              return null;
            })
        .when(coordinator)
        .stop();
    CoordinatorThread coordinatorThread = new CoordinatorThread(coordinator);
    coordinatorThread.start();

    verify(coordinator, timeout(1000)).start();
    verify(coordinator, timeout(1000).atLeast(1)).process();

    Thread terminator = new Thread(coordinatorThread::terminate);
    terminator.start();

    assertThat(stopStarted.await(1, TimeUnit.SECONDS)).isTrue();
    assertThat(terminator.isAlive()).isTrue();

    stopCanFinish.countDown();
    terminator.join(1000);

    assertThat(terminator.isAlive()).isFalse();
    assertThat(coordinatorThread.isAlive()).isFalse();
  }
}
