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
package org.apache.iceberg.actions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Tasks;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestCommitService extends TestBase {

  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1);
  }

  @TestTemplate
  public void testCommittedResultsCorrectly() {
    CustomCommitService commitService = new CustomCommitService(table, 5, 10000);
    commitService.start();

    ExecutorService executorService = Executors.newFixedThreadPool(10);
    int numberOfFileGroups = 100;
    Tasks.range(numberOfFileGroups).executeWith(executorService).run(commitService::offer);
    commitService.close();

    Set<Integer> expected = Sets.newHashSet(IntStream.range(0, 100).iterator());
    Set<Integer> actual = Sets.newHashSet(commitService.results());
    assertThat(actual).isEqualTo(expected);
  }

  @TestTemplate
  public void testAbortFileGroupsAfterTimeout() {
    CustomCommitService commitService = new CustomCommitService(table, 5, 200);
    commitService.start();

    // Add file groups [0-3] for commit.
    // There are less than the rewritesPerCommit, and thus will not trigger a commit action. Those
    // file groups will be added to the completedRewrites queue.
    // Now the queue has 4 file groups that need to commit.
    for (int i = 0; i < 4; i++) {
      commitService.offer(i);
    }

    // Add file groups [4-7] for commit
    // These are gated to not be able to commit, so all those 4 file groups will be added to the
    // queue as well.
    // Now the queue has 8 file groups that need to commit.
    CustomCommitService spyCommitService = spy(commitService);
    doReturn(false).when(spyCommitService).canCreateCommitGroup();
    for (int i = 4; i < 8; i++) {
      spyCommitService.offer(i);
    }

    // close commitService.
    // This allows committerService thread to start to commit the remaining file groups [0-7] in the
    // completedRewrites queue. And also the main thread waits for the committerService thread to
    // finish within a timeout.

    // The committerService thread commits file groups [0-4]. These will wait a fixed duration to
    // simulate timeout on the main thread, which then tries to abort file groups [5-7].
    // This tests the race conditions, as the committerService is also trying to commit groups
    // [5-7].
    assertThatThrownBy(commitService::close)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Timeout occurred when waiting for commits");

    // Wait for the commitService to finish. Committed all file groups or aborted remaining file
    // groups.
    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .pollInSameThread()
        .untilAsserted(() -> assertThat(commitService.completedRewritesAllCommitted()).isTrue());
    if (commitService.aborted.isEmpty()) {
      // All file groups are committed
      assertThat(commitService.results()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7);
    } else {
      // File groups [5-7] are aborted
      assertThat(commitService.results()).doesNotContainAnyElementsOf(commitService.aborted);
      assertThat(commitService.results()).containsExactly(0, 1, 2, 3, 4);
      assertThat(commitService.aborted).containsExactly(5, 6, 7);
    }
  }

  private static class CustomCommitService extends BaseCommitService<Integer> {
    private final Set<Integer> aborted = Sets.newConcurrentHashSet();

    CustomCommitService(Table table, int rewritesPerCommit, int timeoutInSeconds) {
      super(table, rewritesPerCommit, timeoutInSeconds);
    }

    @Override
    protected void commitOrClean(Set<Integer> batch) {
      try {
        // Slightly longer than timeout
        Thread.sleep(210);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected void abortFileGroup(Integer group) {
      aborted.add(group);
    }
  }
}
