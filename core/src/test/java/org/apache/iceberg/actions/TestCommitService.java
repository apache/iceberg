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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Tasks;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.Test;

public class TestCommitService extends TableTestBase {

  public TestCommitService() {
    super(1);
  }

  @Test
  public void testCommittedResultsCorrectly() {
    CustomCommitService commitService = new CustomCommitService(table, 5, 10000);
    commitService.start();

    ExecutorService executorService = Executors.newFixedThreadPool(10);
    int numberOfFileGroups = 100;
    Tasks.range(numberOfFileGroups).executeWith(executorService).run(commitService::offer);
    commitService.close();

    Set<Integer> expected = Sets.newHashSet(IntStream.range(0, 100).iterator());
    Set<Integer> actual = Sets.newHashSet(commitService.results());
    Assertions.assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testAbortFileGroupsAfterTimeout() {
    CustomCommitService commitService = new CustomCommitService(table, 5, 200);
    commitService.start();

    // Add the number of less than rewritesPerCommit
    for (int i = 0; i < 4; i++) {
      commitService.offer(i);
    }

    // Simulate the last group of rewrite
    CustomCommitService spyCommitService = spy(commitService);
    doReturn(false).when(spyCommitService).canCreateCommitGroup();
    for (int i = 4; i < 8; i++) {
      spyCommitService.offer(i);
    }

    Assertions.assertThatThrownBy(commitService::close)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Timeout occurred when waiting for commits");

    // Wait for the committerService finish commit the remaining file groups
    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .pollInSameThread()
        .untilAsserted(() -> assertThat(commitService.completedRewritesAllCommitted()).isTrue());
    Assertions.assertThat(commitService.results())
        .doesNotContainAnyElementsOf(commitService.aborted);
    Assertions.assertThat(commitService.results()).isEqualTo(ImmutableList.of(0, 1, 2, 3, 4));
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
