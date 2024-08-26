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
package org.apache.iceberg.flink.maintenance.operator;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarnessBuilder;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

class TestAsyncDeleteFiles extends OperatorTestBase {
  private static final String DUMMY_FILE_NAME = "dummy";

  @Test
  void testDelete() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    Table table = tableLoader.loadTable();

    // Write an extra files
    Path dummyFile =
        FileSystems.getDefault().getPath(table.location().substring(5), DUMMY_FILE_NAME);
    Files.write(dummyFile, "DUMMY".getBytes(StandardCharsets.UTF_8));

    List<Boolean> actual = deleteDummyFileAndWait(tableLoader);

    assertThat(actual).isEqualTo(ImmutableList.of(Boolean.TRUE));
    assertThat(Files.exists(dummyFile)).isFalse();
  }

  @Test
  void testDeleteMissingFile() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);

    List<Boolean> actual = deleteDummyFileAndWait(sql.tableLoader(TABLE_NAME));

    assertThat(actual).isEqualTo(ImmutableList.of(Boolean.TRUE));
  }

  @Test
  void testWrongFile() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);

    StreamTaskMailboxTestHarnessBuilder<Boolean> builder =
        new StreamTaskMailboxTestHarnessBuilder<>(
                OneInputStreamTask::new, BasicTypeInfo.BOOLEAN_TYPE_INFO)
            .addInput(BasicTypeInfo.STRING_TYPE_INFO);
    CountingPredicateChecker predicate = new CountingPredicateChecker();

    try (StreamTaskMailboxTestHarness<Boolean> testHarness =
        builder
            .setupOutputForSingletonOperatorChain(
                asyncWaitOperatorFactory(sql.tableLoader(TABLE_NAME), predicate))
            .build()) {
      testHarness.processElement(new StreamRecord<>("wrong://", System.currentTimeMillis()));

      while (testHarness.getOutput().isEmpty()) {
        Awaitility.await().until(() -> testHarness.getOutput().isEmpty());
        testHarness.processAll();
      }

      // Make sure that we do not complete immediately
      assertThat(CountingPredicateChecker.calls).isEqualTo(3);

      // The result still should be fail
      assertThat(
              testHarness.getOutput().stream()
                  .map(r -> ((StreamRecord<Boolean>) r).getValue())
                  .collect(Collectors.toList()))
          .isEqualTo(ImmutableList.of(Boolean.FALSE));
    }
  }

  private List<Boolean> deleteDummyFileAndWait(TableLoader tableLoader) throws Exception {
    Table table = tableLoader.loadTable();

    try (OneInputStreamOperatorTestHarness<String, Boolean> testHarness =
        new OneInputStreamOperatorTestHarness<>(
            asyncWaitOperatorFactory(tableLoader, new CountingPredicateChecker()),
            StringSerializer.INSTANCE)) {
      testHarness.open();
      testHarness.processElement(
          table.location() + "/" + DUMMY_FILE_NAME, System.currentTimeMillis());

      // wait until all async collectors in the buffer have been emitted out.
      testHarness.endInput();
      testHarness.close();

      return testHarness.extractOutputValues();
    }
  }

  private AsyncWaitOperatorFactory<String, Boolean> asyncWaitOperatorFactory(
      TableLoader tableLoader, Predicate<Collection<Boolean>> predicate) {
    return new AsyncWaitOperatorFactory<>(
        new AsyncDeleteFiles(DUMMY_NAME, tableLoader, 10),
        1000000,
        10,
        AsyncDataStream.OutputMode.ORDERED,
        new AsyncRetryStrategies.ExponentialBackoffDelayRetryStrategyBuilder<Boolean>(
                2, 10, 1000, 1.5)
            .ifResult(predicate)
            .build());
  }

  private static class CountingPredicateChecker
      implements Predicate<Collection<Boolean>>, Serializable {
    private static int calls = 0;

    @Override
    public boolean test(Collection<Boolean> param) {
      ++calls;
      return AsyncDeleteFiles.FAILED_PREDICATE.test(param);
    }
  }
}
