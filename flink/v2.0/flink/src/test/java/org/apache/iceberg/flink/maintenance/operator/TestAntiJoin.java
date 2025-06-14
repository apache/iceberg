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

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.iceberg.actions.FileURI;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.flink.maintenance.api.DeleteOrphanFiles;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

public class TestAntiJoin extends OperatorTestBase {
  private static Map<String, String> equalSchemes =
      Maps.newHashMap(
          ImmutableMap.of(
              "s3n", "s3",
              "s3a", "s3"));
  private static Map<String, String> equalAuthorities = Maps.newHashMap();
  private static final FileURI SCHEME_FILE_1 =
      new FileURI("s3:/fileName1", equalSchemes, equalAuthorities);
  private static final FileURI SCHEME_FILE_2 =
      new FileURI("s3:/fileName2", equalSchemes, equalAuthorities);
  private static final FileURI AUTHORITY_FILE_1 =
      new FileURI("s3://HDFS1002060/fileName1", equalSchemes, equalAuthorities);
  private static final FileURI ONE_AUTHORITY_SCHEME_FILE_1 =
      new FileURI("s3a://HDFS1002060/fileName1", equalSchemes, equalAuthorities);
  private static final FileURI TWO_AUTHORITY_SCHEME_FILE_1 =
      new FileURI("s3b://HDFS1002060/fileName1", equalSchemes, equalAuthorities);

  @Test
  void testFileSystemFirst() throws Exception {
    try (KeyedTwoInputStreamOperatorTestHarness<String, FileURI, FileURI, String> testHarness =
        testHarness()) {
      testHarness.open();

      testHarness.processElement2(SCHEME_FILE_1, EVENT_TIME);
      testHarness.processElement2(SCHEME_FILE_1, EVENT_TIME);
      testHarness.processWatermark1(WATERMARK);
      testHarness.processElement1(SCHEME_FILE_1, EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();
      testHarness.processWatermark2(WATERMARK);
      assertThat(testHarness.extractOutputValues()).isEmpty();
      assertThat(testHarness.getSideOutput(DeleteOrphanFiles.ERROR_STREAM)).isNull();
    }
  }

  @Test
  void testTableFirst() throws Exception {
    try (KeyedTwoInputStreamOperatorTestHarness<String, FileURI, FileURI, String> testHarness =
        testHarness()) {
      testHarness.open();

      testHarness.processElement1(SCHEME_FILE_1, EVENT_TIME);
      testHarness.processElement2(SCHEME_FILE_1, EVENT_TIME);
      testHarness.processWatermark1(WATERMARK);
      testHarness.processElement2(SCHEME_FILE_1, EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();
      testHarness.processWatermark2(WATERMARK);
      assertThat(testHarness.extractOutputValues()).isEmpty();
      assertThat(testHarness.getSideOutput(DeleteOrphanFiles.ERROR_STREAM)).isNull();
    }
  }

  @Test
  void testOnlyFileSystem() throws Exception {
    try (KeyedTwoInputStreamOperatorTestHarness<String, FileURI, FileURI, String> testHarness =
        testHarness()) {
      testHarness.open();

      testHarness.processElement2(SCHEME_FILE_1, EVENT_TIME);
      testHarness.processElement2(SCHEME_FILE_1, EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();
      testHarness.processBothWatermarks(WATERMARK);
      assertThat(testHarness.extractOutputValues())
          .isEqualTo(ImmutableList.of(SCHEME_FILE_1.getUriAsString()));
      assertThat(testHarness.getSideOutput(DeleteOrphanFiles.ERROR_STREAM)).isNull();
    }
  }

  @Test
  void testOnlyTable() throws Exception {
    try (KeyedTwoInputStreamOperatorTestHarness<String, FileURI, FileURI, String> testHarness =
        testHarness()) {
      testHarness.open();

      testHarness.processElement1(SCHEME_FILE_1, EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();
      testHarness.processBothWatermarks(WATERMARK);
      assertThat(testHarness.extractOutputValues()).isEmpty();
      assertThat(testHarness.getSideOutput(DeleteOrphanFiles.ERROR_STREAM)).isNull();
    }
  }

  @Test
  void testFileSystemWithAuthority() throws Exception {
    try (KeyedTwoInputStreamOperatorTestHarness<String, FileURI, FileURI, String> testHarness =
        testHarness()) {
      testHarness.open();

      testHarness.processElement1(SCHEME_FILE_1, EVENT_TIME);
      testHarness.processElement2(AUTHORITY_FILE_1, EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();
      testHarness.processBothWatermarks(WATERMARK);
      assertThat(testHarness.extractOutputValues()).isEmpty();
      assertThat(testHarness.getSideOutput(DeleteOrphanFiles.ERROR_STREAM)).isNull();
    }
  }

  @Test
  void testTableWithAuthority() throws Exception {
    try (KeyedTwoInputStreamOperatorTestHarness<String, FileURI, FileURI, String> testHarness =
        testHarness()) {
      testHarness.open();

      testHarness.processElement1(AUTHORITY_FILE_1, EVENT_TIME);
      testHarness.processElement2(SCHEME_FILE_1, EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();
      testHarness.processBothWatermarks(WATERMARK);
      ConcurrentLinkedQueue<StreamRecord<Exception>> errorList =
          testHarness.getSideOutput(DeleteOrphanFiles.ERROR_STREAM);
      assertThat(errorList).hasSize(1);
      assertThat(errorList.stream().findFirst().get().getValue())
          .isInstanceOf(ValidationException.class);

      assertThat(testHarness.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void testDiffScheme() throws Exception {
    try (KeyedTwoInputStreamOperatorTestHarness<String, FileURI, FileURI, String> testHarness =
        testHarness()) {
      testHarness.open();

      testHarness.processElement1(AUTHORITY_FILE_1, EVENT_TIME);
      testHarness.processElement2(ONE_AUTHORITY_SCHEME_FILE_1, EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();
      testHarness.processBothWatermarks(WATERMARK);
      assertThat(testHarness.extractOutputValues()).isEmpty();
      assertThat(testHarness.getSideOutput(DeleteOrphanFiles.ERROR_STREAM)).isNull();
    }
  }

  @Test
  void testUnRegisterScheme() throws Exception {
    try (KeyedTwoInputStreamOperatorTestHarness<String, FileURI, FileURI, String> testHarness =
        testHarness()) {
      testHarness.open();

      testHarness.processElement1(AUTHORITY_FILE_1, EVENT_TIME);
      testHarness.processElement2(TWO_AUTHORITY_SCHEME_FILE_1, EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();
      testHarness.processBothWatermarks(WATERMARK);
      ConcurrentLinkedQueue<StreamRecord<Exception>> errorList =
          testHarness.getSideOutput(DeleteOrphanFiles.ERROR_STREAM);
      assertThat(errorList).hasSize(1);
      assertThat(errorList.stream().findFirst().get().getValue())
          .isInstanceOf(ValidationException.class);

      assertThat(testHarness.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void testPrefixMismatchModeDelete() throws Exception {
    try (KeyedTwoInputStreamOperatorTestHarness<String, FileURI, FileURI, String> testHarness =
        testHarness(org.apache.iceberg.actions.DeleteOrphanFiles.PrefixMismatchMode.DELETE)) {
      testHarness.open();

      testHarness.processElement1(AUTHORITY_FILE_1, EVENT_TIME);
      testHarness.processElement2(SCHEME_FILE_1, EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();
      testHarness.processBothWatermarks(WATERMARK);
      assertThat(testHarness.extractOutputValues())
          .isEqualTo(ImmutableList.of(SCHEME_FILE_1.getUriAsString()));
      assertThat(testHarness.getSideOutput(DeleteOrphanFiles.ERROR_STREAM)).isNull();
    }
  }

  @Test
  void testPrefixMismatchModeIgnore() throws Exception {
    try (KeyedTwoInputStreamOperatorTestHarness<String, FileURI, FileURI, String> testHarness =
        testHarness(org.apache.iceberg.actions.DeleteOrphanFiles.PrefixMismatchMode.IGNORE)) {
      testHarness.open();

      testHarness.processElement1(AUTHORITY_FILE_1, EVENT_TIME);
      testHarness.processElement2(SCHEME_FILE_1, EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();
      testHarness.processBothWatermarks(WATERMARK);
      assertThat(testHarness.extractOutputValues()).isEmpty();
      assertThat(testHarness.getSideOutput(DeleteOrphanFiles.ERROR_STREAM)).isNull();
    }
  }

  private static KeyedTwoInputStreamOperatorTestHarness<String, FileURI, FileURI, String>
      testHarness(
          org.apache.iceberg.actions.DeleteOrphanFiles.PrefixMismatchMode prefixMismatchMode)
          throws Exception {
    return ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
        new AntiJoin(prefixMismatchMode),
        (KeySelector<FileURI, String>) t -> t.getPath(),
        (KeySelector<FileURI, String>) t -> t.getPath(),
        BasicTypeInfo.STRING_TYPE_INFO);
  }

  private static KeyedTwoInputStreamOperatorTestHarness<String, FileURI, FileURI, String>
      testHarness() throws Exception {
    return testHarness(org.apache.iceberg.actions.DeleteOrphanFiles.PrefixMismatchMode.ERROR);
  }
}
