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
package org.apache.iceberg.flink.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestOutputFileFactoryProvider {
  private static final long TARGET_FILE_SIZE = 128 * 1024 * 1024;

  @TempDir protected java.nio.file.Path temporaryFolder;

  private Table table;

  @BeforeEach
  public void before() throws IOException {
    File folder = Files.createTempDirectory(temporaryFolder, "junit").toFile();
    table = SimpleDataUtil.createTable(folder.getAbsolutePath(), ImmutableMap.of(), false);
  }

  @Test
  public void testCustomProviderIsUsed() throws IOException {
    AtomicBoolean providerCalled = new AtomicBoolean(false);

    OutputFileFactoryProvider provider =
        (tbl, taskId, attemptId, format, spec) -> {
          providerCalled.set(true);
          return OutputFileFactory.builderFor(tbl, taskId, attemptId)
              .format(format)
              .defaultSpec(spec)
              .build();
        };

    TaskWriterFactory<RowData> factory = createTaskWriterFactory(provider);
    factory.initialize(0, 0);

    assertThat(providerCalled.get()).isTrue();

    try (TaskWriter<RowData> writer = factory.create()) {
      writer.write(SimpleDataUtil.createRowData(1, "a"));
      DataFile[] dataFiles = writer.dataFiles();
      assertThat(dataFiles).hasSize(1);
    }
  }

  @Test
  public void testDefaultBehaviorWhenProviderIsNull() throws IOException {
    TaskWriterFactory<RowData> factory = createTaskWriterFactory(null);
    factory.initialize(0, 0);

    try (TaskWriter<RowData> writer = factory.create()) {
      writer.write(SimpleDataUtil.createRowData(1, "a"));
      DataFile[] dataFiles = writer.dataFiles();
      assertThat(dataFiles).hasSize(1);
    }
  }

  @Test
  public void testProviderReceivesCorrectArguments() {
    int expectedTaskId = 42;
    int expectedAttemptId = 7;

    AtomicInteger capturedTaskId = new AtomicInteger(-1);
    AtomicInteger capturedAttemptId = new AtomicInteger(-1);
    AtomicReference<FileFormat> capturedFormat = new AtomicReference<>();
    AtomicReference<PartitionSpec> capturedSpec = new AtomicReference<>();

    OutputFileFactoryProvider provider =
        (tbl, taskId, attemptId, format, spec) -> {
          capturedTaskId.set(taskId);
          capturedAttemptId.set(attemptId);
          capturedFormat.set(format);
          capturedSpec.set(spec);
          return OutputFileFactory.builderFor(tbl, taskId, attemptId)
              .format(format)
              .defaultSpec(spec)
              .build();
        };

    TaskWriterFactory<RowData> factory = createTaskWriterFactory(provider);
    factory.initialize(expectedTaskId, expectedAttemptId);

    assertThat(capturedTaskId.get()).isEqualTo(expectedTaskId);
    assertThat(capturedAttemptId.get()).isEqualTo(expectedAttemptId);
    assertThat(capturedFormat.get()).isEqualTo(FileFormat.PARQUET);
    assertThat(capturedSpec.get()).isEqualTo(table.spec());
  }

  @Test
  public void testProviderReturningNullThrows() {
    OutputFileFactoryProvider provider = (tbl, taskId, attemptId, format, spec) -> null;

    TaskWriterFactory<RowData> factory = createTaskWriterFactory(provider);

    assertThatThrownBy(() -> factory.initialize(0, 0))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("OutputFileFactoryProvider must not return null");
  }

  private TaskWriterFactory<RowData> createTaskWriterFactory(OutputFileFactoryProvider provider) {
    return new RowDataTaskWriterFactory(
        (org.apache.iceberg.util.SerializableSupplier<Table>) () -> table,
        SimpleDataUtil.ROW_TYPE,
        TARGET_FILE_SIZE,
        FileFormat.PARQUET,
        Collections.emptyMap(),
        null,
        false,
        table.schema(),
        table.spec(),
        provider);
  }
}
