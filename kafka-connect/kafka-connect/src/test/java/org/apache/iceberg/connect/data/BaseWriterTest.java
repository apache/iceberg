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
package org.apache.iceberg.connect.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.IcebergDecoder;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;

public class BaseWriterTest {

  protected InMemoryFileIO fileIO;
  protected Table table;

  protected static final Schema SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "data", Types.StringType.get()),
              Types.NestedField.required(3, "id2", Types.LongType.get())),
          ImmutableSet.of(1, 3));

  protected static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("data").build();

  @BeforeEach
  public void before() {
    fileIO = new InMemoryFileIO();

    table = mock(Table.class);
    when(table.schema()).thenReturn(SCHEMA);
    when(table.spec()).thenReturn(PartitionSpec.unpartitioned());
    when(table.io()).thenReturn(fileIO);
    when(table.locationProvider())
        .thenReturn(LocationProviders.locationsFor("file", ImmutableMap.of()));
    when(table.encryption()).thenReturn(PlaintextEncryptionManager.instance());
    when(table.properties()).thenReturn(ImmutableMap.of());
  }

  protected WriteResult writeTest(
      List<Record> rows, IcebergSinkConfig config, Class<?> expectedWriterClass) {
    try (TaskWriter<Record> writer = RecordUtils.createTableWriter(table, "name", config, "topic=test_topic/partition=test_partition/offset=test_offset_0")) {
      assertThat(writer.getClass()).isEqualTo(expectedWriterClass);

      rows.forEach(
          row -> {
            try {
              writer.write(row);
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          });

      return writer.complete();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
