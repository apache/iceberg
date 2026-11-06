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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

public class TestIcebergWriter extends WriterTestBase {

  private static final TableReference TABLE_REFERENCE =
      TableReference.of("test_catalog", TableIdentifier.of("name"), UUID.randomUUID());

  @Test
  public void testConversionErrorIsDataException() {
    IcebergWriter writer = new IcebergWriter(table, TABLE_REFERENCE, config(false));

    SinkRecord record = record(ImmutableMap.of("id", "abc", "data", "x", "id2", 1L));
    assertThatThrownBy(() -> writer.write(record))
        .isInstanceOf(DataException.class)
        .hasMessage("An error occurred converting record, topic: topic, partition, 1, offset: 100")
        .hasRootCauseInstanceOf(NumberFormatException.class);
  }

  @Test
  public void testSchemaUpdateErrorIsNotDataException() {
    // a failed schema update is a catalog error: the record must not be reported and skipped
    doThrow(new IllegalStateException("catalog unavailable")).when(table).refresh();
    IcebergWriter writer = new IcebergWriter(table, TABLE_REFERENCE, config(true));

    SinkRecord record =
        record(ImmutableMap.of("id", 1L, "data", "x", "id2", 1L, "new_col", "value"));
    assertThatThrownBy(() -> writer.write(record))
        .isInstanceOf(ConnectException.class)
        .isNotInstanceOf(DataException.class)
        .hasMessage("An error occurred writing record, topic: topic, partition, 1, offset: 100")
        .hasRootCauseMessage("catalog unavailable");
  }

  private static IcebergSinkConfig config(boolean evolveSchema) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", "parquet"));
    when(config.evolveSchemaEnabled()).thenReturn(evolveSchema);
    return config;
  }

  private static SinkRecord record(Object value) {
    return new SinkRecord(
        "topic", 1, null, "key", null, value, 100L, 0L, TimestampType.LOG_APPEND_TIME);
  }
}
