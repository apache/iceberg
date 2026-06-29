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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;

public class TestRecordUtils {

  @Test
  public void testCreateTableWriterMissingIdColumnThrowsDataException() {
    Schema icebergSchema =
        new Schema(
            ImmutableList.of(Types.NestedField.required(1, "id", Types.LongType.get())),
            ImmutableSet.of(1));

    Table table = mock(Table.class);
    when(table.schema()).thenReturn(icebergSchema);
    when(table.spec()).thenReturn(PartitionSpec.unpartitioned());
    when(table.io()).thenReturn(new InMemoryFileIO());
    when(table.locationProvider())
        .thenReturn(LocationProviders.locationsFor("file", ImmutableMap.of()));
    when(table.encryption()).thenReturn(PlaintextEncryptionManager.instance());
    when(table.properties()).thenReturn(ImmutableMap.of());

    TableSinkConfig tableConfig = mock(TableSinkConfig.class);
    when(tableConfig.idColumns()).thenReturn(ImmutableList.of("missing_column"));

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(tableConfig);
    when(config.writeProps()).thenReturn(ImmutableMap.of());

    TableReference tableReference =
        TableReference.of("test_catalog", TableIdentifier.of("name"), UUID.randomUUID());

    assertThatThrownBy(() -> RecordUtils.createTableWriter(table, tableReference, config))
        .isInstanceOf(DataException.class)
        .hasMessageContaining("ID column 'missing_column' not found in schema")
        .hasMessageContaining("Available columns:");
  }

  @Test
  public void testExtractFromRecordValueStruct() {
    org.apache.kafka.connect.data.Schema valSchema =
        SchemaBuilder.struct()
            .field("key", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
            .build();
    Struct val = new Struct(valSchema).put("key", 123L);
    Object result = RecordUtils.extractFromRecordValue(val, "key");
    assertThat(result).isEqualTo(123L);
  }

  @Test
  public void testExtractFromRecordValueStructNested() {
    org.apache.kafka.connect.data.Schema idSchema =
        SchemaBuilder.struct()
            .field("key", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
            .build();
    org.apache.kafka.connect.data.Schema dataSchema =
        SchemaBuilder.struct().field("id", idSchema).build();
    org.apache.kafka.connect.data.Schema valSchema =
        SchemaBuilder.struct().field("data", dataSchema).build();

    Struct id = new Struct(idSchema).put("key", 123L);
    Struct data = new Struct(dataSchema).put("id", id);
    Struct val = new Struct(valSchema).put("data", data);

    Object result = RecordUtils.extractFromRecordValue(val, "data.id.key");
    assertThat(result).isEqualTo(123L);
  }

  @Test
  public void testExtractFromRecordValueStructNull() {
    org.apache.kafka.connect.data.Schema valSchema =
        SchemaBuilder.struct()
            .field("key", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
            .build();
    Struct val = new Struct(valSchema).put("key", 123L);

    Object result = RecordUtils.extractFromRecordValue(val, "");
    assertThat(result).isNull();

    result = RecordUtils.extractFromRecordValue(val, "xkey");
    assertThat(result).isNull();
  }

  @Test
  public void testExtractFromRecordValueMap() {
    Map<String, Object> val = ImmutableMap.of("key", 123L);
    Object result = RecordUtils.extractFromRecordValue(val, "key");
    assertThat(result).isEqualTo(123L);
  }

  @Test
  public void testExtractFromRecordValueMapNested() {
    Map<String, Object> id = ImmutableMap.of("key", 123L);
    Map<String, Object> data = ImmutableMap.of("id", id);
    Map<String, Object> val = ImmutableMap.of("data", data);

    Object result = RecordUtils.extractFromRecordValue(val, "data.id.key");
    assertThat(result).isEqualTo(123L);
  }

  @Test
  public void testExtractFromRecordValueMapNull() {
    Map<String, Object> val = ImmutableMap.of("key", 123L);

    Object result = RecordUtils.extractFromRecordValue(val, "");
    assertThat(result).isNull();

    result = RecordUtils.extractFromRecordValue(val, "xkey");
    assertThat(result).isNull();
  }
}
