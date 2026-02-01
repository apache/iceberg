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
import static org.mockito.Mockito.withSettings;

import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

public class TestRecordUtils {

  @Test
  public void testExtractFromRecordValueStruct() {
    Schema valSchema = SchemaBuilder.struct().field("key", Schema.INT64_SCHEMA).build();
    Struct val = new Struct(valSchema).put("key", 123L);
    Object result = RecordUtils.extractFromRecordValue(val, "key");
    assertThat(result).isEqualTo(123L);
  }

  @Test
  public void testExtractFromRecordValueStructNested() {
    Schema idSchema = SchemaBuilder.struct().field("key", Schema.INT64_SCHEMA).build();
    Schema dataSchema = SchemaBuilder.struct().field("id", idSchema).build();
    Schema valSchema = SchemaBuilder.struct().field("data", dataSchema).build();

    Struct id = new Struct(idSchema).put("key", 123L);
    Struct data = new Struct(dataSchema).put("id", id);
    Struct val = new Struct(valSchema).put("data", data);

    Object result = RecordUtils.extractFromRecordValue(val, "data.id.key");
    assertThat(result).isEqualTo(123L);
  }

  @Test
  public void testExtractFromRecordValueStructNull() {
    Schema valSchema = SchemaBuilder.struct().field("key", Schema.INT64_SCHEMA).build();
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

  private static final org.apache.iceberg.Schema CDC_SCHEMA =
      new org.apache.iceberg.Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "data", Types.StringType.get()),
              Types.NestedField.required(3, "id2", Types.LongType.get()),
              Types.NestedField.required(4, "_op", Types.StringType.get())),
          ImmutableSet.of(1, 3));

  @Test
  public void testCreateTableWriterFormatVersion1ThrowsException() {
    Table table = createMockTable(1);
    IcebergSinkConfig config = createCdcConfig();
    TableReference tableReference =
        TableReference.of("test_catalog", TableIdentifier.of("test_table"), UUID.randomUUID());

    assertThatThrownBy(() -> RecordUtils.createTableWriter(table, tableReference, config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "CDC and upsert modes are not supported for Iceberg table format version 1");
  }

  @Test
  public void testCreateTableWriterFormatVersion2CreatesEqualityDeleteWriter() {
    Table table = createMockTable(2);
    IcebergSinkConfig config = createCdcConfig();
    TableReference tableReference =
        TableReference.of("test_catalog", TableIdentifier.of("test_table"), UUID.randomUUID());

    try (TaskWriter<Record> writer = RecordUtils.createTableWriter(table, tableReference, config)) {
      // Format version 2 should create UnpartitionedDeltaWriter with useDv=false
      assertThat(writer).isInstanceOf(UnpartitionedDeltaWriter.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testCreateTableWriterFormatVersion3CreatesDeleteVectorWriter() {
    Table table = createMockTable(3);
    IcebergSinkConfig config = createCdcConfig();
    TableReference tableReference =
        TableReference.of("test_catalog", TableIdentifier.of("test_table"), UUID.randomUUID());

    try (TaskWriter<Record> writer = RecordUtils.createTableWriter(table, tableReference, config)) {
      // Format version 3 should create UnpartitionedDeltaWriter with useDv=true
      assertThat(writer).isInstanceOf(UnpartitionedDeltaWriter.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Table createMockTable(int formatVersion) {
    InMemoryFileIO fileIO = new InMemoryFileIO();
    Table table = mock(Table.class, withSettings().extraInterfaces(HasTableOperations.class));
    when(table.schema()).thenReturn(CDC_SCHEMA);
    when(table.spec()).thenReturn(PartitionSpec.unpartitioned());
    when(table.io()).thenReturn(fileIO);
    when(table.locationProvider())
        .thenReturn(LocationProviders.locationsFor("file", ImmutableMap.of()));
    when(table.encryption()).thenReturn(PlaintextEncryptionManager.instance());
    when(table.properties()).thenReturn(ImmutableMap.of());

    TableOperations ops = mock(TableOperations.class);
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.formatVersion()).thenReturn(formatVersion);
    when(ops.current()).thenReturn(metadata);
    when(((HasTableOperations) table).operations()).thenReturn(ops);

    return table;
  }

  private IcebergSinkConfig createCdcConfig() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of());
    when(config.isUpsertMode()).thenReturn(true);
    when(config.tablesDefaultIdColumns()).thenReturn("id,id2");
    when(config.tablesCdcField()).thenReturn("_op");
    return config;
  }
}
