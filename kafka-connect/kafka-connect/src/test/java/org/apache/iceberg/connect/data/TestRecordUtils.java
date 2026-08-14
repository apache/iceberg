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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestRecordUtils extends WriterTestBase {

  private IcebergSinkConfig config;

  @BeforeEach
  public void initConfig() {
    this.config = mock(IcebergSinkConfig.class);
    when(config.replaceNullWithDefault()).thenReturn(true);
  }

  @Test
  public void testExtractFromRecordValueStruct() {
    Schema valSchema = SchemaBuilder.struct().field("key", Schema.INT64_SCHEMA).build();
    Struct val = new Struct(valSchema).put("key", 123L);
    Object result = RecordUtils.extractFromRecordValue(val, "key", config);
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

    Object result = RecordUtils.extractFromRecordValue(val, "data.id.key", config);
    assertThat(result).isEqualTo(123L);
  }

  @Test
  public void testExtractFromRecordValueStructNull() {
    Schema valSchema = SchemaBuilder.struct().field("key", Schema.INT64_SCHEMA).build();
    Struct val = new Struct(valSchema).put("key", 123L);

    Object result = RecordUtils.extractFromRecordValue(val, "", config);
    assertThat(result).isNull();

    result = RecordUtils.extractFromRecordValue(val, "xkey", config);
    assertThat(result).isNull();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testExtractFromRecordValueStructReplaceNullWithDefault(
      boolean replaceNullWithDefault) {
    when(config.replaceNullWithDefault()).thenReturn(replaceNullWithDefault);

    Schema valSchema =
        SchemaBuilder.struct()
            .field("key", SchemaBuilder.string().optional().defaultValue("default").build())
            .build();
    Struct val = new Struct(valSchema).put("key", null);

    Object result = RecordUtils.extractFromRecordValue(val, "key", config);
    if (replaceNullWithDefault) {
      assertThat(result).isEqualTo("default");
    } else {
      assertThat(result).isNull();
    }
  }

  @Test
  public void testExtractFromRecordValueMap() {
    Map<String, Object> val = ImmutableMap.of("key", 123L);
    Object result = RecordUtils.extractFromRecordValue(val, "key", config);
    assertThat(result).isEqualTo(123L);
  }

  @Test
  public void testExtractFromRecordValueMapNested() {
    Map<String, Object> id = ImmutableMap.of("key", 123L);
    Map<String, Object> data = ImmutableMap.of("id", id);
    Map<String, Object> val = ImmutableMap.of("data", data);

    Object result = RecordUtils.extractFromRecordValue(val, "data.id.key", config);
    assertThat(result).isEqualTo(123L);
  }

  @Test
  public void testExtractFromRecordValueMapNull() {
    Map<String, Object> val = ImmutableMap.of("key", 123L);

    Object result = RecordUtils.extractFromRecordValue(val, "", config);
    assertThat(result).isNull();

    result = RecordUtils.extractFromRecordValue(val, "xkey", config);
    assertThat(result).isNull();
  }

  @Test
  public void createTableWriterWithNonMatchingNamespace() {
    Map<String, String> props =
        ImmutableMap.of(
            "iceberg.catalog.type", "rest",
            "topics", "source-topic",
            "iceberg.tables", "default.events",
            "iceberg.table.default.events.id-columns", "missing_col");
    IcebergSinkConfig sinkConfig = new IcebergSinkConfig(props);
    TableReference tableReference =
        TableReference.of(
            "test_catalog", TableIdentifier.of("default", "events"), UUID.randomUUID());

    // the per-table id-columns config is only found when looked up by the full table identifier
    assertThatThrownBy(() -> RecordUtils.createTableWriter(table, tableReference, sinkConfig))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("ID column not found: missing_col");
  }
}
