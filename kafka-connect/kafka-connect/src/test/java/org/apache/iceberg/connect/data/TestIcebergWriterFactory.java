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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

public class TestIcebergWriterFactory {

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @SuppressWarnings("unchecked")
  public void testAutoCreateTable(boolean partitioned) {
    Catalog catalog = mock(Catalog.class, withSettings().extraInterfaces(SupportsNamespaces.class));
    when(catalog.loadTable(any())).thenThrow(new NoSuchTableException("no such table"));

    TableSinkConfig tableConfig = mock(TableSinkConfig.class);
    if (partitioned) {
      when(tableConfig.partitionBy()).thenReturn(ImmutableList.of("data"));
    }

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.autoCreateProps()).thenReturn(ImmutableMap.of("test-prop", "foo1"));
    when(config.tableConfig(any())).thenReturn(tableConfig);

    SinkRecord record = mock(SinkRecord.class);
    when(record.value()).thenReturn(ImmutableMap.of("id", 123, "data", "foo2"));

    IcebergWriterFactory factory = new IcebergWriterFactory(catalog, config);
    factory.autoCreateTable("foo1.foo2.foo3.bar", record);

    ArgumentCaptor<TableIdentifier> identCaptor = ArgumentCaptor.forClass(TableIdentifier.class);
    ArgumentCaptor<Schema> schemaCaptor = ArgumentCaptor.forClass(Schema.class);
    ArgumentCaptor<PartitionSpec> specCaptor = ArgumentCaptor.forClass(PartitionSpec.class);
    ArgumentCaptor<Map<String, String>> propsCaptor = ArgumentCaptor.forClass(Map.class);

    verify(catalog)
        .createTable(
            identCaptor.capture(),
            schemaCaptor.capture(),
            specCaptor.capture(),
            propsCaptor.capture());

    assertThat(identCaptor.getValue())
        .isEqualTo(TableIdentifier.of(Namespace.of("foo1", "foo2", "foo3"), "bar"));
    assertThat(schemaCaptor.getValue().findField("id").type()).isEqualTo(LongType.get());
    assertThat(schemaCaptor.getValue().findField("data").type()).isEqualTo(StringType.get());
    assertThat(specCaptor.getValue().isPartitioned()).isEqualTo(partitioned);
    assertThat(propsCaptor.getValue()).containsKey("test-prop");

    ArgumentCaptor<Namespace> namespaceCaptor = ArgumentCaptor.forClass(Namespace.class);
    verify((SupportsNamespaces) catalog, times(3)).createNamespace(namespaceCaptor.capture());
    List<Namespace> capturedArguments = namespaceCaptor.getAllValues();
    assertThat(capturedArguments.get(0)).isEqualTo(Namespace.of("foo1"));
    assertThat(capturedArguments.get(1)).isEqualTo(Namespace.of("foo1", "foo2"));
    assertThat(capturedArguments.get(2)).isEqualTo(Namespace.of("foo1", "foo2", "foo3"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAutoCreateTableWithIdentifierFields() {
    Catalog catalog = mock(Catalog.class, withSettings().extraInterfaces(SupportsNamespaces.class));
    when(catalog.loadTable(any())).thenThrow(new NoSuchTableException("no such table"));

    TableSinkConfig tableConfig = mock(TableSinkConfig.class);
    when(tableConfig.partitionBy()).thenReturn(ImmutableList.of());
    // Configure ID columns
    when(tableConfig.idColumns()).thenReturn(ImmutableList.of("id", "data"));

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.autoCreateProps()).thenReturn(ImmutableMap.of("test-prop", "foo1"));
    when(config.tableConfig(any())).thenReturn(tableConfig);

    // Create a Kafka schema with required fields for identifier columns
    org.apache.kafka.connect.data.Schema valueSchema =
        SchemaBuilder.struct()
            .field("id", org.apache.kafka.connect.data.Schema.INT64_SCHEMA) // required
            .field("data", org.apache.kafka.connect.data.Schema.STRING_SCHEMA) // required
            .build();

    Struct value = new Struct(valueSchema).put("id", 123L).put("data", "foo2");

    SinkRecord record = mock(SinkRecord.class);
    when(record.valueSchema()).thenReturn(valueSchema);
    when(record.value()).thenReturn(value);

    IcebergWriterFactory factory = new IcebergWriterFactory(catalog, config);
    factory.autoCreateTable("foo.bar", record);

    ArgumentCaptor<TableIdentifier> identCaptor = ArgumentCaptor.forClass(TableIdentifier.class);
    ArgumentCaptor<Schema> schemaCaptor = ArgumentCaptor.forClass(Schema.class);
    ArgumentCaptor<PartitionSpec> specCaptor = ArgumentCaptor.forClass(PartitionSpec.class);
    ArgumentCaptor<Map<String, String>> propsCaptor = ArgumentCaptor.forClass(Map.class);

    verify(catalog)
        .createTable(
            identCaptor.capture(),
            schemaCaptor.capture(),
            specCaptor.capture(),
            propsCaptor.capture());

    Schema schema = schemaCaptor.getValue();
    assertThat(schema.findField("id").type()).isEqualTo(LongType.get());
    assertThat(schema.findField("data").type()).isEqualTo(StringType.get());

    // Verify identifier field IDs are set correctly
    Set<Integer> identifierFieldIds = schema.identifierFieldIds();
    assertThat(identifierFieldIds)
        .as("Schema should have identifier field IDs set")
        .containsExactlyInAnyOrder(
            schema.findField("id").fieldId(), schema.findField("data").fieldId());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAutoCreateTableWithMissingIdentifierField() {
    Catalog catalog = mock(Catalog.class, withSettings().extraInterfaces(SupportsNamespaces.class));
    when(catalog.loadTable(any())).thenThrow(new NoSuchTableException("no such table"));

    TableSinkConfig tableConfig = mock(TableSinkConfig.class);
    when(tableConfig.partitionBy()).thenReturn(ImmutableList.of());
    // Configure ID column that doesn't exist in the data
    when(tableConfig.idColumns()).thenReturn(ImmutableList.of("missing_column"));

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.autoCreateProps()).thenReturn(ImmutableMap.of());
    when(config.tableConfig(any())).thenReturn(tableConfig);

    // Create schema without the missing column
    org.apache.kafka.connect.data.Schema valueSchema =
        SchemaBuilder.struct()
            .field("id", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
            .field("data", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .build();

    Struct value = new Struct(valueSchema).put("id", 123L).put("data", "foo");

    SinkRecord record = mock(SinkRecord.class);
    when(record.valueSchema()).thenReturn(valueSchema);
    when(record.value()).thenReturn(value);

    IcebergWriterFactory factory = new IcebergWriterFactory(catalog, config);

    // Should throw DataException when ID column is not found in schema
    assertThatThrownBy(() -> factory.autoCreateTable("foo.bar", record))
        .isInstanceOf(DataException.class)
        .hasMessageContaining("ID column 'missing_column' not found in schema")
        .hasMessageContaining("Available columns:");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAutoCreateTableWithNestedIdentifierField() {
    Catalog catalog = mock(Catalog.class, withSettings().extraInterfaces(SupportsNamespaces.class));
    when(catalog.loadTable(any())).thenThrow(new NoSuchTableException("no such table"));

    TableSinkConfig tableConfig = mock(TableSinkConfig.class);
    when(tableConfig.partitionBy()).thenReturn(ImmutableList.of());
    // Dotted path references the required leaf inside a required struct
    when(tableConfig.idColumns()).thenReturn(ImmutableList.of("meta.id"));

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.autoCreateProps()).thenReturn(ImmutableMap.of());
    when(config.tableConfig(any())).thenReturn(tableConfig);

    org.apache.kafka.connect.data.Schema metaSchema =
        SchemaBuilder.struct()
            .field("id", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
            .build();
    org.apache.kafka.connect.data.Schema valueSchema =
        SchemaBuilder.struct()
            .field("meta", metaSchema)
            .field("data", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .build();

    Struct metaValue = new Struct(metaSchema).put("id", 42L);
    Struct value = new Struct(valueSchema).put("meta", metaValue).put("data", "foo");

    SinkRecord record = mock(SinkRecord.class);
    when(record.valueSchema()).thenReturn(valueSchema);
    when(record.value()).thenReturn(value);

    IcebergWriterFactory factory = new IcebergWriterFactory(catalog, config);
    factory.autoCreateTable("foo.bar", record);

    ArgumentCaptor<Schema> schemaCaptor = ArgumentCaptor.forClass(Schema.class);
    verify(catalog).createTable(any(), schemaCaptor.capture(), any(), any());

    Schema schema = schemaCaptor.getValue();
    int nestedLeafId = schema.findField("meta.id").fieldId();
    assertThat(schema.identifierFieldIds())
        .as("Nested leaf referenced by dotted id-column should be stamped as identifier")
        .containsExactly(nestedLeafId);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAutoCreateTableWithOptionalIdentifierField() {
    Catalog catalog = mock(Catalog.class, withSettings().extraInterfaces(SupportsNamespaces.class));
    when(catalog.loadTable(any())).thenThrow(new NoSuchTableException("no such table"));

    TableSinkConfig tableConfig = mock(TableSinkConfig.class);
    when(tableConfig.partitionBy()).thenReturn(ImmutableList.of());
    // Configure ID column
    when(tableConfig.idColumns()).thenReturn(ImmutableList.of("id"));

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.autoCreateProps()).thenReturn(ImmutableMap.of());
    when(config.tableConfig(any())).thenReturn(tableConfig);

    // Create schema with optional identifier field
    org.apache.kafka.connect.data.Schema valueSchema =
        SchemaBuilder.struct()
            .field("id", org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA) // optional!
            .field("data", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .build();

    Struct value = new Struct(valueSchema).put("id", 123L).put("data", "foo");

    SinkRecord record = mock(SinkRecord.class);
    when(record.valueSchema()).thenReturn(valueSchema);
    when(record.value()).thenReturn(value);

    IcebergWriterFactory factory = new IcebergWriterFactory(catalog, config);

    // Should throw DataException when ID column is optional
    assertThatThrownBy(() -> factory.autoCreateTable("foo.bar", record))
        .isInstanceOf(DataException.class)
        .hasMessageContaining("Invalid identifier column configuration for table foo.bar")
        .hasMessageContaining("not a required field");
  }
}
