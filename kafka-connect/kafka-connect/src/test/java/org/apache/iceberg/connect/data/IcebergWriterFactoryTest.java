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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.List;
import java.util.Map;
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
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

public class IcebergWriterFactoryTest {

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
}
