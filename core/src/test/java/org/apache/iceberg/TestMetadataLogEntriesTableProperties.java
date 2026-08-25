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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestMetadataLogEntriesTableProperties {
  private static final String PROPERTY = "key";
  private static final String INITIAL_VALUE = "old";
  private static final String UPDATED_VALUE = "new";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of("ns", "table");
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  private InMemoryCatalog catalog;
  private BaseTable table;
  private Map<String, String> initialProperties;
  private Map<String, String> updatedProperties;

  @BeforeEach
  public void setupTableWithProperties() {
    this.catalog = new InMemoryCatalog();
    catalog.initialize("test", ImmutableMap.of());
    catalog.createNamespace(TABLE_IDENTIFIER.namespace());
    this.table =
        (BaseTable)
            catalog.createTable(
                TABLE_IDENTIFIER,
                SCHEMA,
                PartitionSpec.unpartitioned(),
                ImmutableMap.of(PROPERTY, INITIAL_VALUE));
    this.initialProperties = ImmutableMap.copyOf(table.properties());
    table.updateProperties().set(PROPERTY, UPDATED_VALUE).commit();
    this.updatedProperties = ImmutableMap.copyOf(table.properties());
  }

  @AfterEach
  public void after() throws IOException {
    catalog.dropTable(TABLE_IDENTIFIER);
    catalog.close();
  }

  @Test
  public void loadsHistoricalPropertiesAndReusesCurrentMetadata() throws IOException {
    TableMetadata current = table.operations().current();
    TableMetadata.MetadataLogEntry previous = Iterables.getOnlyElement(current.previousFiles());
    FileIO io = spy(table.io());

    DataTask task = planTask(metadataLogEntriesTable(current, io).newScan().select("properties"));

    assertThat(firstColumnValues(task)).containsExactly(initialProperties, updatedProperties);
    verify(io).newInputFile(previous.file());
    verify(
            io,
            times(1)
                .description(
                    "Current metadata should be reused rather than loading the file twice"))
        .newInputFile(current.metadataFileLocation());
  }

  @Test
  public void doesNotLoadPropertiesWhenNotProjected() throws IOException {
    TableMetadata current = table.operations().current();
    TableMetadata.MetadataLogEntry previous = Iterables.getOnlyElement(current.previousFiles());
    FileIO io = spy(table.io());

    DataTask task = planTask(metadataLogEntriesTable(current, io).newScan().select("file"));

    assertThat(firstColumnValues(task))
        .containsExactly(previous.file(), current.metadataFileLocation());
    verify(io, never()).newInputFile(previous.file());
  }

  @Test
  public void returnsNullForMissingHistoricalMetadata() throws IOException {
    TableMetadata current = table.operations().current();
    TableMetadata.MetadataLogEntry previous = Iterables.getOnlyElement(current.previousFiles());
    table.io().deleteFile(previous.file());

    DataTask task =
        planTask(metadataLogEntriesTable(current, table.io()).newScan().select("properties"));

    assertThat(firstColumnValues(task)).containsExactly(null, updatedProperties);
  }

  private static MetadataLogEntriesTable metadataLogEntriesTable(TableMetadata current, FileIO io) {
    TableOperations operations = mock(TableOperations.class);
    when(operations.current()).thenReturn(current);
    when(operations.io()).thenReturn(io);
    return new MetadataLogEntriesTable(new BaseTable(operations, "table"));
  }

  private static DataTask planTask(TableScan scan) throws IOException {
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      return Iterables.getOnlyElement(tasks).asDataTask();
    }
  }

  private static List<Object> firstColumnValues(DataTask task) throws IOException {
    List<Object> values = Lists.newArrayList();
    try (CloseableIterable<StructLike> rows = task.rows()) {
      for (StructLike row : rows) {
        values.add(row.get(0, Object.class));
      }
    }

    return values;
  }
}
