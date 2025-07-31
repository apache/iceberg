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
package org.apache.iceberg.flink.sink.dynamic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.net.URI;
import java.util.Collection;
import java.util.Map;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynFields;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.sink.FlinkAppenderFactory;
import org.apache.iceberg.flink.sink.TestFlinkIcebergSinkBase;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

class TestDynamicWriter extends TestFlinkIcebergSinkBase {

  private static final TableIdentifier TABLE1 = TableIdentifier.of("myTable1");
  private static final TableIdentifier TABLE2 = TableIdentifier.of("myTable2");

  @Test
  void testDynamicWriter() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    Table table1 = catalog.createTable(TABLE1, SimpleDataUtil.SCHEMA);
    Table table2 = catalog.createTable(TABLE2, SimpleDataUtil.SCHEMA);

    DynamicWriter dynamicWriter = createDynamicWriter(catalog);

    DynamicRecordInternal record1 = getDynamicRecordInternal(table1);
    DynamicRecordInternal record2 = getDynamicRecordInternal(table2);

    assertThat(getNumDataFiles(table1)).isEqualTo(0);

    dynamicWriter.write(record1, null);
    dynamicWriter.write(record2, null);
    Collection<DynamicWriteResult> writeResults = dynamicWriter.prepareCommit();

    assertThat(writeResults).hasSize(2);
    assertThat(getNumDataFiles(table1)).isEqualTo(1);
    assertThat(
            dynamicWriter
                .getMetrics()
                .writerMetrics(TABLE1.name())
                .getFlushedDataFiles()
                .getCount())
        .isEqualTo(1);
    assertThat(
            dynamicWriter
                .getMetrics()
                .writerMetrics(TABLE2.name())
                .getFlushedDataFiles()
                .getCount())
        .isEqualTo(1);

    WriteResult wr1 = writeResults.iterator().next().writeResult();
    assertThat(wr1.dataFiles().length).isEqualTo(1);
    assertThat(wr1.dataFiles()[0].format()).isEqualTo(FileFormat.PARQUET);
    assertThat(wr1.deleteFiles()).isEmpty();

    dynamicWriter.write(record1, null);
    dynamicWriter.write(record2, null);
    writeResults = dynamicWriter.prepareCommit();

    assertThat(writeResults).hasSize(2);
    assertThat(getNumDataFiles(table1)).isEqualTo(2);
    assertThat(
            dynamicWriter
                .getMetrics()
                .writerMetrics(TABLE1.name())
                .getFlushedDataFiles()
                .getCount())
        .isEqualTo(2);
    assertThat(
            dynamicWriter
                .getMetrics()
                .writerMetrics(TABLE2.name())
                .getFlushedDataFiles()
                .getCount())
        .isEqualTo(2);

    WriteResult wr2 = writeResults.iterator().next().writeResult();
    assertThat(wr2.dataFiles().length).isEqualTo(1);
    assertThat(wr2.dataFiles()[0].format()).isEqualTo(FileFormat.PARQUET);
    assertThat(wr2.deleteFiles()).isEmpty();

    dynamicWriter.close();
  }

  @Test
  void testDynamicWriterPropertiesDefault() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    Table table1 =
        catalog.createTable(
            TABLE1,
            SimpleDataUtil.SCHEMA,
            null,
            ImmutableMap.of("write.parquet.compression-codec", "zstd"));

    DynamicWriter dynamicWriter = createDynamicWriter(catalog);
    DynamicRecordInternal record1 = getDynamicRecordInternal(table1);

    assertThat(getNumDataFiles(table1)).isEqualTo(0);

    dynamicWriter.write(record1, null);
    Map<String, String> properties = properties(dynamicWriter);
    assertThat(properties).containsEntry("write.parquet.compression-codec", "zstd");

    dynamicWriter.close();
  }

  @Test
  void testDynamicWriterPropertiesPriority() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    Table table1 =
        catalog.createTable(
            TABLE1,
            SimpleDataUtil.SCHEMA,
            null,
            ImmutableMap.of("write.parquet.compression-codec", "zstd"));

    DynamicWriter dynamicWriter =
        createDynamicWriter(catalog, ImmutableMap.of("write.parquet.compression-codec", "gzip"));
    DynamicRecordInternal record1 = getDynamicRecordInternal(table1);

    assertThat(getNumDataFiles(table1)).isEqualTo(0);

    dynamicWriter.write(record1, null);
    Map<String, String> properties = properties(dynamicWriter);
    assertThat(properties).containsEntry("write.parquet.compression-codec", "gzip");

    dynamicWriter.close();
  }

  @Test
  void testDynamicWriterUpsert() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    DynamicWriter dyamicWriter = createDynamicWriter(catalog);
    Table table1 = CATALOG_EXTENSION.catalog().createTable(TABLE1, SimpleDataUtil.SCHEMA);

    DynamicRecordInternal record = getDynamicRecordInternal(table1);
    record.setUpsertMode(true);
    record.setEqualityFieldIds(Sets.newHashSet(1));

    dyamicWriter.write(record, null);
    dyamicWriter.prepareCommit();

    assertThat(
            dyamicWriter
                .getMetrics()
                .writerMetrics(TABLE1.name())
                .getFlushedDeleteFiles()
                .getCount())
        .isEqualTo(1);
    assertThat(
            dyamicWriter.getMetrics().writerMetrics(TABLE1.name()).getFlushedDataFiles().getCount())
        .isEqualTo(1);
  }

  @Test
  void testDynamicWriterUpsertNoEqualityFields() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    DynamicWriter dyamicWriter = createDynamicWriter(catalog);
    Table table1 = CATALOG_EXTENSION.catalog().createTable(TABLE1, SimpleDataUtil.SCHEMA);

    DynamicRecordInternal record = getDynamicRecordInternal(table1);
    record.setUpsertMode(true);

    assertThatThrownBy(() -> dyamicWriter.write(record, null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Equality field columns shouldn't be empty when configuring to use UPSERT data.");
  }

  private static @NotNull DynamicWriter createDynamicWriter(
      Catalog catalog, Map<String, String> properties) {
    DynamicWriter dynamicWriter =
        new DynamicWriter(
            catalog,
            FileFormat.PARQUET,
            1024L,
            properties,
            100,
            new DynamicWriterMetrics(new UnregisteredMetricsGroup()),
            0,
            0);
    return dynamicWriter;
  }

  private static @NotNull DynamicWriter createDynamicWriter(Catalog catalog) {
    return createDynamicWriter(catalog, Map.of());
  }

  private static @NotNull DynamicRecordInternal getDynamicRecordInternal(Table table1) {
    DynamicRecordInternal record = new DynamicRecordInternal();
    record.setTableName(TableIdentifier.parse(table1.name()).name());
    record.setSchema(table1.schema());
    record.setSpec(table1.spec());
    record.setRowData(SimpleDataUtil.createRowData(1, "test"));
    return record;
  }

  private static int getNumDataFiles(Table table) {
    File dataDir = new File(URI.create(table.location()).getPath(), "data");
    if (dataDir.exists()) {
      return dataDir.listFiles((dir, name) -> !name.startsWith(".")).length;
    }
    return 0;
  }

  private Map<String, String> properties(DynamicWriter dynamicWriter) {
    DynFields.BoundField<Map<WriteTarget, TaskWriter<RowData>>> writerField =
        DynFields.builder().hiddenImpl(dynamicWriter.getClass(), "writers").build(dynamicWriter);

    DynFields.BoundField<FlinkAppenderFactory> appenderField =
        DynFields.builder()
            .hiddenImpl(BaseTaskWriter.class, "appenderFactory")
            .build(writerField.get().values().iterator().next());
    DynFields.BoundField<Map<String, String>> propsField =
        DynFields.builder()
            .hiddenImpl(FlinkAppenderFactory.class, "props")
            .build(appenderField.get());
    return propsField.get();
  }
}
