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
package org.apache.iceberg.flink;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.reader.ReaderUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.UUIDUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.io.TempDir;

class TestFlinkUuidType extends CatalogTestBase {
  private static final String TABLE_NAME = "test_table";
  private static final long TARGET_FILE_SIZE = 128 * 1024 * 1024;
  private static final UUID EXPECTED_UUID = UUID.fromString("0f8fad5b-d9cb-469f-a165-70867728950e");
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "uuid", Types.UUIDType.get()));

  private Table icebergTable;
  @TempDir private Path warehouseDir;

  @Parameter(index = 2)
  private FileFormat fileFormat;

  @Parameters(name = "catalogName={0}, baseNamespace={1}, fileFormat={2}")
  protected static List<Object[]> parameters() {
    return Arrays.asList(
        new Object[] {"testhadoop", Namespace.empty(), FileFormat.PARQUET},
        new Object[] {"testhadoop", Namespace.empty(), FileFormat.AVRO},
        new Object[] {"testhadoop", Namespace.empty(), FileFormat.ORC});
  }

  @Override
  @BeforeEach
  public void before() {
    super.before();
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
  }

  /** Writes UUID via Generic writer, reads via Flink. */
  @TestTemplate
  void uuidWrittenByGenericWriter() throws Exception {
    icebergTable =
        validationCatalog.createTable(
            TableIdentifier.of(icebergNamespace, TABLE_NAME),
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat.name()));

    Record record =
        GenericRecord.create(icebergTable.schema()).copy("id", 1, "uuid", EXPECTED_UUID);
    new GenericAppenderHelper(icebergTable, fileFormat, warehouseDir)
        .appendToTable(ImmutableList.of(record));
    icebergTable.refresh();

    List<GenericRowData> genericRowData = Lists.newArrayList();
    try (CloseableIterable<CombinedScanTask> combinedScanTasks =
        icebergTable.newScan().planTasks()) {
      for (CombinedScanTask combinedScanTask : combinedScanTasks) {
        try (DataIterator<RowData> dataIterator =
            ReaderUtil.createDataIterator(
                combinedScanTask, icebergTable.schema(), icebergTable.schema())) {
          while (dataIterator.hasNext()) {
            GenericRowData rowData = (GenericRowData) dataIterator.next();
            genericRowData.add(rowData);
          }
        }
      }
    }

    assertThat(genericRowData).hasSize(1);
    assertThat(genericRowData.get(0).getField(1)).isInstanceOf(byte[].class);
    byte[] bytes = (byte[]) genericRowData.get(0).getField(1);
    assertThat(bytes).hasSize(16);

    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    UUID actualUuid = new UUID(byteBuffer.getLong(), byteBuffer.getLong());
    assertThat(actualUuid).isEqualTo(EXPECTED_UUID);
  }

  /** Writes UUID via Flink TaskWriter, reads via Generic reader. */
  @TestTemplate
  void writeUuidViaFlinkWriter() throws Exception {
    icebergTable =
        validationCatalog.createTable(
            TableIdentifier.of(icebergNamespace, TABLE_NAME),
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat.name()));

    RowType rowType = FlinkSchemaUtil.convert(SCHEMA);
    RowDataTaskWriterFactory rowDataTaskWriterFactory =
        new RowDataTaskWriterFactory(
            icebergTable,
            rowType,
            TARGET_FILE_SIZE,
            fileFormat,
            icebergTable.properties(),
            null,
            false);
    rowDataTaskWriterFactory.initialize(1, 1);

    byte[] uuidBytes = UUIDUtil.convert(EXPECTED_UUID);
    GenericRowData genericRowData = GenericRowData.of(1, uuidBytes);

    try (TaskWriter<RowData> writer = rowDataTaskWriterFactory.create()) {
      writer.write(genericRowData);
      writer.close();

      AppendFiles append = icebergTable.newAppend();
      for (DataFile dataFile : writer.dataFiles()) {
        append.appendFile(dataFile);
      }

      append.commit();
    }

    List<Record> records = SimpleDataUtil.tableRecords(icebergTable);
    assertThat(records).hasSize(1);
    assertThat(records.get(0).getField("uuid")).isEqualTo(EXPECTED_UUID);
  }

  /** Writes UUID via SQL INSERT, reads via Generic reader. */
  @TestTemplate
  void sqlInsertUuid() throws Exception {
    icebergTable =
        validationCatalog.createTable(
            TableIdentifier.of(icebergNamespace, TABLE_NAME),
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(DEFAULT_FILE_FORMAT, fileFormat.name()));

    String uuidHex = EXPECTED_UUID.toString().replace("-", "");
    sql("INSERT INTO %s VALUES (1, CAST(X'%s' AS BINARY(16)))", TABLE_NAME, uuidHex);

    List<Record> records = SimpleDataUtil.tableRecords(icebergTable);
    assertThat(records).hasSize(1);
    assertThat(records.get(0).getField("uuid")).isEqualTo(EXPECTED_UUID);
  }
}
