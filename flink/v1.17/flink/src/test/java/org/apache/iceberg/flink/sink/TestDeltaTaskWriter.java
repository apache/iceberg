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
package org.apache.iceberg.flink.sink;

import static org.apache.iceberg.flink.SimpleDataUtil.createDelete;
import static org.apache.iceberg.flink.SimpleDataUtil.createInsert;
import static org.apache.iceberg.flink.SimpleDataUtil.createRecord;
import static org.apache.iceberg.flink.SimpleDataUtil.createUpdateAfter;
import static org.apache.iceberg.flink.SimpleDataUtil.createUpdateBefore;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestDeltaTaskWriter extends TableTestBase {
  private static final int FORMAT_V2 = 2;

  private final FileFormat format;

  @Parameterized.Parameters(name = "FileFormat = {0}")
  public static Object[][] parameters() {
    return new Object[][] {{"avro"}, {"orc"}, {"parquet"}};
  }

  public TestDeltaTaskWriter(String fileFormat) {
    super(FORMAT_V2);
    this.format = FileFormat.fromString(fileFormat);
  }

  @Override
  @Before
  public void setupTable() throws IOException {
    this.tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete()); // created by table create

    this.metadataDir = new File(tableDir, "metadata");
  }

  private int idFieldId() {
    return table.schema().findField("id").fieldId();
  }

  private int dataFieldId() {
    return table.schema().findField("data").fieldId();
  }

  private void testCdcEvents(boolean partitioned) throws IOException {
    List<Integer> equalityFieldIds = Lists.newArrayList(idFieldId());
    TaskWriterFactory<RowData> taskWriterFactory = createTaskWriterFactory(equalityFieldIds);
    taskWriterFactory.initialize(1, 1);

    // Start the 1th transaction.
    TaskWriter<RowData> writer = taskWriterFactory.create();

    writer.write(createInsert(1, "aaa"));
    writer.write(createInsert(2, "bbb"));
    writer.write(createInsert(3, "ccc"));

    // Update <2, 'bbb'> to <2, 'ddd'>
    writer.write(createUpdateBefore(2, "bbb")); // 1 pos-delete and 1 eq-delete.
    writer.write(createUpdateAfter(2, "ddd"));

    // Update <1, 'aaa'> to <1, 'eee'>
    writer.write(createUpdateBefore(1, "aaa")); // 1 pos-delete and 1 eq-delete.
    writer.write(createUpdateAfter(1, "eee"));

    // Insert <4, 'fff'>
    writer.write(createInsert(4, "fff"));
    // Insert <5, 'ggg'>
    writer.write(createInsert(5, "ggg"));

    // Delete <3, 'ccc'>
    writer.write(createDelete(3, "ccc")); // 1 pos-delete and 1 eq-delete.

    WriteResult result = writer.complete();
    Assert.assertEquals(partitioned ? 7 : 1, result.dataFiles().length);
    Assert.assertEquals(partitioned ? 3 : 1, result.deleteFiles().length);
    commitTransaction(result);

    Assert.assertEquals(
        "Should have expected records.",
        expectedRowSet(
            createRecord(1, "eee"),
            createRecord(2, "ddd"),
            createRecord(4, "fff"),
            createRecord(5, "ggg")),
        actualRowSet("*"));

    // Start the 2nd transaction.
    writer = taskWriterFactory.create();

    // Update <2, 'ddd'> to <6, 'hhh'> - (Update both key and value)
    writer.write(createUpdateBefore(2, "ddd")); // 1 eq-delete
    writer.write(createUpdateAfter(6, "hhh"));

    // Update <5, 'ggg'> to <5, 'iii'>
    writer.write(createUpdateBefore(5, "ggg")); // 1 eq-delete
    writer.write(createUpdateAfter(5, "iii"));

    // Delete <4, 'fff'>
    writer.write(createDelete(4, "fff")); // 1 eq-delete.

    result = writer.complete();
    Assert.assertEquals(partitioned ? 2 : 1, result.dataFiles().length);
    Assert.assertEquals(partitioned ? 3 : 1, result.deleteFiles().length);
    commitTransaction(result);

    Assert.assertEquals(
        "Should have expected records",
        expectedRowSet(createRecord(1, "eee"), createRecord(5, "iii"), createRecord(6, "hhh")),
        actualRowSet("*"));
  }

  @Test
  public void testUnpartitioned() throws IOException {
    createAndInitTable(false);
    testCdcEvents(false);
  }

  @Test
  public void testPartitioned() throws IOException {
    createAndInitTable(true);
    testCdcEvents(true);
  }

  private void testWritePureEqDeletes(boolean partitioned) throws IOException {
    createAndInitTable(partitioned);
    List<Integer> equalityFieldIds = Lists.newArrayList(idFieldId());
    TaskWriterFactory<RowData> taskWriterFactory = createTaskWriterFactory(equalityFieldIds);
    taskWriterFactory.initialize(1, 1);

    TaskWriter<RowData> writer = taskWriterFactory.create();
    writer.write(createDelete(1, "aaa"));
    writer.write(createDelete(2, "bbb"));
    writer.write(createDelete(3, "ccc"));

    WriteResult result = writer.complete();
    Assert.assertEquals(0, result.dataFiles().length);
    Assert.assertEquals(partitioned ? 3 : 1, result.deleteFiles().length);
    commitTransaction(result);

    Assert.assertEquals("Should have no record", expectedRowSet(), actualRowSet("*"));
  }

  @Test
  public void testUnpartitionedPureEqDeletes() throws IOException {
    testWritePureEqDeletes(false);
  }

  @Test
  public void testPartitionedPureEqDeletes() throws IOException {
    testWritePureEqDeletes(true);
  }

  private void testAbort(boolean partitioned) throws IOException {
    createAndInitTable(partitioned);
    List<Integer> equalityFieldIds = Lists.newArrayList(idFieldId());
    TaskWriterFactory<RowData> taskWriterFactory = createTaskWriterFactory(equalityFieldIds);
    taskWriterFactory.initialize(1, 1);

    TaskWriter<RowData> writer = taskWriterFactory.create();
    for (int i = 0; i < 8_000; i += 2) {
      writer.write(createUpdateBefore(i + 1, "aaa"));
      writer.write(createUpdateAfter(i + 1, "aaa"));

      writer.write(createUpdateBefore(i + 2, "bbb"));
      writer.write(createUpdateAfter(i + 2, "bbb"));
    }

    // Assert the current data/delete file count.
    List<Path> files =
        Files.walk(Paths.get(tableDir.getPath(), "data"))
            .filter(p -> p.toFile().isFile())
            .filter(p -> !p.toString().endsWith(".crc"))
            .collect(Collectors.toList());
    Assert.assertEquals(
        "Should have expected file count, but files are: " + files,
        partitioned ? 4 : 2,
        files.size());

    writer.abort();
    for (Path file : files) {
      Assert.assertFalse(Files.exists(file));
    }
  }

  @Test
  public void testUnpartitionedAbort() throws IOException {
    testAbort(false);
  }

  @Test
  public void testPartitionedAbort() throws IOException {
    testAbort(true);
  }

  @Test
  public void testPartitionedTableWithDataAsKey() throws IOException {
    createAndInitTable(true);
    List<Integer> equalityFieldIds = Lists.newArrayList(dataFieldId());
    TaskWriterFactory<RowData> taskWriterFactory = createTaskWriterFactory(equalityFieldIds);
    taskWriterFactory.initialize(1, 1);

    // Start the 1th transaction.
    TaskWriter<RowData> writer = taskWriterFactory.create();
    writer.write(createInsert(1, "aaa"));
    writer.write(createInsert(2, "aaa"));
    writer.write(createInsert(3, "bbb"));
    writer.write(createInsert(4, "ccc"));

    WriteResult result = writer.complete();
    Assert.assertEquals(3, result.dataFiles().length);
    Assert.assertEquals(1, result.deleteFiles().length);
    commitTransaction(result);

    Assert.assertEquals(
        "Should have expected records",
        expectedRowSet(createRecord(2, "aaa"), createRecord(3, "bbb"), createRecord(4, "ccc")),
        actualRowSet("*"));

    // Start the 2nd transaction.
    writer = taskWriterFactory.create();
    writer.write(createInsert(5, "aaa"));
    writer.write(createInsert(6, "bbb"));
    writer.write(createDelete(7, "ccc")); // 1 eq-delete.

    result = writer.complete();
    Assert.assertEquals(2, result.dataFiles().length);
    Assert.assertEquals(1, result.deleteFiles().length);
    commitTransaction(result);

    Assert.assertEquals(
        "Should have expected records",
        expectedRowSet(
            createRecord(2, "aaa"),
            createRecord(5, "aaa"),
            createRecord(3, "bbb"),
            createRecord(6, "bbb")),
        actualRowSet("*"));
  }

  @Test
  public void testPartitionedTableWithDataAndIdAsKey() throws IOException {
    createAndInitTable(true);
    List<Integer> equalityFieldIds = Lists.newArrayList(dataFieldId(), idFieldId());
    TaskWriterFactory<RowData> taskWriterFactory = createTaskWriterFactory(equalityFieldIds);
    taskWriterFactory.initialize(1, 1);

    TaskWriter<RowData> writer = taskWriterFactory.create();
    writer.write(createInsert(1, "aaa"));
    writer.write(createInsert(2, "aaa"));

    writer.write(createDelete(2, "aaa")); // 1 pos-delete.

    WriteResult result = writer.complete();
    Assert.assertEquals(1, result.dataFiles().length);
    Assert.assertEquals(1, result.deleteFiles().length);
    Assert.assertEquals(
        Sets.newHashSet(FileContent.POSITION_DELETES),
        Sets.newHashSet(result.deleteFiles()[0].content()));
    commitTransaction(result);

    Assert.assertEquals(
        "Should have expected records", expectedRowSet(createRecord(1, "aaa")), actualRowSet("*"));
  }

  @Test
  public void testEqualityColumnOnCustomPrecisionTSColumn() throws IOException {
    Schema tableSchema =
        new Schema(
            required(3, "id", Types.IntegerType.get()),
            required(4, "ts", Types.TimestampType.withZone()));
    RowType flinkType =
        new RowType(
            false,
            ImmutableList.of(
                new RowType.RowField("id", new IntType()),
                new RowType.RowField("ts", new LocalZonedTimestampType(3))));

    this.table = create(tableSchema, PartitionSpec.unpartitioned());
    initTable(table);

    List<Integer> equalityIds = ImmutableList.of(table.schema().findField("ts").fieldId());
    TaskWriterFactory<RowData> taskWriterFactory = createTaskWriterFactory(flinkType, equalityIds);
    taskWriterFactory.initialize(1, 1);

    TaskWriter<RowData> writer = taskWriterFactory.create();
    RowDataSerializer serializer = new RowDataSerializer(flinkType);
    OffsetDateTime start = OffsetDateTime.now();
    writer.write(
        serializer.toBinaryRow(
            GenericRowData.ofKind(
                RowKind.INSERT, 1, TimestampData.fromInstant(start.toInstant()))));
    writer.write(
        serializer.toBinaryRow(
            GenericRowData.ofKind(
                RowKind.INSERT, 2, TimestampData.fromInstant(start.plusSeconds(1).toInstant()))));
    writer.write(
        serializer.toBinaryRow(
            GenericRowData.ofKind(
                RowKind.DELETE, 2, TimestampData.fromInstant(start.plusSeconds(1).toInstant()))));

    WriteResult result = writer.complete();
    // One data file
    Assertions.assertThat(result.dataFiles().length).isEqualTo(1);
    // One eq delete file + one pos delete file
    Assertions.assertThat(result.deleteFiles().length).isEqualTo(2);
    Assertions.assertThat(
            Arrays.stream(result.deleteFiles())
                .map(ContentFile::content)
                .collect(Collectors.toSet()))
        .isEqualTo(Sets.newHashSet(FileContent.POSITION_DELETES, FileContent.EQUALITY_DELETES));
    commitTransaction(result);

    Record expectedRecord = GenericRecord.create(tableSchema);
    expectedRecord.setField("id", 1);
    int cutPrecisionNano = start.getNano() / 1000000 * 1000000;
    expectedRecord.setField("ts", start.withNano(cutPrecisionNano));

    Assertions.assertThat(actualRowSet("*")).isEqualTo(expectedRowSet(expectedRecord));
  }

  private void commitTransaction(WriteResult result) {
    RowDelta rowDelta = table.newRowDelta();
    Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
    Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);
    rowDelta
        .validateDeletedFiles()
        .validateDataFilesExist(Lists.newArrayList(result.referencedDataFiles()))
        .commit();
  }

  private StructLikeSet expectedRowSet(Record... records) {
    return SimpleDataUtil.expectedRowSet(table, records);
  }

  private StructLikeSet actualRowSet(String... columns) throws IOException {
    return SimpleDataUtil.actualRowSet(table, columns);
  }

  private TaskWriterFactory<RowData> createTaskWriterFactory(List<Integer> equalityFieldIds) {
    return new RowDataTaskWriterFactory(
        SerializableTable.copyOf(table),
        FlinkSchemaUtil.convert(table.schema()),
        128 * 1024 * 1024,
        format,
        table.properties(),
        equalityFieldIds,
        false);
  }

  private TaskWriterFactory<RowData> createTaskWriterFactory(
      RowType flinkType, List<Integer> equalityFieldIds) {
    return new RowDataTaskWriterFactory(
        SerializableTable.copyOf(table),
        flinkType,
        128 * 1024 * 1024,
        format,
        table.properties(),
        equalityFieldIds,
        true);
  }

  private void createAndInitTable(boolean partitioned) {
    if (partitioned) {
      this.table = create(SCHEMA, PartitionSpec.builderFor(SCHEMA).identity("data").build());
    } else {
      this.table = create(SCHEMA, PartitionSpec.unpartitioned());
    }

    initTable(table);
  }

  private void initTable(TestTables.TestTable testTable) {
    testTable
        .updateProperties()
        .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, String.valueOf(8 * 1024))
        .defaultFormat(format)
        .commit();
  }
}
