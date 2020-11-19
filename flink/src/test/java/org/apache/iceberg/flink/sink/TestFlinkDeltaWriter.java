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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.data.RandomRowData;
import org.apache.iceberg.io.DeltaWriter;
import org.apache.iceberg.io.DeltaWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriterResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.flink.SimpleDataUtil.ROW_TYPE;
import static org.apache.iceberg.flink.SimpleDataUtil.createRowData;

@RunWith(Parameterized.class)
public class TestFlinkDeltaWriter extends TableTestBase {
  private static final String TABLE_NAME = "flink_delta_table";
  private static final int FORMAT_V2 = 2;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Parameterized.Parameters(name = "format = {0}, partitioned = {1}")
  public static Object[][] parameters() {
    return new Object[][] {
        {"avro", false},
        {"parquet", false},
    };
  }

  private final FileFormat format;
  private final boolean partitioned;
  private Table table;

  public TestFlinkDeltaWriter(String format, boolean partitioned) {
    super(FORMAT_V2);
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
    this.partitioned = partitioned;
  }

  @Before
  public void before() throws IOException {
    File tableDir = tempFolder.newFolder();
    Assert.assertTrue(tableDir.delete());

    if (partitioned) {
      this.table = TestTables.create(tableDir, TABLE_NAME, SCHEMA, SPEC, formatVersion);
    } else {
      this.table = TestTables.create(tableDir, TABLE_NAME, SCHEMA, PartitionSpec.unpartitioned(), formatVersion);
    }
  }

  @Test
  public void testWritePureInsert() throws IOException {
    DeltaWriterFactory<RowData> writerFactory = createDeltaWriterFactory();

    DeltaWriterFactory.Context ctxt = DeltaWriterFactory.Context.builder()
        .allowPosDelete(false)
        .allowEqualityDelete(false)
        .build();

    DeltaWriter<RowData> deltaWriter = writerFactory.createDeltaWriter(null, ctxt);

    Iterable<Record> expectedRecords = RandomGenericData.generate(SCHEMA, 100, 22112234L);
    for (RowData record : RandomRowData.generate(SCHEMA, 100, 22112234L)) {
      deltaWriter.writeRow(record);
    }

    WriterResult result = deltaWriter.complete();

    Assert.assertEquals(result.dataFiles().length, 1);
    Assert.assertEquals(result.deleteFiles().length, 0);

    commitTransaction(result);

    Assert.assertEquals(Sets.newHashSet(expectedRecords), Sets.newHashSet(IcebergGenerics.read(table).build()));
  }

  @Test
  public void testWriteEqualityDelete() throws IOException {
    DeltaWriterFactory<RowData> writerFactory = createDeltaWriterFactory();

    List<Integer> equalityFieldIds = ImmutableList.of(table.schema().findField("id").fieldId());
    DeltaWriterFactory.Context ctxt = DeltaWriterFactory.Context.builder()
        .allowEqualityDelete(true)
        .equalityFieldIds(equalityFieldIds)
        .eqDeleteRowSchema(table.schema().select("id"))
        .build();

    // TODO More unit tests to test the partitioned case.
    DeltaWriter<RowData> deltaWriter = writerFactory.createDeltaWriter(null, ctxt);

    RowData record1 = createRowData(1, "aaa");
    RowData record2 = createRowData(2, "bbb");
    RowData record3 = createRowData(3, "ccc");

    deltaWriter.writeRow(record1);
    deltaWriter.writeRow(record2);

    deltaWriter.writeEqualityDelete(record1);
    deltaWriter.writeEqualityDelete(record2);

    deltaWriter.writeRow(record3);

    WriterResult result = deltaWriter.complete();
    Assert.assertEquals(result.dataFiles().length, 1);
    Assert.assertEquals(result.deleteFiles().length, 1);
    commitTransaction(result);

    assertTableRecords(Sets.newHashSet(record3));

    deltaWriter = writerFactory.createDeltaWriter(null, ctxt);
    deltaWriter.writeEqualityDelete(record3);

    result = deltaWriter.complete();
    Assert.assertEquals(result.dataFiles().length, 0);
    Assert.assertEquals(result.deleteFiles().length, 1);
    commitTransaction(result);

    assertTableRecords(ImmutableSet.of());
  }

  @Test
  public void testEqualityDeleteSameRow() throws IOException {
    DeltaWriterFactory<RowData> writerFactory = createDeltaWriterFactory();

    List<Integer> equalityFieldIds = ImmutableList.of(table.schema().findField("id").fieldId());
    DeltaWriterFactory.Context ctxt = DeltaWriterFactory.Context.builder()
        .allowEqualityDelete(true)
        .equalityFieldIds(equalityFieldIds)
        .eqDeleteRowSchema(table.schema())
        .posDeleteRowSchema(table.schema())
        .build();

    DeltaWriter<RowData> deltaWriter1 = writerFactory.createDeltaWriter(null, ctxt);

    RowData record1 = createRowData(1, "aaa");

    deltaWriter1.writeRow(record1);
    deltaWriter1.writeEqualityDelete(record1);
    deltaWriter1.writeRow(record1);
    deltaWriter1.writeEqualityDelete(record1);
    deltaWriter1.writeRow(record1);

    AssertHelpers.assertThrows("Encountered duplicated keys in the same transaction",
        ValidationException.class, () -> deltaWriter1.writeRow(record1));

    WriterResult result = deltaWriter1.complete();
    Assert.assertEquals(result.dataFiles().length, 1);
    Assert.assertEquals(result.deleteFiles().length, 1);
    commitTransaction(result);

    assertTableRecords(ImmutableSet.of(record1));

    DeltaWriter<RowData> deltaWriter2 = writerFactory.createDeltaWriter(null, ctxt);
    deltaWriter2.writeRow(record1);

    result = deltaWriter2.complete();
    Assert.assertEquals(result.dataFiles().length, 1);
    Assert.assertEquals(result.deleteFiles().length, 0);
    commitTransaction(result);

    assertTableRecords(ImmutableSet.of(record1, record1));
  }

  @Test
  public void testPositionDelete() throws IOException {
    DeltaWriterFactory<RowData> writerFactory = createDeltaWriterFactory();
    DeltaWriterFactory.Context ctxt = DeltaWriterFactory.Context.builder()
        .allowPosDelete(true)
        .build();

    DeltaWriter<RowData> deltaWriter = writerFactory.createDeltaWriter(null, ctxt);

    RowData record1 = createRowData(1, "aaa");
    RowData record2 = createRowData(2, "bbb");
    RowData record3 = createRowData(3, "ccc");

    // Write two records.
    deltaWriter.writeRow(record1);
    deltaWriter.writeRow(record2);

    WriterResult result = deltaWriter.complete();
    Assert.assertEquals(result.dataFiles().length, 1);
    Assert.assertEquals(result.deleteFiles().length, 0);
    commitTransaction(result);

    CharSequence dataFilePath = result.dataFiles()[0].path();

    // Delete the second record.
    deltaWriter = writerFactory.createDeltaWriter(null, ctxt);
    deltaWriter.writePosDelete(dataFilePath, 1);

    result = deltaWriter.complete();
    Assert.assertEquals(result.dataFiles().length, 0);
    Assert.assertEquals(result.deleteFiles().length, 1);
    commitTransaction(result);

    assertTableRecords(ImmutableSet.of(record1));

    // Delete the first record.
    deltaWriter = writerFactory.createDeltaWriter(null, ctxt);
    deltaWriter.writePosDelete(dataFilePath, 0);
    deltaWriter.writeRow(record3);

    result = deltaWriter.complete();
    Assert.assertEquals(result.dataFiles().length, 1);
    Assert.assertEquals(result.deleteFiles().length, 1);
    commitTransaction(result);

    assertTableRecords(ImmutableSet.of(record3));
  }

  @Test
  public void testUpsertSameRow() throws IOException {
    DeltaWriterFactory<RowData> writerFactory = createDeltaWriterFactory();
    List<Integer> equalityFieldIds = ImmutableList.of(table.schema().findField("id").fieldId());
    DeltaWriterFactory.Context ctxt = DeltaWriterFactory.Context.builder()
        .allowEqualityDelete(true)
        .equalityFieldIds(equalityFieldIds)
        .eqDeleteRowSchema(table.schema().select("id"))
        .build();
    DeltaWriter<RowData> deltaWriter1 = writerFactory.createDeltaWriter(null, ctxt);

    RowData record1 = createRowData(1, "aaa");
    RowData record2 = createRowData(1, "bbb");
    RowData record3 = createRowData(1, "ccc");
    RowData record4 = createRowData(1, "ddd");
    RowData record5 = createRowData(1, "eee");
    RowData record6 = createRowData(1, "fff");
    RowData record7 = createRowData(1, "ggg");

    deltaWriter1.writeRow(record1);
    AssertHelpers.assertThrows("Duplicated keys", ValidationException.class,
        () -> deltaWriter1.writeRow(record2));

    // Commit the transaction.
    WriterResult result = deltaWriter1.complete();
    Assert.assertEquals(result.dataFiles().length, 1);
    Assert.assertEquals(result.deleteFiles().length, 0);
    commitTransaction(result);

    assertTableRecords(ImmutableSet.of(record1));

    DeltaWriter<RowData> deltaWriter2 = writerFactory.createDeltaWriter(null, ctxt);

    // UPSERT (1, "ccc")
    deltaWriter2.writeEqualityDelete(record3);
    deltaWriter2.writeRow(record3);

    // INSERT (1, "ddd")
    // INSERT (1, "eee")
    AssertHelpers.assertThrows("Duplicated keys", ValidationException.class,
        () -> deltaWriter2.writeRow(record4));
    AssertHelpers.assertThrows("Duplicated keys", ValidationException.class,
        () -> deltaWriter2.writeRow(record5));

    // UPSERT (1, "fff")
    deltaWriter2.writeEqualityDelete(record6);
    deltaWriter2.writeRow(record6);

    // INSERT (1, "ggg")
    AssertHelpers.assertThrows("Duplicated keys", ValidationException.class,
        () -> deltaWriter2.writeRow(record7));

    // Commit the transaction.
    result = deltaWriter2.complete();
    Assert.assertEquals(1, result.dataFiles().length);
    // One pos-delete file, and one equality-delete file.
    Assert.assertEquals(2, result.deleteFiles().length);
    commitTransaction(result);

    assertTableRecords(ImmutableSet.of(record6));
  }

  private void assertTableRecords(Set<RowData> expectedRowDataSet) {
    StructLikeSet expectedSet = StructLikeSet.create(SCHEMA.asStruct());
    for (RowData rowData : expectedRowDataSet) {
      RowDataWrapper wrapper = new RowDataWrapper(ROW_TYPE, SCHEMA.asStruct());
      expectedSet.add(wrapper.wrap(rowData));
    }

    StructLikeSet actualSet = StructLikeSet.create(SCHEMA.asStruct());
    Iterables.addAll(actualSet, IcebergGenerics.read(table).build());
    Assert.assertEquals(expectedSet, actualSet);
  }

  private void commitTransaction(WriterResult result) {
    RowDelta rowDelta = table.newRowDelta();

    for (DataFile dataFile : result.dataFiles()) {
      rowDelta.addRows(dataFile);
    }

    for (DeleteFile deleteFile : result.deleteFiles()) {
      rowDelta.addDeletes(deleteFile);
    }

    rowDelta.commit();
  }

  private DeltaWriterFactory<RowData> createDeltaWriterFactory() {
    OutputFileFactory outputFileFactory =
        new OutputFileFactory(table.spec(), format, table.locationProvider(), table.io(),
            table.encryption(), 1, 1);

    RowType flinkSchema = FlinkSchemaUtil.convert(table.schema());
    return new FlinkDeltaWriterFactory(table.schema(), flinkSchema, table.spec(), format, outputFileFactory, table.io(),
        128 * 1024 * 1024L, table.properties()
    );
  }
}
