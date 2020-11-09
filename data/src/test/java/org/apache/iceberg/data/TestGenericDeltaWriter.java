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

package org.apache.iceberg.data;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.TestTables;
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

@RunWith(Parameterized.class)
public class TestGenericDeltaWriter extends TableTestBase {

  private static final String TABLE_NAME = "delta_table";

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Parameterized.Parameters(name = "formatVersion = {0}, format = {1}, partitioned = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
        {2, "avro", false},
        {2, "parquet", false},
    };
  }

  private final FileFormat format;
  private final boolean partitioned;
  private Table table;

  public TestGenericDeltaWriter(int formatVersion, String format, boolean partitioned) {
    super(formatVersion);
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
    DeltaWriterFactory<Record> writerFactory = createDeltaWriterFactory();

    DeltaWriterFactory.Context ctxt = DeltaWriterFactory.Context.builder()
        .allowPosDelete(false)
        .allowEqualityDelete(false)
        .build();

    DeltaWriter<Record> deltaWriter = writerFactory.createDeltaWriter(null, ctxt);

    List<Record> expected = RandomGenericData.generate(SCHEMA, 100, 22112234L);
    for (Record record : expected) {
      deltaWriter.writeRow(record);
    }

    WriterResult result = deltaWriter.complete();

    Assert.assertEquals(result.dataFiles().length, 1);
    Assert.assertEquals(result.deleteFiles().length, 0);

    commitTransaction(result);

    Assert.assertEquals(Sets.newHashSet(expected), Sets.newHashSet(IcebergGenerics.read(table).build()));
  }

  @Test
  public void testWriteEqualityDelete() throws IOException {
    DeltaWriterFactory<Record> writerFactory = createDeltaWriterFactory();

    List<Integer> equalityFieldIds = ImmutableList.of(table.schema().findField("id").fieldId());
    DeltaWriterFactory.Context ctxt = DeltaWriterFactory.Context.builder()
        .allowEqualityDelete(true)
        .equalityFieldIds(equalityFieldIds)
        .rowSchema(table.schema())
        .build();

    // TODO More unit tests to test the partitioned case.
    DeltaWriter<Record> deltaWriter = writerFactory.createDeltaWriter(null, ctxt);

    GenericRecord record = GenericRecord.create(SCHEMA);
    Record record1 = record.copy("id", 1, "data", "aaa");
    Record record2 = record.copy("id", 2, "data", "bbb");
    Record record3 = record.copy("id", 3, "data", "ccc");

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
  public void testUpsertSameRow() throws IOException {
    DeltaWriterFactory<Record> writerFactory = createDeltaWriterFactory();
    List<Integer> equalityFieldIds = ImmutableList.of(table.schema().findField("id").fieldId());
    DeltaWriterFactory.Context ctxt = DeltaWriterFactory.Context.builder()
        .allowEqualityDelete(true)
        .equalityFieldIds(equalityFieldIds)
        .rowSchema(table.schema().select("id"))
        .build();
    DeltaWriter<Record> deltaWriter = writerFactory.createDeltaWriter(null, ctxt);

    GenericRecord record = GenericRecord.create(SCHEMA);
    Record record1 = record.copy("id", 1, "data", "aaa");
    Record record2 = record.copy("id", 1, "data", "bbb");
    Record record3 = record.copy("id", 1, "data", "ccc");
    Record record4 = record.copy("id", 1, "data", "ddd");
    Record record5 = record.copy("id", 1, "data", "eee");
    Record record6 = record.copy("id", 1, "data", "fff");
    Record record7 = record.copy("id", 1, "data", "ggg");

    deltaWriter.writeRow(record1);
    deltaWriter.writeRow(record2);

    // Commit the transaction.
    WriterResult result = deltaWriter.complete();
    Assert.assertEquals(result.dataFiles().length, 1);
    Assert.assertEquals(result.deleteFiles().length, 0);
    commitTransaction(result);

    assertTableRecords(ImmutableSet.of(record1, record2));

    deltaWriter = writerFactory.createDeltaWriter(null, ctxt);

    // UPSERT (1, "ccc")
    deltaWriter.writeEqualityDelete(record3);
    deltaWriter.writeRow(record3);

    // INSERT (1, "ddd")
    // INSERT (1, "eee")
    deltaWriter.writeRow(record4);
    deltaWriter.writeRow(record5);

    // UPSERT (1, "fff")
    deltaWriter.writeEqualityDelete(record6);
    deltaWriter.writeRow(record6);

    // INSERT (1, "ggg")
    deltaWriter.writeRow(record7);

    // Commit the transaction.
    result = deltaWriter.complete();
    Assert.assertEquals(1, result.dataFiles().length);
    // One pos-delete file, and one equality-delete file.
    Assert.assertEquals(2, result.deleteFiles().length);
    commitTransaction(result);

    assertTableRecords(ImmutableSet.of(record6, record7));
  }

  private void assertTableRecords(Set<Record> expectedRecords) {
    StructLikeSet expectedSet = StructLikeSet.create(SCHEMA.asStruct());
    expectedSet.addAll(expectedRecords);

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

  private DeltaWriterFactory<Record> createDeltaWriterFactory() {
    OutputFileFactory outputFileFactory =
        new OutputFileFactory(table.spec(), format, table.locationProvider(), table.io(),
            table.encryption(), 1, 1);

    return new GenericDeltaWriterFactory(table.schema(), table.spec(), format, outputFileFactory, table.io(),
        128 * 1024 * 1024L, table.properties()
    );
  }
}
