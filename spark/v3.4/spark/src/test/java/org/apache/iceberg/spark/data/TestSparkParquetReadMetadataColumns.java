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
package org.apache.iceberg.spark.data;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkParquetReaders;
import org.apache.iceberg.types.Types;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSparkParquetReadMetadataColumns {
  private static final Schema DATA_SCHEMA =
      new Schema(
          required(100, "id", Types.LongType.get()), required(101, "data", Types.StringType.get()));

  private static final Schema PROJECTION_SCHEMA =
      new Schema(
          required(100, "id", Types.LongType.get()),
          required(101, "data", Types.StringType.get()),
          MetadataColumns.ROW_POSITION,
          MetadataColumns.IS_DELETED);

  private static final int NUM_ROWS = 1000;
  private static final List<InternalRow> DATA_ROWS;
  private static final List<InternalRow> EXPECTED_ROWS;
  private static final int NUM_ROW_GROUPS = 10;
  private static final int ROWS_PER_SPLIT = NUM_ROWS / NUM_ROW_GROUPS;
  private static final int RECORDS_PER_BATCH = ROWS_PER_SPLIT / 10;

  static {
    DATA_ROWS = Lists.newArrayListWithCapacity(NUM_ROWS);
    for (long i = 0; i < NUM_ROWS; i += 1) {
      InternalRow row = new GenericInternalRow(DATA_SCHEMA.columns().size());
      if (i >= NUM_ROWS / 2) {
        row.update(0, 2 * i);
      } else {
        row.update(0, i);
      }
      row.update(1, UTF8String.fromString("str" + i));
      DATA_ROWS.add(row);
    }

    EXPECTED_ROWS = Lists.newArrayListWithCapacity(NUM_ROWS);
    for (long i = 0; i < NUM_ROWS; i += 1) {
      InternalRow row = new GenericInternalRow(PROJECTION_SCHEMA.columns().size());
      if (i >= NUM_ROWS / 2) {
        row.update(0, 2 * i);
      } else {
        row.update(0, i);
      }
      row.update(1, UTF8String.fromString("str" + i));
      row.update(2, i);
      row.update(3, false);
      EXPECTED_ROWS.add(row);
    }
  }

  @Parameterized.Parameters(name = "vectorized = {0}")
  public static Object[][] parameters() {
    return new Object[][] {new Object[] {false}, new Object[] {true}};
  }

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private final boolean vectorized;
  private File testFile;

  public TestSparkParquetReadMetadataColumns(boolean vectorized) {
    this.vectorized = vectorized;
  }

  @Before
  public void writeFile() throws IOException {
    List<Path> fileSplits = Lists.newArrayList();
    StructType struct = SparkSchemaUtil.convert(DATA_SCHEMA);
    Configuration conf = new Configuration();

    testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());
    ParquetFileWriter parquetFileWriter =
        new ParquetFileWriter(
            conf,
            ParquetSchemaUtil.convert(DATA_SCHEMA, "testSchema"),
            new Path(testFile.getAbsolutePath()));

    parquetFileWriter.start();
    for (int i = 0; i < NUM_ROW_GROUPS; i += 1) {
      File split = temp.newFile();
      Assert.assertTrue("Delete should succeed", split.delete());
      fileSplits.add(new Path(split.getAbsolutePath()));
      try (FileAppender<InternalRow> writer =
          Parquet.write(Files.localOutput(split))
              .createWriterFunc(msgType -> SparkParquetWriters.buildWriter(struct, msgType))
              .schema(DATA_SCHEMA)
              .overwrite()
              .build()) {
        writer.addAll(DATA_ROWS.subList(i * ROWS_PER_SPLIT, (i + 1) * ROWS_PER_SPLIT));
      }
      parquetFileWriter.appendFile(
          HadoopInputFile.fromPath(new Path(split.getAbsolutePath()), conf));
    }
    parquetFileWriter.end(
        ParquetFileWriter.mergeMetadataFiles(fileSplits, conf)
            .getFileMetaData()
            .getKeyValueMetaData());
  }

  @Test
  public void testReadRowNumbers() throws IOException {
    readAndValidate(null, null, null, EXPECTED_ROWS);
  }

  @Test
  public void testReadRowNumbersWithDelete() throws IOException {
    Assume.assumeTrue(vectorized);

    List<InternalRow> expectedRowsAfterDelete = Lists.newArrayList();
    EXPECTED_ROWS.forEach(row -> expectedRowsAfterDelete.add(row.copy()));
    // remove row at position 98, 99, 100, 101, 102, this crosses two row groups [0, 100) and [100,
    // 200)
    for (int i = 98; i <= 102; i++) {
      expectedRowsAfterDelete.get(i).update(3, true);
    }

    Parquet.ReadBuilder builder =
        Parquet.read(Files.localInput(testFile)).project(PROJECTION_SCHEMA);

    DeleteFilter deleteFilter = mock(DeleteFilter.class);
    when(deleteFilter.hasPosDeletes()).thenReturn(true);
    PositionDeleteIndex deletedRowPos = new CustomizedPositionDeleteIndex();
    deletedRowPos.delete(98, 103);
    when(deleteFilter.deletedRowPositions()).thenReturn(deletedRowPos);

    builder.createBatchedReaderFunc(
        fileSchema ->
            VectorizedSparkParquetReaders.buildReader(
                PROJECTION_SCHEMA, fileSchema, Maps.newHashMap(), deleteFilter));
    builder.recordsPerBatch(RECORDS_PER_BATCH);

    validate(expectedRowsAfterDelete, builder);
  }

  private class CustomizedPositionDeleteIndex implements PositionDeleteIndex {
    private final Set<Long> deleteIndex;

    private CustomizedPositionDeleteIndex() {
      deleteIndex = Sets.newHashSet();
    }

    @Override
    public void delete(long position) {
      deleteIndex.add(position);
    }

    @Override
    public void delete(long posStart, long posEnd) {
      for (long l = posStart; l < posEnd; l++) {
        delete(l);
      }
    }

    @Override
    public boolean isDeleted(long position) {
      return deleteIndex.contains(position);
    }

    @Override
    public boolean isEmpty() {
      return deleteIndex.isEmpty();
    }
  }

  @Test
  public void testReadRowNumbersWithFilter() throws IOException {
    // current iceberg supports row group filter.
    for (int i = 1; i < 5; i += 1) {
      readAndValidate(
          Expressions.and(
              Expressions.lessThan("id", NUM_ROWS / 2),
              Expressions.greaterThanOrEqual("id", i * ROWS_PER_SPLIT)),
          null,
          null,
          EXPECTED_ROWS.subList(i * ROWS_PER_SPLIT, NUM_ROWS / 2));
    }
  }

  @Test
  public void testReadRowNumbersWithSplits() throws IOException {
    ParquetFileReader fileReader =
        new ParquetFileReader(
            HadoopInputFile.fromPath(new Path(testFile.getAbsolutePath()), new Configuration()),
            ParquetReadOptions.builder().build());
    List<BlockMetaData> rowGroups = fileReader.getRowGroups();
    for (int i = 0; i < NUM_ROW_GROUPS; i += 1) {
      readAndValidate(
          null,
          rowGroups.get(i).getColumns().get(0).getStartingPos(),
          rowGroups.get(i).getCompressedSize(),
          EXPECTED_ROWS.subList(i * ROWS_PER_SPLIT, (i + 1) * ROWS_PER_SPLIT));
    }
  }

  private void readAndValidate(
      Expression filter, Long splitStart, Long splitLength, List<InternalRow> expected)
      throws IOException {
    Parquet.ReadBuilder builder =
        Parquet.read(Files.localInput(testFile)).project(PROJECTION_SCHEMA);

    if (vectorized) {
      builder.createBatchedReaderFunc(
          fileSchema ->
              VectorizedSparkParquetReaders.buildReader(
                  PROJECTION_SCHEMA, fileSchema, Maps.newHashMap(), null));
      builder.recordsPerBatch(RECORDS_PER_BATCH);
    } else {
      builder =
          builder.createReaderFunc(
              msgType -> SparkParquetReaders.buildReader(PROJECTION_SCHEMA, msgType));
    }

    if (filter != null) {
      builder = builder.filter(filter);
    }

    if (splitStart != null && splitLength != null) {
      builder = builder.split(splitStart, splitLength);
    }

    validate(expected, builder);
  }

  private void validate(List<InternalRow> expected, Parquet.ReadBuilder builder)
      throws IOException {
    try (CloseableIterable<InternalRow> reader =
        vectorized ? batchesToRows(builder.build()) : builder.build()) {
      final Iterator<InternalRow> actualRows = reader.iterator();

      for (InternalRow internalRow : expected) {
        Assert.assertTrue("Should have expected number of rows", actualRows.hasNext());
        TestHelpers.assertEquals(PROJECTION_SCHEMA, internalRow, actualRows.next());
      }

      Assert.assertFalse("Should not have extra rows", actualRows.hasNext());
    }
  }

  private CloseableIterable<InternalRow> batchesToRows(CloseableIterable<ColumnarBatch> batches) {
    return CloseableIterable.combine(
        Iterables.concat(Iterables.transform(batches, b -> (Iterable<InternalRow>) b::rowIterator)),
        batches);
  }
}
