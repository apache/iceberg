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
package org.apache.iceberg.spark.data.vectorized.parquet;

import static org.apache.iceberg.TableProperties.PARQUET_PAGE_ROW_LIMIT;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.data.AvroDataTestBase;
import org.apache.iceberg.spark.data.GenericsHelpers;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkParquetReaders;
import org.apache.iceberg.spark.source.BatchReaderUtil;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestParquetPageSkippingVectorizedReads extends AvroDataTestBase {

  private static final Schema DATA_SCHEMA = new Schema(SUPPORTED_PRIMITIVES.fields());

  @TempDir protected static Path temp;

  private static List<Record> allRecords = Lists.newArrayList();

  /* Column and offset indexes info of `l` column in `testFile` copied from text printed by parquet-cli's
  column-index command:
  row-group 0:
  column index for column l:
                    null count                   min                  max
  page-0                     0                     0                   99
  page-1                     0                   100                  199
  page-2                     0                   200                  299
  page-3                     0                   300                  399
  page-4                     0                   400                  499
  page-5                     0                   500                  599
  page-6                     0                   600                  612
  offset index for column l:
                        offset       compressed size       first row index
  page-0                 21230                   213                     0
  page-1                 21443                   213                   100
  page-2                 21656                   213                   200
  page-3                 21869                   216                   300
  page-4                 22085                   217                   400
  page-5                 22302                   216                   500
  page-6                 22518                    77                   600
  row-group 1:
  column index for column l:
                    null count                   min                  max
  page-0                     0                   613                  712
  page-1                     0                   713                  812
  page-2                     0                   813                  912
  page-3                     0                   913                  999
  offset index for column l:
                        offset       compressed size       first row index
  page-0                117066                   217                     0
  page-1                117283                   216                   100
  page-2                117499                   217                   200
  page-3                117716                   197                   300
  */
  private static long index = -1;
  private static File testFile;

  @BeforeAll
  public static void generateFile() throws IOException {
    Function<Record, Record> transform =
        record -> {
          index += 1;
          record.setField("l", index);
          return record;
        };
    int numRecords = 1000;
    allRecords =
        RandomGenericData.generate(DATA_SCHEMA, numRecords, 0L).stream()
            .map(transform)
            .collect(Collectors.toList());
    testFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(testFile.delete()).as("Delete should succeed").isTrue();
    try (FileAppender<Record> fileAppender =
        Parquet.write(Files.localOutput(testFile))
            .schema(DATA_SCHEMA)
            .createWriterFunc(GenericParquetWriter::create)
            .set(PARQUET_PAGE_ROW_LIMIT, "100")
            .set(PARQUET_ROW_GROUP_SIZE_BYTES, "80000") // 2 row groups
            .named("page-skipping-test")
            .build()) {
      fileAppender.addAll(allRecords.iterator());
    }
  }

  @Test
  public void testSinglePageMatch() {
    Expression filter =
        Expressions.and(
            Expressions.greaterThanOrEqual("l", 150),
            Expressions.lessThan("l", 200)); // page-1 in row group 0  -> row ranges: [100, 199]

    List<Record> expected = selectRecords(allRecords, Pair.of(100, 200));
    readAndValidate(filter, expected);
  }

  @Test
  public void testMultiplePagesMatch() {
    Expression filter =
        Expressions.or(
            // page-1 in row group 0 -> row ranges: [100, 199]
            Expressions.and(
                Expressions.greaterThanOrEqual("l", 150), Expressions.lessThan("l", 200)),
            // page-3, page-4 in row group 0  -> row ranges[300, 499]
            Expressions.and(
                Expressions.greaterThanOrEqual("l", 350), Expressions.lessThan("l", 450)));

    List<Record> expected = selectRecords(allRecords, Pair.of(100, 200), Pair.of(300, 500));
    readAndValidate(filter, expected);
  }

  @Test
  public void testMultipleRowGroupsMatch() {
    Expression filter =
        Expressions.or(
            // page-1 in row group 0 -> row ranges: [100, 199]
            Expressions.and(
                Expressions.greaterThanOrEqual("l", 150), Expressions.lessThan("l", 200)),
            // page-3, page-4 in row group 0  -> row ranges[300, 499]
            Expressions.and(
                Expressions.greaterThanOrEqual("l", 350), Expressions.lessThan("l", 450)));
    filter =
        Expressions.or(
            filter,
            // firstRowIndex in row group 1 -> 613
            // page-1, page-2 in row group 1 -> row ranges: [100, 299]
            Expressions.and(
                Expressions.greaterThanOrEqual("l", 750), Expressions.lessThan("l", 850)));

    List<Record> expected =
        selectRecords(allRecords, Pair.of(100, 200), Pair.of(300, 500), Pair.of(713, 913));
    readAndValidate(filter, expected);
  }

  @Test
  public void testNoRowsMatch() {
    Expression filter = Expressions.greaterThan("l", 1000);
    readAndValidate(filter, ImmutableList.of());
  }

  @Test
  public void testAllRowsMatch() {
    Expression filter = Expressions.greaterThanOrEqual("l", Long.MIN_VALUE);
    readAndValidate(filter, allRecords);
  }

  @Test
  public void testPageSkippingWithDelete() {
    Schema projectSchema =
        new Schema(
            Stream.concat(DATA_SCHEMA.columns().stream(), Stream.of(MetadataColumns.ROW_POSITION))
                .collect(Collectors.toList()));

    DeleteFilter deleteFilter = mock(DeleteFilter.class);
    when(deleteFilter.requiredSchema()).thenReturn(projectSchema);
    when(deleteFilter.hasPosDeletes()).thenReturn(true);
    when(deleteFilter.eqDeletedRowFilter()).thenReturn(null);
    PositionDeleteIndex deletedRowPos = mock(PositionDeleteIndex.class);

    // pos 180, 800 are deleted in file
    when(deletedRowPos.isDeleted(180L)).thenReturn(true);
    when(deletedRowPos.isDeleted(800L)).thenReturn(true);
    when(deleteFilter.deletedRowPositions()).thenReturn(deletedRowPos);

    Expression filter =
        Expressions.or(
            // page-1 in row group 0 -> row ranges: [100, 199]
            Expressions.and(
                Expressions.greaterThanOrEqual("l", 150), Expressions.lessThan("l", 200)),
            // firstRowIndex in row group 1 -> 613
            // page-1, page-2 in row group 1 -> row ranges: [100, 299]
            Expressions.and(
                Expressions.greaterThanOrEqual("l", 750), Expressions.lessThan("l", 850)));

    List<Record> expected =
        selectRecords(
            allRecords, Pair.of(100, 180), Pair.of(181, 200), Pair.of(713, 800), Pair.of(801, 913));

    readAndValidate(filter, expected, deleteFilter);
  }

  private void readAndValidate(Expression filter, List<Record> expected) {
    Parquet.ReadBuilder builder =
        Parquet.read(Files.localInput(testFile)).project(DATA_SCHEMA).filter(filter);
    CloseableIterable<ColumnarBatch> batches =
        builder
            .createBatchedReaderFunc(
                type ->
                    VectorizedSparkParquetReaders.buildReader(DATA_SCHEMA, type, ImmutableMap.of()))
            .build();
    Iterator<Record> expectedIter = expected.iterator();
    for (ColumnarBatch batch : batches) {
      GenericsHelpers.assertEqualsBatch(
          SUPPORTED_PRIMITIVES, expectedIter, batch, null, batch.numRows());
    }
  }

  private void readAndValidate(
      Expression filter, List<Record> expected, DeleteFilter deleteFilter) {
    Parquet.ReadBuilder builder =
        Parquet.read(Files.localInput(testFile)).project(DATA_SCHEMA).filter(filter);
    builder =
        builder.createBatchedReaderFunc(
            type ->
                VectorizedSparkParquetReaders.buildReader(
                    deleteFilter.requiredSchema(), type, ImmutableMap.of()));
    CloseableIterable<ColumnarBatch> batches =
        BatchReaderUtil.applyDeleteFilter(builder.build(), deleteFilter);

    Iterator<Record> expectedIter = expected.iterator();
    for (ColumnarBatch batch : batches) {
      GenericsHelpers.assertEqualsBatch(
          SUPPORTED_PRIMITIVES, expectedIter, batch, null, batch.numRows());
    }
  }

  private List<Record> selectRecords(List<Record> records, Pair<Integer, Integer>... ranges) {
    return Arrays.stream(ranges)
        .map(range -> records.subList(range.first(), range.second()))
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {}
}
