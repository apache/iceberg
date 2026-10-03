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
package org.apache.iceberg.spark.source;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.BaseScanTaskGroup;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.spark.source.metrics.TaskNumDeletes;
import org.apache.iceberg.spark.source.metrics.TaskNumSplits;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

class TestMergingSortedRowDataReader extends TestBase {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), required(2, "data", Types.StringType.get()));

  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();

  private static final TableIdentifier TABLE_IDENT =
      TableIdentifier.of("default", "test_merging_reader");

  private Table table;

  @TempDir private Path temp;

  @BeforeEach
  void before() {
    table = catalog.createTable(TABLE_IDENT, SCHEMA, SPEC);
    table.replaceSortOrder().asc("id").commit();
  }

  @AfterEach
  void after() {
    catalog.dropTable(TABLE_IDENT);
  }

  @Test
  void mergeTwoSortedFiles() throws IOException {
    DataFile file1 = writeDataFile(record(1, "a"), record(3, "c"), record(5, "e"));
    DataFile file2 = writeDataFile(record(2, "b"), record(4, "d"), record(6, "f"));

    table.newAppend().appendFile(file1).appendFile(file2).commit();

    List<InternalRow> rows = readMerged(table);

    assertThat(extractIds(rows)).containsExactly(1, 2, 3, 4, 5, 6);
  }

  @Test
  void mergeWithDuplicateKeys() throws IOException {
    DataFile file1 = writeDataFile(record(1, "a"), record(2, "b"));
    DataFile file2 = writeDataFile(record(1, "c"), record(2, "d"));
    DataFile file3 = writeDataFile(record(1, "e"), record(3, "f"));

    table.newAppend().appendFile(file1).appendFile(file2).appendFile(file3).commit();

    List<InternalRow> rows = readMerged(table);

    assertThat(extractIds(rows)).containsExactly(1, 1, 1, 2, 2, 3);
  }

  @Test
  void mergeWithCompositeSortKeyBreaksTiesOnSecondColumn() throws IOException {
    table.replaceSortOrder().asc("id").asc("data").commit();

    // the larger data value of each id tie is in the first file, so an id-only comparator would
    // return c, a, d, b
    DataFile file1 = writeDataFile(record(1, "c"), record(2, "d"));
    DataFile file2 = writeDataFile(record(1, "a"), record(2, "b"));

    table.newAppend().appendFile(file1).appendFile(file2).commit();

    List<InternalRow> rows = readMerged(table);

    assertThat(extractIds(rows)).containsExactly(1, 1, 2, 2);
    assertThat(extractData(rows, 1)).containsExactly("a", "c", "b", "d");
  }

  @Test
  void mergeDescendingOrder() throws IOException {
    table.replaceSortOrder().desc("id").commit();

    DataFile file1 = writeDataFile(record(6, "f"), record(4, "d"));
    DataFile file2 = writeDataFile(record(5, "e"), record(3, "c"), record(1, "a"));

    table.newAppend().appendFile(file1).appendFile(file2).commit();

    List<InternalRow> rows = readMerged(table);

    assertThat(extractIds(rows)).containsExactly(6, 5, 4, 3, 1);
  }

  @Test
  void mergeWithNulls() throws IOException {
    // id is required in the base schema; make it optional so rows can carry null sort keys
    table.updateSchema().makeColumnOptional("id").commit();

    DataFile file1 = writeDataFile(nullRecord("x"), record(3, "c"));
    DataFile file2 = writeDataFile(nullRecord("y"), record(1, "a"), record(2, "b"));

    table.newAppend().appendFile(file1).appendFile(file2).commit();

    List<InternalRow> rows = readMerged(table);

    // two null-id rows sort first, then 1, 2, 3
    assertThat(extractIds(rows)).containsExactly(null, null, 1, 2, 3);
  }

  @Test
  void mergeWithNullsLast() throws IOException {
    table.updateSchema().makeColumnOptional("id").commit();
    table.replaceSortOrder().asc("id", NullOrder.NULLS_LAST).commit();

    DataFile file1 = writeDataFile(record(1, "a"), record(3, "c"), nullRecord("x"));
    DataFile file2 = writeDataFile(record(2, "b"), nullRecord("y"));

    table.newAppend().appendFile(file1).appendFile(file2).commit();

    List<InternalRow> rows = readMerged(table);

    assertThat(extractIds(rows)).containsExactly(1, 2, 3, null, null);
  }

  @Test
  void mergeDescendingWithNullsFirst() throws IOException {
    table.updateSchema().makeColumnOptional("id").commit();
    table.replaceSortOrder().desc("id", NullOrder.NULLS_FIRST).commit();

    DataFile file1 = writeDataFile(nullRecord("x"), record(3, "c"), record(1, "a"));
    DataFile file2 = writeDataFile(nullRecord("y"), record(2, "b"));

    table.newAppend().appendFile(file1).appendFile(file2).commit();

    List<InternalRow> rows = readMerged(table);

    assertThat(extractIds(rows)).containsExactly(null, null, 3, 2, 1);
  }

  @Test
  void mergeThreeFiles() throws IOException {
    DataFile file1 = writeDataFile(record(1, "a"), record(4, "d"), record(7, "g"));
    DataFile file2 = writeDataFile(record(2, "b"), record(5, "e"), record(8, "h"));
    DataFile file3 = writeDataFile(record(3, "c"), record(6, "f"), record(9, "i"));

    table.newAppend().appendFile(file1).appendFile(file2).appendFile(file3).commit();

    List<InternalRow> rows = readMerged(table);

    assertThat(extractIds(rows)).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9);
  }

  @Test
  void mergeWithSortKeyNotInProjection() throws IOException {
    DataFile file1 = writeDataFile(record(1, "a"), record(3, "c"), record(5, "e"));
    DataFile file2 = writeDataFile(record(2, "b"), record(4, "d"), record(6, "f"));

    table.newAppend().appendFile(file1).appendFile(file2).commit();

    // Project only "data". The sort key "id" is missing from the projection, so it is added to
    // the read schema for the merge comparator and stripped from the rows returned to Spark.
    Schema dataOnly = table.schema().select("data");
    List<InternalRow> rows = readMerged(table, dataOnly);

    // Rows come back ordered by id even though id is not projected.
    assertThat(extractData(rows, 0)).containsExactly("a", "b", "c", "d", "e", "f");
    // Only the projected column is present in the returned rows.
    assertThat(rows.get(0).numFields()).isEqualTo(1);
  }

  @Test
  void mergeAfterSortOrderEvolution() throws IOException {
    // data runs opposite to id, so merging by the table's new order would reverse the output
    DataFile file1 = writeDataFile(record(1, "f"), record(3, "d"));
    DataFile file2 = writeDataFile(record(2, "e"), record(4, "c"));

    table.newAppend().appendFile(file1).appendFile(file2).commit();

    // files were written sorted by id; the reader merges by the files' sort order, not the table's
    table.replaceSortOrder().asc("data").commit();

    List<InternalRow> rows = readMerged(table);

    assertThat(extractIds(rows)).containsExactly(1, 2, 3, 4);
  }

  @Test
  void mergeFilesWithDifferentWrittenSchemas() throws IOException {
    DataFile file1 = writeDataFile(record(1, "a"), record(3, "c"));

    table.updateSchema().addColumn("extra", Types.IntegerType.get()).commit();

    Record record2 = GenericRecord.create(table.schema());
    record2.setField("id", 2);
    record2.setField("data", "b");
    record2.setField("extra", 20);
    Record record4 = GenericRecord.create(table.schema());
    record4.setField("id", 4);
    record4.setField("data", "d");
    record4.setField("extra", 40);
    DataFile file2 = writeDataFile(record2, record4);

    table.newAppend().appendFile(file1).appendFile(file2).commit();

    List<InternalRow> rows = readMerged(table);

    assertThat(extractIds(rows)).containsExactly(1, 2, 3, 4);
    List<Integer> extra = rows.stream().map(row -> row.isNullAt(2) ? null : row.getInt(2)).toList();
    assertThat(extra).containsExactly(null, 20, null, 40);
  }

  @Test
  void mergeWithStructColumnNotInSortOrder() throws IOException {
    // add a struct column that is not part of the sort order
    table
        .updateSchema()
        .addColumn("location", Types.StructType.of(required(5, "city", Types.StringType.get())))
        .commit();

    DataFile file1 = writeDataFile(structRecord(1, "a", "NYC"), structRecord(3, "c", "SFO"));
    DataFile file2 = writeDataFile(structRecord(2, "b", "LAX"), structRecord(4, "d", "SEA"));

    table.newAppend().appendFile(file1).appendFile(file2).commit();

    // Project the struct but not the sort key, so the merge schema is widened around a struct.
    Schema projection = table.schema().select("location");
    List<InternalRow> rows = readMerged(table, projection);

    assertThat(rows.get(0).numFields()).isEqualTo(1);
    assertThat(rows.stream().map(row -> row.getStruct(0, 1).getUTF8String(0).toString()).toList())
        .containsExactly("NYC", "LAX", "SFO", "SEA");
  }

  @Test
  void mergeWithArrayOfStructsDoesNotCorruptElements() throws IOException {
    Types.StructType element = Types.StructType.of(required(4, "a", Types.IntegerType.get()));
    Schema arrayOfStructs =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "arr", Types.ListType.ofOptional(3, element)));

    // replace data with the array-of-structs column; base is already sorted by id
    table
        .updateSchema()
        .deleteColumn("data")
        .addColumn("arr", Types.ListType.ofOptional(3, element))
        .commit();

    // File1 = [(1,[a=10]), (3,[a=30])], File2 = [(2,[a=20])]. SortedMerge advances file1's reader
    // before returning the row for id=1, so a shallow copy would let id=3's struct clobber id=1's.
    DataFile file1 =
        writeDataFile(
            arrayOfStructsRecord(arrayOfStructs, element, 1, 10),
            arrayOfStructsRecord(arrayOfStructs, element, 3, 30));
    DataFile file2 = writeDataFile(arrayOfStructsRecord(arrayOfStructs, element, 2, 20));

    table.newAppend().appendFile(file1).appendFile(file2).commit();

    List<InternalRow> rows = readMerged(table);

    assertThat(extractIds(rows)).containsExactly(1, 2, 3);
    assertThat(rows.stream().map(row -> row.getArray(1).getStruct(0, 1).getInt(0)).toList())
        .containsExactly(10, 20, 30);
  }

  @Test
  void mergeWithMapOfStructsDoesNotCorruptElements() throws IOException {
    Types.StructType element = Types.StructType.of(required(5, "a", Types.IntegerType.get()));
    Schema mapOfStructs =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(
                2, "m", Types.MapType.ofOptional(3, 4, Types.StringType.get(), element)));

    // replace data with the map-of-structs column; base is already sorted by id
    table
        .updateSchema()
        .deleteColumn("data")
        .addColumn("m", Types.MapType.ofOptional(3, 4, Types.StringType.get(), element))
        .commit();

    DataFile file1 =
        writeDataFile(
            mapOfStructsRecord(mapOfStructs, element, 1, 10),
            mapOfStructsRecord(mapOfStructs, element, 3, 30));
    DataFile file2 = writeDataFile(mapOfStructsRecord(mapOfStructs, element, 2, 20));

    table.newAppend().appendFile(file1).appendFile(file2).commit();

    List<InternalRow> rows = readMerged(table);

    assertThat(extractIds(rows)).containsExactly(1, 2, 3);
    assertThat(
            rows.stream().map(row -> row.getMap(1).valueArray().getStruct(0, 1).getInt(0)).toList())
        .containsExactly(10, 20, 30);
  }

  @Test
  void mergeRejectsStaleSortOrderId() throws IOException {
    SortOrder oldSortOrder = table.sortOrder();

    // file1 keeps the old order id, file2 is written with the evolved one
    DataFile file1 =
        DataFiles.builder(table.spec())
            .copy(writeRecords(record(1, "a"), record(3, "c")))
            .withSortOrder(oldSortOrder)
            .build();

    table.replaceSortOrder().asc("data").commit();
    DataFile file2 = writeDataFile(record(2, "b"), record(4, "d"));

    table.newAppend().appendFile(file1).appendFile(file2).commit();

    assertThatThrownBy(() -> readMerged(table))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot merge files with different sort orders");
  }

  @Test
  void mergeRejectsMissingSortOrderIdOnFirstFile() {
    ScanTaskGroup<FileScanTask> taskGroup =
        taskGroupWithSortOrderIds(null, table.sortOrder().orderId());

    assertThatThrownBy(
            () ->
                new MergingSortedRowDataReader(
                    table, table.io(), taskGroup, table.schema(), true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Merging reader requires sorted files");
  }

  @Test
  void mergeRejectsSortFieldMissingFromSchema() throws IOException {
    table.replaceSortOrder().asc("data").commit();

    DataFile file1 = writeDataFile(record(1, "b"), record(2, "d"));
    DataFile file2 = writeDataFile(record(3, "a"), record(4, "c"));

    table.newAppend().appendFile(file1).appendFile(file2).commit();

    // the files' sort order still references data after the column is dropped
    table.replaceSortOrder().asc("id").commit();
    table.updateSchema().deleteColumn("data").commit();

    BaseScanTaskGroup<FileScanTask> taskGroup = new BaseScanTaskGroup<>(planFiles(table));

    assertThatThrownBy(
            () ->
                new MergingSortedRowDataReader(
                    table, table.io(), taskGroup, table.schema(), true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot find sort field id");
  }

  @Test
  void mergeRejectsSingleTask() throws IOException {
    DataFile file1 = writeDataFile(record(1, "a"), record(3, "c"));

    table.newAppend().appendFile(file1).commit();
    table.refresh();

    BaseScanTaskGroup<FileScanTask> taskGroup = new BaseScanTaskGroup<>(planFiles(table));

    assertThatThrownBy(
            () ->
                new MergingSortedRowDataReader(
                    table, table.io(), taskGroup, table.schema(), true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Merging reader requires at least two tasks, got 1");
  }

  @Test
  void mergeRejectsUnsortedFiles() throws IOException {
    // drop the sort order so files carry the unsorted order id.
    table.replaceSortOrder().commit();

    DataFile file1 = writeDataFile(record(1, "a"), record(3, "c"));
    DataFile file2 = writeDataFile(record(2, "b"), record(4, "d"));

    table.newAppend().appendFile(file1).appendFile(file2).commit();
    table.refresh();

    BaseScanTaskGroup<FileScanTask> taskGroup = new BaseScanTaskGroup<>(planFiles(table));

    assertThatThrownBy(
            () ->
                new MergingSortedRowDataReader(
                    table, table.io(), taskGroup, table.schema(), true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Merging reader requires sorted files");
  }

  @Test
  void mergeWithFileFullyRemovedByDeletesAmongMultipleFiles() throws IOException {
    // With only two files, file1 draining leaves nothing to merge against. With three, file2 and
    // file3 are still genuinely merged against each other after file1 drops out of the heap.
    DataFile file1 = writeDataFile(record(1, "a"), record(4, "d"));
    DataFile file2 = writeDataFile(record(2, "b"), record(5, "e"));
    DataFile file3 = writeDataFile(record(3, "c"), record(6, "f"));

    table.newAppend().appendFile(file1).appendFile(file2).appendFile(file3).commit();

    DeleteFile deleteFile =
        FileHelpers.writeDeleteFile(
                table,
                Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
                Lists.newArrayList(Pair.of(file1.location(), 0L), Pair.of(file1.location(), 1L)),
                TableUtil.formatVersion(table))
            .first();
    table.newRowDelta().addDeletes(deleteFile).commit();

    List<InternalRow> rows = readMerged(table);

    assertThat(extractIds(rows)).containsExactly(2, 3, 5, 6);
  }

  @Test
  void metricsReportTasksAndDeletes() throws IOException {
    DataFile file1 = writeDataFile(record(1, "a"), record(4, "d"));
    DataFile file2 = writeDataFile(record(2, "b"), record(5, "e"));
    DataFile file3 = writeDataFile(record(3, "c"), record(6, "f"));

    table.newAppend().appendFile(file1).appendFile(file2).appendFile(file3).commit();

    DeleteFile deleteFile =
        FileHelpers.writeDeleteFile(
                table,
                Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
                Lists.newArrayList(Pair.of(file1.location(), 0L), Pair.of(file2.location(), 1L)),
                TableUtil.formatVersion(table))
            .first();
    table.newRowDelta().addDeletes(deleteFile).commit();

    BaseScanTaskGroup<FileScanTask> taskGroup = new BaseScanTaskGroup<>(planFiles(table));
    try (MergingSortedRowDataReader reader =
        new MergingSortedRowDataReader(table, table.io(), taskGroup, table.schema(), true, false)) {
      while (reader.next()) {
        // drain
      }

      Map<String, Long> metrics =
          Arrays.stream(reader.currentMetricsValues())
              .collect(Collectors.toMap(CustomTaskMetric::name, CustomTaskMetric::value));
      assertThat(metrics)
          .containsEntry(new TaskNumSplits(0).name(), 3L)
          .containsEntry(new TaskNumDeletes(0).name(), 2L);
    }
  }

  @Test
  void constructorClosesOpenedFilesWhenAnotherFileFailsToOpen() throws IOException {
    // Parquet reads files up to 1 MB into memory and closes the stream at once, so the file left
    // open must be larger than that
    Random random = new Random(42);
    StringBuilder large = new StringBuilder();
    for (int i = 0; i < 2_000_000; i++) {
      large.append((char) ('!' + random.nextInt(94)));
    }

    DataFile file1 = writeDataFile(record(1, large.toString()), record(3, "c"));
    DataFile file2 = writeDataFile(record(2, "b"), record(4, "d"));
    assertThat(file1.fileSizeInBytes()).isGreaterThan(1024 * 1024);

    table.newAppend().appendFile(file1).appendFile(file2).commit();

    List<FileScanTask> tasks = planFiles(table);
    String failing = tasks.get(1).file().location();
    TrackingFileIO io = new TrackingFileIO(table.io(), failing);

    assertThatThrownBy(
            () ->
                new MergingSortedRowDataReader(
                    table, io, new BaseScanTaskGroup<>(tasks), table.schema(), true, false))
        .hasMessageContaining("Failed to open " + failing);

    assertThat(io.opened()).isPositive();
    assertThat(io.closed()).isEqualTo(io.opened());
  }

  @Test
  void mergeWithPositionDeletes() throws IOException {
    // File1: [1, 3, 5], File2: [2, 4, 6]
    DataFile file1 = writeDataFile(record(1, "a"), record(3, "c"), record(5, "e"));
    DataFile file2 = writeDataFile(record(2, "b"), record(4, "d"), record(6, "f"));

    table.newAppend().appendFile(file1).appendFile(file2).commit();

    // Delete the row at position 1 in file1 (value 3).
    DeleteFile deleteFile =
        FileHelpers.writeDeleteFile(
                table,
                Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
                Lists.newArrayList(Pair.of(file1.location(), 1L)),
                TableUtil.formatVersion(table))
            .first();
    table.newRowDelta().addDeletes(deleteFile).commit();

    List<InternalRow> rows = readMerged(table);

    assertThat(extractIds(rows)).containsExactly(1, 2, 4, 5, 6);
  }

  @Test
  void mergeWithSortOrderReferencingSameColumnMultipleTimes() throws IOException {
    table.replaceSortOrder().asc(Expressions.bucket("id", 16)).asc("id").commit();

    List<Record> sorted = recordsInBucketThenIdOrder();
    List<Record> left = Lists.newArrayList();
    List<Record> right = Lists.newArrayList();
    for (int i = 0; i < sorted.size(); i++) {
      (i % 2 == 0 ? left : right).add(sorted.get(i));
    }

    DataFile file1 = writeDataFile(left.toArray(new Record[0]));
    DataFile file2 = writeDataFile(right.toArray(new Record[0]));

    table.newAppend().appendFile(file1).appendFile(file2).commit();

    // the sort key "id" is not in the projection and is referenced by two sort fields.
    Schema dataOnly = table.schema().select("data");
    List<InternalRow> rows = readMerged(table, dataOnly);

    assertThat(rows.get(0).numFields()).isEqualTo(1);
    assertThat(extractData(rows, 0))
        .containsExactlyElementsOf(sorted.stream().map(rec -> (String) rec.get(1)).toList());
  }

  private List<Record> recordsInBucketThenIdOrder() {
    Transform<Integer, Integer> bucket = Transforms.bucket(16);
    Function<Integer, Integer> toBucket = bucket.bind(Types.IntegerType.get())::apply;

    return Stream.of(
            record(1, "a"),
            record(2, "b"),
            record(3, "c"),
            record(4, "d"),
            record(5, "e"),
            record(6, "f"))
        .sorted(
            Comparator.<Record, Integer>comparing(rec -> toBucket.apply((Integer) rec.get(0)))
                .thenComparing(rec -> (Integer) rec.get(0)))
        .toList();
  }

  @Test
  void inputFileBlockHolderReportsCorrectFile() throws IOException {
    DataFile file1 = writeDataFile(record(1, "a"), record(3, "c"));
    DataFile file2 = writeDataFile(record(2, "b"), record(4, "d"));

    table.newAppend().appendFile(file1).appendFile(file2).commit();

    BaseScanTaskGroup<FileScanTask> taskGroup = new BaseScanTaskGroup<>(planFiles(table));

    List<String> paths = Lists.newArrayList();
    List<Long> starts = Lists.newArrayList();
    List<Long> lengths = Lists.newArrayList();
    List<Integer> ids = Lists.newArrayList();
    try (MergingSortedRowDataReader reader =
        new MergingSortedRowDataReader(table, table.io(), taskGroup, table.schema(), true, false)) {
      while (reader.next()) {
        paths.add(InputFileBlockHolder.getInputFilePath().toString());
        starts.add(InputFileBlockHolder.getStartOffset());
        lengths.add(InputFileBlockHolder.getLength());
        ids.add(reader.get().getInt(0));
      }
    }

    assertThat(ids).containsExactly(1, 2, 3, 4);
    assertThat(paths)
        .containsExactly(file1.location(), file2.location(), file1.location(), file2.location());
    assertThat(starts).containsOnly(0L);
    assertThat(lengths)
        .containsExactly(
            file1.fileSizeInBytes(),
            file2.fileSizeInBytes(),
            file1.fileSizeInBytes(),
            file2.fileSizeInBytes());
  }

  @Test
  void mergeSplitsOfOneFile() throws IOException {
    List<Integer> ids = Lists.newArrayList();
    try (MergingSortedRowDataReader reader = splitReader()) {
      while (reader.next()) {
        ids.add(reader.get().getInt(0));
      }
    }

    assertThat(ids).containsExactly(1, 2, 3, 4, 5, 6, 7, 8);
  }

  @Test
  void inputFileBlockHolderReportsSplitOffsets() throws IOException {
    List<Long> splitFileStarts = Lists.newArrayList();
    try (MergingSortedRowDataReader reader = splitReader()) {
      while (reader.next()) {
        // odd ids are in the split file
        if (reader.get().getInt(0) % 2 == 1) {
          splitFileStarts.add(InputFileBlockHolder.getStartOffset());
        }
      }
    }

    assertThat(splitFileStarts).hasSize(4).doesNotHaveDuplicates();
  }

  /** Merges a file split at every row with an unsplit file. */
  private MergingSortedRowDataReader splitReader() throws IOException {
    // one row per row group, so the file can be split between rows
    table
        .updateProperties()
        .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "1")
        .set(TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT, "1")
        .set(TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT, "1")
        .commit();

    DataFile file1 = writeDataFile(record(1, "a"), record(3, "c"), record(5, "e"), record(7, "g"));
    DataFile file2 = writeDataFile(record(2, "b"), record(4, "d"), record(6, "f"), record(8, "h"));

    table.newAppend().appendFile(file1).appendFile(file2).commit();

    List<FileScanTask> tasks = Lists.newArrayList();
    for (FileScanTask task : planFiles(table)) {
      if (task.file().location().equals(file1.location())) {
        task.split(1).forEach(tasks::add);
      } else {
        tasks.add(task);
      }
    }

    assertThat(tasks).hasSize(5);
    return new MergingSortedRowDataReader(
        table, table.io(), new BaseScanTaskGroup<>(tasks), table.schema(), true, false);
  }

  @Test
  void mergeWithNestedSortKeyInProjection() throws IOException {
    BaseScanTaskGroup<FileScanTask> taskGroup = setUpNestedSortKeyTable();

    List<InternalRow> rows = Lists.newArrayList();
    try (MergingSortedRowDataReader reader =
        new MergingSortedRowDataReader(table, table.io(), taskGroup, table.schema(), true, false)) {
      while (reader.next()) {
        rows.add(reader.get().copy());
      }
    }

    assertThat(rows.stream().map(row -> row.getStruct(1, 1).getUTF8String(0).toString()).toList())
        .containsExactly("LA", "NYC");
  }

  @Test
  void mergeRejectsNestedSortKeyNotInProjection() throws IOException {
    BaseScanTaskGroup<FileScanTask> taskGroup = setUpNestedSortKeyTable();

    Schema idOnly = table.schema().select("id");

    assertThatThrownBy(
            () -> new MergingSortedRowDataReader(table, table.io(), taskGroup, idOnly, true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot merge on nested sort key location.city");
  }

  @Test
  void mergeRejectsUuidSortKey() throws IOException {
    table.updateSchema().deleteColumn("data").addColumn("key", Types.UUIDType.get()).commit();
    table.replaceSortOrder().asc("key").commit();

    DataFile file1 = writeDataFile(uuidRecord(1, UUID.nameUUIDFromBytes(new byte[] {1})));
    DataFile file2 = writeDataFile(uuidRecord(2, UUID.nameUUIDFromBytes(new byte[] {2})));
    table.newAppend().appendFile(file1).appendFile(file2).commit();

    BaseScanTaskGroup<FileScanTask> taskGroup = new BaseScanTaskGroup<>(planFiles(table));

    assertThatThrownBy(
            () ->
                new MergingSortedRowDataReader(
                    table, table.io(), taskGroup, table.schema(), true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot merge on UUID sort key key");
  }

  @Test
  void mergeWithBucketedUuidSortKey() throws IOException {
    table.updateSchema().deleteColumn("data").addColumn("key", Types.UUIDType.get()).commit();
    table.replaceSortOrder().asc(Expressions.bucket("key", 8)).commit();

    // keys with distinct buckets, in bucket order; ids follow that order
    Transform<UUID, Integer> bucket = Transforms.bucket(8);
    Function<UUID, Integer> toBucket = bucket.bind(Types.UUIDType.get())::apply;
    Map<Integer, UUID> keyByBucket = Maps.newTreeMap();
    for (byte i = 0; keyByBucket.size() < 4; i++) {
      UUID key = UUID.nameUUIDFromBytes(new byte[] {i});
      keyByBucket.putIfAbsent(toBucket.apply(key), key);
    }

    List<UUID> keys = Lists.newArrayList(keyByBucket.values());
    DataFile file1 = writeDataFile(uuidRecord(1, keys.get(0)), uuidRecord(3, keys.get(2)));
    DataFile file2 = writeDataFile(uuidRecord(2, keys.get(1)), uuidRecord(4, keys.get(3)));
    table.newAppend().appendFile(file1).appendFile(file2).commit();

    List<InternalRow> rows = readMerged(table);

    assertThat(extractIds(rows)).containsExactly(1, 2, 3, 4);
  }

  @Test
  void mergeRejectsFloatingPointSortKeyFollowedByAnotherKey() {
    table.updateSchema().addColumn("score", Types.DoubleType.get()).commit();
    table.replaceSortOrder().asc("score").asc("id").commit();

    int orderId = table.sortOrder().orderId();
    ScanTaskGroup<FileScanTask> taskGroup = taskGroupWithSortOrderIds(orderId, orderId);

    assertThatThrownBy(
            () ->
                new MergingSortedRowDataReader(
                    table, table.io(), taskGroup, table.schema(), true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Cannot merge on floating point sort key score followed by other sort keys");
  }

  @Test
  void mergeWithTrailingFloatingPointSortKey() throws IOException {
    table.updateSchema().addColumn("score", Types.DoubleType.get()).commit();
    table.replaceSortOrder().asc("id").asc("score").commit();

    DataFile file1 = writeDataFile(scoreRecord(1, 2.0), scoreRecord(2, -0.0));
    DataFile file2 = writeDataFile(scoreRecord(1, 1.0), scoreRecord(2, 0.0));
    table.newAppend().appendFile(file1).appendFile(file2).commit();

    List<InternalRow> rows = readMerged(table);

    assertThat(extractIds(rows)).containsExactly(1, 1, 2, 2);
    assertThat(rows.stream().map(row -> row.getDouble(2)).toList())
        .containsExactly(1.0, 2.0, -0.0, 0.0);
  }

  private Record uuidRecord(int id, UUID key) {
    Record record = GenericRecord.create(table.schema());
    record.setField("id", id);
    record.setField("key", key);
    return record;
  }

  private Record scoreRecord(int id, double score) {
    Record record = GenericRecord.create(table.schema());
    record.setField("id", id);
    record.setField("data", "x");
    record.setField("score", score);
    return record;
  }

  private BaseScanTaskGroup<FileScanTask> setUpNestedSortKeyTable() throws IOException {
    table
        .updateSchema()
        .deleteColumn("data")
        .addColumn("location", Types.StructType.of(required(3, "city", Types.StringType.get())))
        .commit();
    table.replaceSortOrder().asc("location.city").commit();

    DataFile file1 = writeDataFile(locationRecord(1, "NYC"));
    DataFile file2 = writeDataFile(locationRecord(2, "LA"));
    table.newAppend().appendFile(file1).appendFile(file2).commit();

    return new BaseScanTaskGroup<>(planFiles(table));
  }

  private Record locationRecord(int id, String city) {
    Types.StructType locationType = table.schema().findField("location").type().asStructType();
    Record location = GenericRecord.create(locationType);
    location.setField("city", city);

    Record record = GenericRecord.create(table.schema());
    record.setField("id", id);
    record.setField("location", location);
    return record;
  }

  private List<InternalRow> readMerged(Table tbl) throws IOException {
    return readMerged(tbl, tbl.schema());
  }

  private List<InternalRow> readMerged(Table tbl, Schema projection) throws IOException {
    List<FileScanTask> fileTasks = planFiles(tbl);
    assertThat(fileTasks).hasSizeGreaterThan(1);

    BaseScanTaskGroup<FileScanTask> taskGroup = new BaseScanTaskGroup<>(fileTasks);

    List<InternalRow> rows = Lists.newArrayList();
    try (MergingSortedRowDataReader reader =
        new MergingSortedRowDataReader(tbl, tbl.io(), taskGroup, projection, true, false)) {
      while (reader.next()) {
        rows.add(reader.get().copy());
      }
    }

    return rows;
  }

  private List<FileScanTask> planFiles(Table tbl) throws IOException {
    tbl.refresh();

    List<FileScanTask> fileTasks = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> tasks = tbl.newScan().planFiles()) {
      tasks.forEach(fileTasks::add);
    }

    return fileTasks;
  }

  @SuppressWarnings("unchecked")
  private ScanTaskGroup<FileScanTask> taskGroupWithSortOrderIds(Integer... sortOrderIds) {
    List<FileScanTask> tasks = Lists.newArrayList();
    for (Integer sortOrderId : sortOrderIds) {
      DataFile file = Mockito.mock(DataFile.class);
      when(file.sortOrderId()).thenReturn(sortOrderId);

      FileScanTask task = Mockito.mock(FileScanTask.class);
      when(task.file()).thenReturn(file);
      tasks.add(task);
    }

    ScanTaskGroup<FileScanTask> taskGroup = Mockito.mock(ScanTaskGroup.class);
    doReturn(tasks).when(taskGroup).tasks();
    return taskGroup;
  }

  private List<Integer> extractIds(List<InternalRow> rows) {
    return rows.stream().map(row -> row.isNullAt(0) ? null : row.getInt(0)).toList();
  }

  private List<String> extractData(List<InternalRow> rows, int ordinal) {
    return rows.stream().map(row -> row.getUTF8String(ordinal).toString()).toList();
  }

  private Record record(int id, String data) {
    GenericRecord record = GenericRecord.create(SCHEMA);
    record.set(0, id);
    record.set(1, data);
    return record;
  }

  private Record structRecord(int id, String data, String city) {
    Types.StructType locationType = table.schema().findField("location").type().asStructType();
    GenericRecord location = GenericRecord.create(locationType);
    location.set(0, city);

    GenericRecord record = GenericRecord.create(table.schema());
    record.set(0, id);
    record.set(1, data);
    record.set(2, location);
    return record;
  }

  private Record arrayOfStructsRecord(
      Schema schema, Types.StructType element, int id, int elementValue) {
    GenericRecord elementRecord = GenericRecord.create(element);
    elementRecord.set(0, elementValue);
    GenericRecord record = GenericRecord.create(schema);
    record.set(0, id);
    record.set(1, Lists.newArrayList(elementRecord));
    return record;
  }

  private Record mapOfStructsRecord(
      Schema schema, Types.StructType element, int id, int elementValue) {
    GenericRecord elementRecord = GenericRecord.create(element);
    elementRecord.set(0, elementValue);
    GenericRecord record = GenericRecord.create(schema);
    record.set(0, id);
    record.set(1, Map.of("k", elementRecord));
    return record;
  }

  private Record nullRecord(String data) {
    Schema nullableSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            required(2, "data", Types.StringType.get()));
    GenericRecord record = GenericRecord.create(nullableSchema);
    record.set(0, null);
    record.set(1, data);
    return record;
  }

  private DataFile writeDataFile(Record... records) throws IOException {
    return DataFiles.builder(table.spec())
        .copy(writeRecords(records))
        .withSortOrder(table.sortOrder())
        .build();
  }

  private DataFile writeRecords(Record... records) throws IOException {
    return FileHelpers.writeDataFile(
        table,
        Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
        Lists.newArrayList(records));
  }

  /** Counts opened and closed streams, and fails to open one location. */
  private static class TrackingFileIO implements FileIO {
    private final FileIO delegate;
    private final String failingLocation;
    private final AtomicInteger opened = new AtomicInteger();
    private final AtomicInteger closed = new AtomicInteger();

    private TrackingFileIO(FileIO delegate, String failingLocation) {
      this.delegate = delegate;
      this.failingLocation = failingLocation;
    }

    private int opened() {
      return opened.get();
    }

    private int closed() {
      return closed.get();
    }

    @Override
    public InputFile newInputFile(String path) {
      return new TrackingInputFile(delegate.newInputFile(path));
    }

    @Override
    public InputFile newInputFile(String path, long length) {
      return new TrackingInputFile(delegate.newInputFile(path, length));
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return delegate.newOutputFile(path);
    }

    @Override
    public void deleteFile(String path) {
      delegate.deleteFile(path);
    }

    private class TrackingInputFile implements InputFile {
      private final InputFile file;

      private TrackingInputFile(InputFile file) {
        this.file = file;
      }

      @Override
      public long getLength() {
        return file.getLength();
      }

      @Override
      public SeekableInputStream newStream() {
        if (file.location().equals(failingLocation)) {
          throw new UncheckedIOException(new IOException("Failed to open " + failingLocation));
        }

        SeekableInputStream stream = file.newStream();
        opened.incrementAndGet();
        return new SeekableInputStream() {
          @Override
          public long getPos() throws IOException {
            return stream.getPos();
          }

          @Override
          public void seek(long newPos) throws IOException {
            stream.seek(newPos);
          }

          @Override
          public int read() throws IOException {
            return stream.read();
          }

          @Override
          public int read(byte[] bytes, int off, int len) throws IOException {
            return stream.read(bytes, off, len);
          }

          @Override
          public void close() throws IOException {
            closed.incrementAndGet();
            stream.close();
          }
        };
      }

      @Override
      public String location() {
        return file.location();
      }

      @Override
      public boolean exists() {
        return file.exists();
      }
    }
  }
}
