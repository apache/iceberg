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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongConsumer;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestEqualityDeleteConverter {

  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "tbl");
  private static final Schema SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "category", Types.StringType.get()),
              Types.NestedField.optional(3, "data", Types.StringType.get())),
          ImmutableSet.of(1));

  /** The change of one row, as a CDC writer would produce it. */
  private enum Op {
    INSERT,
    UPDATE,
    DELETE
  }

  private InMemoryCatalog catalog;
  private List<Integer> equalityFieldIds;

  @BeforeEach
  public void before() {
    catalog = new InMemoryCatalog();
    catalog.initialize("test_catalog", ImmutableMap.of());
    catalog.createNamespace(NAMESPACE);
    equalityFieldIds = ImmutableList.copyOf(SCHEMA.identifierFieldIds());
  }

  @AfterEach
  public void after() throws IOException {
    catalog.close();
  }

  @Test
  public void testConvertsDeletesOfEarlierCommits() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    WriteResult first =
        write(
            table,
            insert(1L, "a", "v1"),
            insert(2L, "a", "v1"),
            insert(3L, "a", "v1"),
            insert(4L, "a", "v1"),
            insert(5L, "a", "v1"));
    commit(table, first.dataFiles(), first.deleteFiles(), ImmutableList.of());
    DataFile firstDataFile = first.dataFiles()[0];
    long firstSnapshotId = table.currentSnapshot().snapshotId();

    WriteResult second = write(table, delete(2L, "a"), update(4L, "a", "v2"));
    assertThat(second.deleteFiles()).hasSize(1);

    EqualityDeleteConverter converter = new EqualityDeleteConverter(table, null);
    EqualityDeleteConverter.Result result = converter.convert(Arrays.asList(second.deleteFiles()));

    assertThat(result.baseSnapshotId()).isEqualTo(firstSnapshotId);
    assertThat(result.rewrittenDvFiles()).isEmpty();
    assertThat(result.dvFiles()).hasSize(1);
    DeleteFile dv = result.dvFiles().get(0);
    assertThat(ContentFileUtil.isDV(dv)).isTrue();
    assertThat(dv.referencedDataFile()).isEqualTo(firstDataFile.location());
    assertThat(dv.recordCount()).isEqualTo(2L);
    assertThat(result.plannedDataFiles()).isEqualTo(1);

    commit(table, second.dataFiles(), result.dvFiles(), result.rewrittenDvFiles());
    assertThat(readRows(table)).containsExactlyInAnyOrder("1|a|v1", "3|a|v1", "4|a|v2", "5|a|v1");

    // a second conversion for the same data file merges the existing deletion vector
    WriteResult third = write(table, delete(1L, "a"));
    result = converter.convert(Arrays.asList(third.deleteFiles()));

    assertThat(result.dvFiles()).hasSize(1);
    assertThat(result.dvFiles().get(0).referencedDataFile()).isEqualTo(firstDataFile.location());
    assertThat(result.dvFiles().get(0).recordCount()).isEqualTo(3L);
    assertThat(result.rewrittenDvFiles()).hasSize(1);
    assertThat(result.rewrittenDvFiles().get(0).location()).isEqualTo(dv.location());
    assertThat(result.rewrittenDvFiles().get(0).contentOffset()).isEqualTo(dv.contentOffset());
    // the data file written by the second batch only holds id 4 and is pruned by its column bounds
    assertThat(result.plannedDataFiles()).isEqualTo(1);

    commit(table, third.dataFiles(), result.dvFiles(), result.rewrittenDvFiles());
    assertThat(readRows(table)).containsExactlyInAnyOrder("3|a|v1", "4|a|v2", "5|a|v1");
  }

  @Test
  public void testPartitionPruning() throws IOException {
    Table table = createTable(PartitionSpec.builderFor(SCHEMA).identity("category").build());
    // the partition column has to be part of the key for the delete to land in its partition
    equalityFieldIds = ImmutableList.of(1, 2);

    WriteResult first =
        write(
            table,
            insert(1L, "a", "v1"),
            insert(2L, "a", "v1"),
            insert(3L, "b", "v1"),
            insert(4L, "b", "v1"));
    assertThat(first.dataFiles()).hasSize(2);
    commit(table, first.dataFiles(), first.deleteFiles(), ImmutableList.of());

    WriteResult second = write(table, delete(1L, "a"));
    EqualityDeleteConverter converter = new EqualityDeleteConverter(table, null);
    EqualityDeleteConverter.Result result = converter.convert(Arrays.asList(second.deleteFiles()));

    // only the data file of partition a is a candidate
    assertThat(result.plannedDataFiles()).isEqualTo(1);
    assertThat(result.dvFiles()).hasSize(1);
    assertThat(result.dvFiles().get(0).recordCount()).isEqualTo(1L);

    commit(table, second.dataFiles(), result.dvFiles(), result.rewrittenDvFiles());
    assertThat(readRows(table)).containsExactlyInAnyOrder("2|a|v1", "3|b|v1", "4|b|v1");
  }

  @Test
  public void testPartitionedDeletesSkipDataFilesOfOtherSpecs() throws IOException {
    Table table = createTable(PartitionSpec.builderFor(SCHEMA).identity("category").build());
    equalityFieldIds = ImmutableList.of(1, 2);

    WriteResult first = write(table, insert(1L, "a", "v1"), insert(2L, "a", "v1"));
    commit(table, first.dataFiles(), first.deleteFiles(), ImmutableList.of());

    table.updateSpec().addField(Expressions.bucket("id", 4)).commit();
    WriteResult second = write(table, insert(3L, "a", "v1"));
    commit(table, second.dataFiles(), second.deleteFiles(), ImmutableList.of());
    long baseSnapshotId = table.currentSnapshot().snapshotId();

    WriteResult third = write(table, delete(1L, "a"), delete(3L, "a"));

    // the equality deletes committed as they are define the expected rows: a delete stored with
    // the new spec does not apply to the data file written with the old spec
    table.manageSnapshots().createBranch("expected", baseSnapshotId).commit();
    RowDelta expected = table.newRowDelta().toBranch("expected");
    Arrays.stream(third.deleteFiles()).forEach(expected::addDeletes);
    expected.commit();
    Set<String> expectedRows = readRows(table, table.snapshot("expected").snapshotId());
    assertThat(expectedRows).containsExactlyInAnyOrder("1|a|v1", "2|a|v1");

    EqualityDeleteConverter converter = new EqualityDeleteConverter(table, null);
    EqualityDeleteConverter.Result result = converter.convert(Arrays.asList(third.deleteFiles()));
    commit(table, third.dataFiles(), result.dvFiles(), result.rewrittenDvFiles());

    assertThat(readRows(table)).isEqualTo(expectedRows);
  }

  @Test
  public void testManyKeys() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());

    List<Object[]> inserts = Lists.newArrayList();
    for (long id = 1; id <= 300; id++) {
      inserts.add(insert(id, "a", "v1"));
    }
    WriteResult first = write(table, inserts.toArray(new Object[0][]));
    commit(table, first.dataFiles(), first.deleteFiles(), ImmutableList.of());

    List<Object[]> deletes = Lists.newArrayList();
    for (long id = 1; id <= 250; id++) {
      deletes.add(delete(id, "a"));
    }
    WriteResult second = write(table, deletes.toArray(new Object[0][]));

    EqualityDeleteConverter converter = new EqualityDeleteConverter(table, null);
    EqualityDeleteConverter.Result result = converter.convert(Arrays.asList(second.deleteFiles()));

    assertThat(result.dvFiles()).hasSize(1);
    assertThat(result.dvFiles().get(0).recordCount()).isEqualTo(250L);

    commit(table, second.dataFiles(), result.dvFiles(), result.rewrittenDvFiles());
    Set<String> rows = readRows(table);
    assertThat(rows).hasSize(50);
    assertThat(rows).contains("251|a|v1", "300|a|v1");
    assertThat(rows).doesNotContain("1|a|v1", "250|a|v1");
  }

  @Test
  public void testClusteredKeysPruneToTheirFiles() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());

    // 200 data files with 50 consecutive ids each
    int fileCount = 200;
    int rowsPerFile = 50;
    List<DataFile> dataFiles = Lists.newArrayList();
    for (int file = 0; file < fileCount; file++) {
      List<Object[]> inserts = Lists.newArrayList();
      for (int row = 0; row < rowsPerFile; row++) {
        inserts.add(insert((long) file * rowsPerFile + row, "a", "v1"));
      }
      WriteResult result = write(table, inserts.toArray(new Object[0][]));
      assertThat(result.dataFiles()).hasSize(1);
      dataFiles.add(result.dataFiles()[0]);
    }
    commit(table, dataFiles.toArray(new DataFile[0]), new DeleteFile[0], ImmutableList.of());

    // 295 hot keys in the last six files and five keys scattered over the table
    List<Object[]> deletes = Lists.newArrayList();
    for (long id = 9700; id < 9995; id++) {
      deletes.add(delete(id, "a"));
    }
    for (long id : new long[] {5, 1005, 3005, 6005, 8005}) {
      deletes.add(delete(id, "a"));
    }
    WriteResult second = write(table, deletes.toArray(new Object[0][]));

    EqualityDeleteConverter converter = new EqualityDeleteConverter(table, null);
    EqualityDeleteConverter.Result result = converter.convert(Arrays.asList(second.deleteFiles()));

    // more keys than one IN list holds, so the keys are matched with several lists: the hot
    // range covers six files and every scattered key stays in its own file
    assertThat(deletes).hasSizeGreaterThan(KeyFilters.IN_PREDICATE_LIMIT);
    assertThat(result.plannedDataFiles()).isEqualTo(11);
    assertThat(result.dvFiles()).hasSize(11);
    assertThat(result.dvFiles().stream().mapToLong(DeleteFile::recordCount).sum()).isEqualTo(300L);

    commit(table, second.dataFiles(), result.dvFiles(), result.rewrittenDvFiles());
    assertThat(readRows(table)).hasSize(fileCount * rowsPerFile - 300);
  }

  @Test
  public void testPartitionIsPinnedWithManyKeys() throws IOException {
    Table table = createTable(PartitionSpec.builderFor(SCHEMA).identity("category").build());

    // three files of 100 ids in each of the partitions a and b, ids overlap across partitions, so
    // the keys, which are ids alone, cannot tell the partitions apart
    List<DataFile> dataFiles = Lists.newArrayList();
    for (String category : new String[] {"a", "b"}) {
      for (int file = 0; file < 3; file++) {
        List<Object[]> inserts = Lists.newArrayList();
        for (int row = 0; row < 100; row++) {
          inserts.add(insert((long) file * 100 + row, category, "v1"));
        }
        WriteResult result = write(table, inserts.toArray(new Object[0][]));
        dataFiles.add(result.dataFiles()[0]);
      }
    }
    commit(table, dataFiles.toArray(new DataFile[0]), new DeleteFile[0], ImmutableList.of());

    List<Object[]> deletes = Lists.newArrayList();
    for (long id = 0; id < 250; id++) {
      deletes.add(delete(id, "a"));
    }
    WriteResult second = write(table, deletes.toArray(new Object[0][]));

    EqualityDeleteConverter converter = new EqualityDeleteConverter(table, null);
    EqualityDeleteConverter.Result result = converter.convert(Arrays.asList(second.deleteFiles()));

    // the id filter alone covers files of both partitions, the partition pins it to a
    assertThat(result.plannedDataFiles()).isEqualTo(3);
    assertThat(result.dvFiles()).hasSize(3);

    commit(table, second.dataFiles(), result.dvFiles(), result.rewrittenDvFiles());
    Set<String> rows = readRows(table);
    assertThat(rows).hasSize(350);
    assertThat(rows).contains("0|b|v1", "250|a|v1").doesNotContain("0|a|v1", "249|a|v1");
  }

  @Test
  public void testTransformPartitionIsPinnedWithManyKeys() throws IOException {
    Table table = createTable(PartitionSpec.builderFor(SCHEMA).bucket("id", 4).build());

    // three batches, each writes one file per bucket
    int rowsPerBatch = 2000;
    List<DataFile> dataFiles = Lists.newArrayList();
    for (int batch = 0; batch < 3; batch++) {
      List<Object[]> inserts = Lists.newArrayList();
      for (long id = (long) batch * rowsPerBatch; id < (long) (batch + 1) * rowsPerBatch; id++) {
        inserts.add(insert(id, "a", "v1"));
      }
      WriteResult result = write(table, inserts.toArray(new Object[0][]));
      assertThat(result.dataFiles()).hasSize(4);
      dataFiles.addAll(Arrays.asList(result.dataFiles()));
    }
    commit(table, dataFiles.toArray(new DataFile[0]), new DeleteFile[0], ImmutableList.of());

    // keys of bucket 0 spread over all three batches, more than IN lists may hold, so they are
    // matched with the bounds of the ids, which bucket(4, id) cannot project: only the pinned
    // bucket prunes the files of the other buckets
    Function<Object, Integer> bucket = Transforms.bucket(4).bind(Types.LongType.get());
    List<Long> bucketZeroIds = Lists.newArrayList();
    for (long id = 0; id < 3L * rowsPerBatch; id++) {
      if (bucket.apply(id) == 0) {
        bucketZeroIds.add(id);
      }
    }
    int deletedKeys = 1200;
    List<Object[]> deletes = Lists.newArrayList();
    for (int i = 0; i < deletedKeys; i++) {
      deletes.add(delete(bucketZeroIds.get(i * bucketZeroIds.size() / deletedKeys), "a"));
    }
    WriteResult second = write(table, deletes.toArray(new Object[0][]));
    assertThat(second.deleteFiles()).hasSize(1);

    EqualityDeleteConverter converter = new EqualityDeleteConverter(table, null);
    EqualityDeleteConverter.Result result = converter.convert(Arrays.asList(second.deleteFiles()));

    // the pinned bucket keeps only the three files of bucket 0
    assertThat(result.plannedDataFiles()).isEqualTo(3);
    assertThat(result.dvFiles()).hasSize(3);
    assertThat(result.dvFiles().stream().mapToLong(DeleteFile::recordCount).sum())
        .isEqualTo(deletedKeys);

    commit(table, second.dataFiles(), result.dvFiles(), result.rewrittenDvFiles());
    assertThat(readRows(table)).hasSize(3 * rowsPerBatch - deletedKeys);
  }

  @Test
  public void testScatteredKeysAreMatchedWithLists() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());

    // 200 data files with 50 consecutive ids each
    int fileCount = 200;
    int rowsPerFile = 50;
    List<DataFile> dataFiles = Lists.newArrayList();
    for (int file = 0; file < fileCount; file++) {
      List<Object[]> inserts = Lists.newArrayList();
      for (int row = 0; row < rowsPerFile; row++) {
        inserts.add(insert((long) file * rowsPerFile + row, "a", "v1"));
      }
      dataFiles.add(write(table, inserts.toArray(new Object[0][])).dataFiles()[0]);
    }
    commit(table, dataFiles.toArray(new DataFile[0]), new DeleteFile[0], ImmutableList.of());

    // 300 hot keys in the last six files plus one key in each of the first 150 files: more
    // scattered keys than value ranges could isolate, so the keys are matched with IN lists
    List<Object[]> deletes = Lists.newArrayList();
    for (long id = 9700; id < 10000; id++) {
      deletes.add(delete(id, "a"));
    }
    for (int file = 0; file < 150; file++) {
      deletes.add(delete((long) file * rowsPerFile + 7, "a"));
    }
    WriteResult second = write(table, deletes.toArray(new Object[0][]));

    EqualityDeleteConverter converter = new EqualityDeleteConverter(table, null);
    EqualityDeleteConverter.Result result = converter.convert(Arrays.asList(second.deleteFiles()));

    // exactly the files that hold a deleted key are read: 6 hot files and 150 scattered files
    assertThat(result.plannedDataFiles()).isEqualTo(156);
    assertThat(result.dvFiles()).hasSize(156);
    assertThat(result.dvFiles().stream().mapToLong(DeleteFile::recordCount).sum()).isEqualTo(450L);

    commit(table, second.dataFiles(), result.dvFiles(), result.rewrittenDvFiles());
    assertThat(readRows(table)).hasSize(fileCount * rowsPerFile - 450);
  }

  @Test
  public void testKeysAboveTheListLimitAreMatchedWithBounds() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());

    // 30 data files with 100 consecutive ids each
    int fileCount = 30;
    int rowsPerFile = 100;
    List<DataFile> dataFiles = Lists.newArrayList();
    for (int file = 0; file < fileCount; file++) {
      List<Object[]> inserts = Lists.newArrayList();
      for (int row = 0; row < rowsPerFile; row++) {
        inserts.add(insert((long) file * rowsPerFile + row, "a", "v1"));
      }
      dataFiles.add(write(table, inserts.toArray(new Object[0][])).dataFiles()[0]);
    }
    commit(table, dataFiles.toArray(new DataFile[0]), new DeleteFile[0], ImmutableList.of());

    // two clusters of 600 keys at both ends of the table, more keys than IN lists may hold
    long rowCount = (long) fileCount * rowsPerFile;
    Set<Long> deletedIds = Sets.newHashSet();
    for (long id = 0; id < 600; id++) {
      deletedIds.add(id);
      deletedIds.add(rowCount - 1 - id);
    }
    List<Object[]> deletes = Lists.newArrayList();
    deletedIds.forEach(id -> deletes.add(delete(id, "a")));
    WriteResult second = write(table, deletes.toArray(new Object[0][]));

    EqualityDeleteConverter.Result result =
        new EqualityDeleteConverter(table, null).convert(Arrays.asList(second.deleteFiles()));

    // the bounds of the two clusters cover the files between them too
    assertThat(result.plannedDataFiles()).isEqualTo(fileCount);

    commit(table, second.dataFiles(), result.dvFiles(), result.rewrittenDvFiles());
    Set<String> expectedRows = Sets.newHashSet();
    for (long id = 0; id < rowCount; id++) {
      if (!deletedIds.contains(id)) {
        expectedRows.add(id + "|a|v1");
      }
    }
    assertThat(readRows(table)).isEqualTo(expectedRows);
  }

  @Test
  public void testCustomResolver() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    WriteResult first = write(table, insert(1L, "a", "v1"), insert(2L, "a", "v1"));
    commit(table, first.dataFiles(), first.deleteFiles(), ImmutableList.of());
    DataFile dataFile = first.dataFiles()[0];

    WriteResult second = write(table, delete(2L, "a"));

    // a resolver that knows the position of the row without reading the data file
    KeyPositionResolver resolver =
        (base, keySchema, keys, deleteSpec, deletePartition) -> {
          assertThat(keys).hasSize(1);
          KeyPositionResolver.FileMatches matches =
              new KeyPositionResolver.FileMatches() {
                @Override
                public String path() {
                  return dataFile.location();
                }

                @Override
                public PartitionSpec spec() {
                  return table.spec();
                }

                @Override
                public StructLike partition() {
                  return dataFile.partition();
                }

                @Override
                public PositionDeleteIndex existingDeletes() {
                  return null;
                }

                @Override
                public void forEachPosition(LongConsumer consumer) {
                  consumer.accept(1L);
                }
              };
          return new KeyPositionResolver.Resolution() {
            @Override
            public Collection<KeyPositionResolver.FileMatches> matches() {
              return ImmutableList.of(matches);
            }

            @Override
            public int plannedDataFiles() {
              return 0;
            }
          };
        };

    EqualityDeleteConverter converter = new EqualityDeleteConverter(table, null, resolver);
    EqualityDeleteConverter.Result result = converter.convert(Arrays.asList(second.deleteFiles()));

    assertThat(result.plannedDataFiles()).isEqualTo(0);
    assertThat(result.dvFiles()).hasSize(1);
    assertThat(result.dvFiles().get(0).referencedDataFile()).isEqualTo(dataFile.location());
    assertThat(result.dvFiles().get(0).recordCount()).isEqualTo(1L);

    commit(table, second.dataFiles(), result.dvFiles(), result.rewrittenDvFiles());
    assertThat(readRows(table)).containsExactly("1|a|v1");
  }

  @Test
  public void testConvertOnBranch() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    WriteResult first = write(table, insert(1L, "a", "v1"), insert(2L, "a", "v1"));
    RowDelta rowDelta = table.newRowDelta().toBranch("staging");
    Arrays.stream(first.dataFiles()).forEach(rowDelta::addRows);
    rowDelta.commit();

    WriteResult second = write(table, delete(1L, "a"));

    // main has no snapshot, nothing to resolve against
    EqualityDeleteConverter.Result onMain =
        new EqualityDeleteConverter(table, null).convert(Arrays.asList(second.deleteFiles()));
    assertThat(onMain.baseSnapshotId()).isNull();
    assertThat(onMain.dvFiles()).isEmpty();

    EqualityDeleteConverter.Result onBranch =
        new EqualityDeleteConverter(table, "staging").convert(Arrays.asList(second.deleteFiles()));
    assertThat(onBranch.baseSnapshotId()).isEqualTo(table.snapshot("staging").snapshotId());
    assertThat(onBranch.dvFiles()).hasSize(1);
    assertThat(onBranch.dvFiles().get(0).recordCount()).isEqualTo(1L);
  }

  @Test
  public void testConvertOnBranchThatDoesNotExistYet() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    WriteResult first = write(table, insert(1L, "a", "v1"), insert(2L, "a", "v1"));
    commit(table, first.dataFiles(), first.deleteFiles(), ImmutableList.of());
    WriteResult second = write(table, delete(1L, "a"), insert(3L, "a", "v1"));

    // a commit creating the branch builds on main, so the deletes are resolved against main
    EqualityDeleteConverter.Result result =
        new EqualityDeleteConverter(table, "staging").convert(Arrays.asList(second.deleteFiles()));
    assertThat(result.baseSnapshotId()).isEqualTo(table.currentSnapshot().snapshotId());
    assertThat(result.dvFiles()).hasSize(1);

    RowDelta rowDelta =
        table
            .newRowDelta()
            .toBranch("staging")
            .validateFromSnapshot(result.baseSnapshotId())
            .conflictDetectionFilter(result.conflictFilter())
            .validateNoConflictingDataFiles()
            .validateNoConflictingDeleteFiles();
    Arrays.stream(second.dataFiles()).forEach(rowDelta::addRows);
    result.dvFiles().forEach(rowDelta::addDeletes);
    rowDelta.commit();

    assertThat(readRows(table, table.snapshot("staging").snapshotId()))
        .containsExactlyInAnyOrder("2|a|v1", "3|a|v1");
  }

  @Test
  public void testConflictFilterCoversKeysWithoutSnapshot() {
    Table table = createTable(PartitionSpec.unpartitioned());
    WriteResult deletes = write(table, delete(1L, "a"));

    EqualityDeleteConverter.Result result =
        new EqualityDeleteConverter(table, null).convert(Arrays.asList(deletes.deleteFiles()));
    assertThat(result.baseSnapshotId()).isNull();
    assertThat(result.dvFiles()).isEmpty();

    // another writer makes the first commit, with a row the equality delete would have removed
    WriteResult concurrent = write(table, insert(1L, "a", "v1"));
    table.newAppend().appendFile(concurrent.dataFiles()[0]).commit();

    RowDelta rowDelta =
        table
            .newRowDelta()
            .conflictDetectionFilter(result.conflictFilter())
            .validateNoConflictingDataFiles();
    assertThatThrownBy(rowDelta::commit)
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Found conflicting files that can contain records matching");
  }

  @Test
  public void testRejectsNonEqualityDeletes() {
    Table table = createTable(PartitionSpec.unpartitioned());
    WriteResult first = write(table, insert(1L, "a", "v1"));
    commit(table, first.dataFiles(), first.deleteFiles(), ImmutableList.of());

    DeleteFile dv =
        FileMetadata.deleteFileBuilder(table.spec())
            .ofPositionDeletes()
            .withFormat(FileFormat.PUFFIN)
            .withPath("/path/to/dv.puffin")
            .withFileSizeInBytes(10L)
            .withRecordCount(1L)
            .withReferencedDataFile(first.dataFiles()[0].location())
            .withContentOffset(4L)
            .withContentSizeInBytes(6L)
            .build();

    assertThatThrownBy(() -> new EqualityDeleteConverter(table, null).convert(ImmutableList.of(dv)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Not an equality delete file: /path/to/dv.puffin");
  }

  @Test
  public void testMergesPositionDeletesFromFormatVersion2() throws IOException {
    Table table =
        catalog.createTable(
            TABLE_IDENTIFIER,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of("format-version", "2"));
    DataFile first =
        write(
                table,
                insert(1L, "a", "v1"),
                insert(2L, "a", "v1"),
                insert(3L, "a", "v1"),
                insert(4L, "a", "v1"))
            .dataFiles()[0];
    DataFile second = write(table, insert(5L, "a", "v1"), insert(6L, "a", "v1")).dataFiles()[0];

    // a file-scoped delete of id 1 and a partition-scoped delete of ids 2 and 5
    DeleteFile fileScoped =
        writePositionDeletes(table, ImmutableList.of(Pair.of(first.location(), 0L)));
    DeleteFile partitionScoped =
        writePositionDeletes(
            table, ImmutableList.of(Pair.of(first.location(), 1L), Pair.of(second.location(), 0L)));
    assertThat(ContentFileUtil.isFileScoped(fileScoped)).isTrue();
    assertThat(ContentFileUtil.isFileScoped(partitionScoped)).isFalse();
    table
        .newRowDelta()
        .addRows(first)
        .addRows(second)
        .addDeletes(fileScoped)
        .addDeletes(partitionScoped)
        .commit();
    table.updateProperties().set("format-version", "3").commit();

    WriteResult deletes = write(table, delete(3L, "a"));
    EqualityDeleteConverter.Result result =
        new EqualityDeleteConverter(table, null).convert(Arrays.asList(deletes.deleteFiles()));

    // the new vector carries both older deletes of the first file, but only the file-scoped one
    // can be removed from the table
    assertThat(result.dvFiles()).hasSize(1);
    assertThat(result.dvFiles().get(0).referencedDataFile()).isEqualTo(first.location());
    assertThat(result.dvFiles().get(0).recordCount()).isEqualTo(3L);
    assertThat(result.rewrittenDvFiles())
        .extracting(DeleteFile::location)
        .containsExactly(fileScoped.location());

    commit(table, deletes.dataFiles(), result.dvFiles(), result.rewrittenDvFiles());
    // the partition-scoped delete still applies to the second data file
    assertThat(readRows(table)).containsExactlyInAnyOrder("4|a|v1", "6|a|v1");
  }

  @Test
  public void testRejectsFormatVersion2() {
    Table table =
        catalog.createTable(
            TABLE_IDENTIFIER,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of("format-version", "2"));

    assertThatThrownBy(() -> new EqualityDeleteConverter(table, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot convert equality deletes of table test_catalog.db.tbl: deletion vectors "
                + "require format version 3, found 2");
  }

  private Table createTable(PartitionSpec spec) {
    return catalog.createTable(
        TABLE_IDENTIFIER, SCHEMA, spec, ImmutableMap.of("format-version", "3"));
  }

  private static Object[] insert(Long id, String category, String data) {
    return new Object[] {Op.INSERT, row(id, category, data)};
  }

  private static Object[] update(Long id, String category, String data) {
    return new Object[] {Op.UPDATE, row(id, category, data)};
  }

  private static Object[] delete(Long id, String category) {
    return new Object[] {Op.DELETE, row(id, category, null)};
  }

  private static Record row(Long id, String category, String data) {
    Record row = GenericRecord.create(SCHEMA);
    row.setField("id", id);
    row.setField("category", category);
    row.setField("data", data);
    return row;
  }

  /**
   * Writes the changes the way a CDC writer does: inserts and the new version of updates go to a
   * data file, updates and deletes write the key to an equality delete file, one file of each kind
   * per partition.
   */
  private WriteResult write(Table table, Object[]... operations) {
    Schema keySchema = TypeUtil.select(table.schema(), Sets.newHashSet(equalityFieldIds));
    FileWriterFactory<Record> writerFactory =
        new GenericFileWriterFactory.Builder(table)
            .dataSchema(table.schema())
            .dataFileFormat(FileFormat.PARQUET)
            .deleteFileFormat(FileFormat.PARQUET)
            .equalityFieldIds(equalityFieldIds.stream().mapToInt(Integer::intValue).toArray())
            .equalityDeleteRowSchema(keySchema)
            .build();
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, System.nanoTime())
            .format(FileFormat.PARQUET)
            .build();
    PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());
    InternalRecordWrapper wrapper = new InternalRecordWrapper(table.schema().asStruct());

    Map<String, StructLike> partitions = Maps.newHashMap();
    Map<String, DataWriter<Record>> dataWriters = Maps.newHashMap();
    Map<String, EqualityDeleteWriter<Record>> deleteWriters = Maps.newHashMap();
    WriteResult.Builder result = WriteResult.builder();
    try {
      for (Object[] operation : operations) {
        Record row = (Record) operation[1];
        StructLike partition = null;
        String path = "";
        if (table.spec().isPartitioned()) {
          partitionKey.partition(wrapper.wrap(row));
          partition = partitionKey.copy();
          path = partitionKey.toPath();
        }
        partitions.put(path, partition);

        Op op = (Op) operation[0];
        if (op != Op.INSERT) {
          Record key = GenericRecord.create(keySchema);
          for (Types.NestedField field : keySchema.columns()) {
            key.setField(field.name(), row.getField(field.name()));
          }
          deleteWriters
              .computeIfAbsent(
                  path,
                  ignored ->
                      writerFactory.newEqualityDeleteWriter(
                          fileFactory.newOutputFile(table.spec(), partitions.get(ignored)),
                          table.spec(),
                          partitions.get(ignored)))
              .write(key);
        }
        if (op != Op.DELETE) {
          dataWriters
              .computeIfAbsent(
                  path,
                  ignored ->
                      writerFactory.newDataWriter(
                          fileFactory.newOutputFile(table.spec(), partitions.get(ignored)),
                          table.spec(),
                          partitions.get(ignored)))
              .write(row);
        }
      }

      for (DataWriter<Record> writer : dataWriters.values()) {
        writer.close();
        result.addDataFiles(writer.toDataFile());
      }
      for (EqualityDeleteWriter<Record> writer : deleteWriters.values()) {
        writer.close();
        result.addDeleteFiles(writer.toDeleteFile());
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return result.build();
  }

  private static DeleteFile writePositionDeletes(
      Table table, List<Pair<String, Long>> pathsAndPositions) {
    List<Pair<String, Long>> sorted = Lists.newArrayList(pathsAndPositions);
    sorted.sort(
        Comparator.comparing((Pair<String, Long> delete) -> delete.first())
            .thenComparing(Pair::second));

    FileWriterFactory<Record> writerFactory =
        new GenericFileWriterFactory.Builder(table).deleteFileFormat(FileFormat.PARQUET).build();
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, System.nanoTime())
            .format(FileFormat.PARQUET)
            .build();
    PositionDeleteWriter<Record> writer =
        writerFactory.newPositionDeleteWriter(fileFactory.newOutputFile(), table.spec(), null);
    PositionDelete<Record> positionDelete = PositionDelete.create();
    try (PositionDeleteWriter<Record> closeable = writer) {
      for (Pair<String, Long> delete : sorted) {
        closeable.write(positionDelete.set(delete.first(), delete.second()));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return writer.toDeleteFile();
  }

  private static void commit(
      Table table,
      DataFile[] dataFiles,
      Iterable<DeleteFile> addedDeletes,
      Iterable<DeleteFile> removedDeletes) {
    RowDelta rowDelta = table.newRowDelta();
    if (table.currentSnapshot() != null) {
      // replacing a deletion vector requires the starting snapshot, like the coordinator sets it
      rowDelta.validateFromSnapshot(table.currentSnapshot().snapshotId());
    }
    Arrays.stream(dataFiles).forEach(rowDelta::addRows);
    addedDeletes.forEach(rowDelta::addDeletes);
    removedDeletes.forEach(rowDelta::removeDeletes);
    rowDelta.commit();
  }

  private static void commit(
      Table table,
      DataFile[] dataFiles,
      DeleteFile[] addedDeletes,
      Iterable<DeleteFile> removedDeletes) {
    commit(table, dataFiles, Arrays.asList(addedDeletes), removedDeletes);
  }

  private static Set<String> readRows(Table table) {
    return readRows(table, table.currentSnapshot().snapshotId());
  }

  private static Set<String> readRows(Table table, long snapshotId) {
    Set<String> rows = Sets.newHashSet();
    try (CloseableIterable<Record> records =
        IcebergGenerics.read(table).useSnapshot(snapshotId).build()) {
      for (Record record : records) {
        rows.add(
            record.getField("id")
                + "|"
                + record.getField("category")
                + "|"
                + record.getField("data"));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return rows;
  }
}
