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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.util.SerializableSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Covers what the DV-only task writer reports for one checkpoint, before anything consumes it. */
class TestDvOnlyDeltaWriter {

  @TempDir protected File temporaryFolder;

  private Table table;

  @BeforeEach
  void before() throws IOException {
    File folder = Files.createTempDirectory(temporaryFolder.toPath(), "junit").toFile();
    table =
        SimpleDataUtil.createTable(
            folder.getAbsolutePath(),
            ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name()),
            false);
  }

  @Test
  void writesADeletionVectorForARowDeletedInTheSameCheckpoint() throws IOException {
    TaskWriter<RowData> writer = dvOnlyWriter(false);
    writer.write(row(1, "a"));
    writer.write(row(2, "b"));
    writer.write(deleteOf(1, "a"));

    WriteResult written = writer.complete();

    assertThat(written.dataFiles()).hasSize(1);
    assertThat(written.deleteFiles()).hasSize(1);
    DeleteFile vector = written.deleteFiles()[0];
    assertThat(vector.referencedDataFile()).isEqualTo(written.dataFiles()[0].location());
    assertThat(vector.recordCount()).isEqualTo(1L);

    // The deleted row is not reported as live, and nothing is left for the index downstream.
    assertThat(liveRows(writer))
        .singleElement()
        .satisfies(
            entry -> {
              assertThat(entry.position().dataFilePath())
                  .isEqualTo(written.dataFiles()[0].location());
              assertThat(entry.position().position()).isEqualTo(1L);
            });
    assertThat(deleteKeys(writer)).isEmpty();
  }

  @Test
  void reportsADeleteOfAnUnknownKeyInsteadOfWritingADeletionVector() throws IOException {
    TaskWriter<RowData> writer = dvOnlyWriter(false);
    writer.write(row(1, "a"));
    writer.write(deleteOf(9, "unknown"));

    WriteResult written = writer.complete();

    assertThat(written.deleteFiles()).isEmpty();
    assertThat(liveRows(writer)).hasSize(1);
    assertThat(deleteKeys(writer))
        .singleElement()
        .satisfies(key -> assertThat(key.data()).isNotEmpty());
  }

  @Test
  void reportsADeleteKeyForEveryRowWhenUpserting() throws IOException {
    TaskWriter<RowData> writer = dvOnlyWriter(true);
    writer.write(row(1, "a"));
    writer.write(row(2, "b"));

    writer.complete();

    // Upserting has to clear whatever an earlier checkpoint committed for the same key, so every
    // row comes with a delete key as well.
    assertThat(deleteKeys(writer)).hasSize(2);
    assertThat(liveRows(writer)).hasSize(2);
  }

  @Test
  void reportsAKeyDeletedTwiceOnce() throws IOException {
    TaskWriter<RowData> writer = dvOnlyWriter(false);
    writer.write(deleteOf(5, "x"));
    writer.write(deleteOf(5, "y"));

    writer.complete();

    assertThat(deleteKeys(writer)).hasSize(1);
  }

  @Test
  void keepsOnlyTheLastRowOfAKeyWithinTheCheckpoint() throws IOException {
    TaskWriter<RowData> writer = dvOnlyWriter(false);
    writer.write(row(1, "a"));
    writer.write(row(1, "b"));

    WriteResult written = writer.complete();

    assertThat(liveRows(writer))
        .singleElement()
        .satisfies(entry -> assertThat(entry.position().position()).isEqualTo(1L));
    assertThat(written.deleteFiles()).hasSize(1);
    assertThat(written.deleteFiles()[0].recordCount()).isEqualTo(1L);
  }

  @Test
  void writesNoDeletionVectorWhenNothingIsDeleted() throws IOException {
    TaskWriter<RowData> writer = dvOnlyWriter(false);
    writer.write(row(1, "a"));
    writer.write(row(2, "b"));

    WriteResult written = writer.complete();

    assertThat(written.deleteFiles()).isEmpty();
    assertThat(liveRows(writer)).hasSize(2);
    assertThat(deleteKeys(writer)).isEmpty();
  }

  @Test
  void reportsThePositionsOfTheRowsItWrote() throws IOException {
    TaskWriter<RowData> writer = dvOnlyWriter(false);
    for (int id = 1; id <= 3; id++) {
      writer.write(row(id, "v" + id));
    }

    writer.complete();

    assertThat(liveRows(writer))
        .extracting(entry -> entry.position().position())
        .containsExactlyInAnyOrder(0L, 1L, 2L);
    assertThat(liveRows(writer))
        .allSatisfy(
            entry -> {
              assertThat(entry.key()).isNotNull();
              assertThat(entry.position().specId()).isEqualTo(table.spec().specId());
              assertThat(entry.position().dataSequenceNumber())
                  .isEqualTo(PkIndexEntry.UNKNOWN_SEQUENCE);
            });
  }

  @Test
  void reportsThePositionsOfRowsWrittenToDifferentFiles() throws IOException {
    // A file size of zero rolls to a new file every ROWS_DIVISOR records.
    int rows = 1_001;
    TaskWriter<RowData> writer = dvOnlyWriter(false, 0L);
    for (int id = 1; id <= rows; id++) {
      writer.write(row(id, "v" + id));
    }

    WriteResult written = writer.complete();

    assertThat(written.dataFiles()).hasSize(2);
    assertThat(written.dataFiles()[0].location()).isNotEqualTo(written.dataFiles()[1].location());
    assertThat(liveRows(writer)).hasSize(rows);
    assertThat(liveRows(writer))
        .extracting(entry -> entry.position().dataFilePath())
        .containsOnly(written.dataFiles()[0].location(), written.dataFiles()[1].location());
  }

  @Test
  void splitsTheReportedPayloadIntoBoundedResults() throws IOException {
    TaskWriter<RowData> writer = dvOnlyWriter(true);
    for (int id = 1; id <= 3; id++) {
      writer.write(row(id, "v" + id));
    }

    WriteResult written = writer.complete();
    List<SinkWriteResult> results = IcebergSinkWriter.toSinkWriteResults(writer, written, 2);

    assertThat(results).hasSize(2);
    assertThat(results.stream().mapToInt(result -> result.deleteKeys().size()).sum()).isEqualTo(3);
    assertThat(results.stream().mapToInt(result -> result.liveRows().size()).sum()).isEqualTo(3);
  }

  @Test
  void commitsTheDataFilesOfTheWriterOnlyOnce() throws IOException {
    TaskWriter<RowData> writer = dvOnlyWriter(true);
    for (int id = 1; id <= 3; id++) {
      writer.write(row(id, "v" + id));
    }

    WriteResult written = writer.complete();
    List<SinkWriteResult> results = IcebergSinkWriter.toSinkWriteResults(writer, written, 2);

    assertThat(written.dataFiles()).isNotEmpty();
    assertThat(results.get(0).writeResult().dataFiles()).hasSameSizeAs(written.dataFiles());
    assertThat(results.subList(1, results.size()))
        .allSatisfy(result -> assertThat(result.writeResult().dataFiles()).isEmpty());
  }

  @Test
  void returnsASingleResultForTheDefaultWritePath() {
    TaskWriter<RowData> writer = defaultWriter();
    WriteResult written = WriteResult.builder().build();

    List<SinkWriteResult> results = IcebergSinkWriter.toSinkWriteResults(writer, written, 2);

    assertThat(results)
        .singleElement()
        .satisfies(
            result -> {
              assertThat(result.writeResult()).isSameAs(written);
              assertThat(result.deleteKeys()).isEmpty();
              assertThat(result.liveRows()).isEmpty();
            });
  }

  /**
   * The DV-only writer handles row kinds on its own, so this keeps it in step with the delta writer
   * of the default write path: whatever that writer turns into a deletion vector has to become one
   * here too, and whatever it writes as an equality delete has to be reported as a delete key.
   *
   * <p>The sequences leave out a key that is deleted twice without being written, since the
   * equality delete file repeats it while the reported delete keys do not.
   */
  @ParameterizedTest(name = "{0}, upsert = {2}")
  @MethodSource("changeSequences")
  void treatsRowKindsLikeTheDefaultDeltaWriter(String name, List<RowData> changes, boolean upsert)
      throws IOException {
    Table referenceTable = versionThreeTable();
    TaskWriter<RowData> reference = defaultDeltaWriter(referenceTable, upsert);
    TaskWriter<RowData> dvOnly = dvOnlyWriter(upsert);
    for (RowData change : changes) {
      reference.write(change);
      dvOnly.write(change);
    }

    WriteResult expected = reference.complete();
    WriteResult actual = dvOnly.complete();

    assertThat(recordCount(actual, FileContent.POSITION_DELETES))
        .isEqualTo(recordCount(expected, FileContent.POSITION_DELETES));
    assertThat((long) deleteKeys(dvOnly).size())
        .isEqualTo(recordCount(expected, FileContent.EQUALITY_DELETES));
    assertThat(actual.dataFiles()).hasSameSizeAs(expected.dataFiles());
  }

  private static Stream<Arguments> changeSequences() {
    return Stream.of(false, true)
        .flatMap(
            upsert ->
                Stream.of(
                    Arguments.of(
                        "insert then delete", List.of(row(1, "a"), deleteOf(1, "a")), upsert),
                    Arguments.of("insert twice", List.of(row(1, "a"), row(1, "b")), upsert),
                    Arguments.of("delete only", List.of(deleteOf(1, "a")), upsert),
                    Arguments.of(
                        "update of a row written earlier",
                        List.of(
                            row(1, "a"),
                            kind(RowKind.UPDATE_BEFORE, 1, "a"),
                            kind(RowKind.UPDATE_AFTER, 1, "b")),
                        upsert),
                    Arguments.of(
                        "update of a row from an earlier checkpoint",
                        List.of(
                            kind(RowKind.UPDATE_BEFORE, 1, "a"),
                            kind(RowKind.UPDATE_AFTER, 1, "b")),
                        upsert),
                    Arguments.of(
                        "several keys",
                        List.of(row(1, "a"), deleteOf(2, "b"), row(3, "c"), deleteOf(1, "a")),
                        upsert)));
  }

  private static long recordCount(WriteResult result, FileContent content) {
    return Stream.of(result.deleteFiles())
        .filter(file -> file.content() == content)
        .mapToLong(DeleteFile::recordCount)
        .sum();
  }

  private Table versionThreeTable() throws IOException {
    File folder = Files.createTempDirectory(temporaryFolder.toPath(), "junit").toFile();
    return SimpleDataUtil.createTable(
        folder.getAbsolutePath(),
        ImmutableMap.of(
            TableProperties.DEFAULT_FILE_FORMAT,
            FileFormat.PARQUET.name(),
            TableProperties.FORMAT_VERSION,
            "3"),
        false);
  }

  private static TaskWriter<RowData> defaultDeltaWriter(Table deltaTable, boolean upsert) {
    SerializableSupplier<Table> tableSupplier = () -> deltaTable;
    RowDataTaskWriterFactory factory =
        new RowDataTaskWriterFactory(
            tableSupplier,
            SimpleDataUtil.ROW_TYPE,
            Long.MAX_VALUE,
            FileFormat.PARQUET,
            deltaTable.properties(),
            ImmutableSet.of(deltaTable.schema().findField("id").fieldId()),
            upsert);
    factory.initialize(1, 1);
    return factory.create();
  }

  private TaskWriter<RowData> defaultWriter() {
    SerializableSupplier<Table> tableSupplier = () -> table;
    RowDataTaskWriterFactory factory =
        new RowDataTaskWriterFactory(
            tableSupplier,
            SimpleDataUtil.ROW_TYPE,
            Long.MAX_VALUE,
            FileFormat.PARQUET,
            table.properties(),
            null,
            false);
    factory.initialize(1, 1);
    return factory.create();
  }

  private TaskWriter<RowData> dvOnlyWriter(boolean upsert) {
    return dvOnlyWriter(upsert, Long.MAX_VALUE);
  }

  private TaskWriter<RowData> dvOnlyWriter(boolean upsert, long targetFileSize) {
    SerializableSupplier<Table> tableSupplier = () -> table;
    RowDataTaskWriterFactory factory =
        new RowDataTaskWriterFactory(
            tableSupplier,
            SimpleDataUtil.ROW_TYPE,
            targetFileSize,
            FileFormat.PARQUET,
            table.properties(),
            ImmutableSet.of(table.schema().findField("id").fieldId()),
            upsert,
            true);
    factory.initialize(1, 1);
    return factory.create();
  }

  private static List<PkIndexEntry> liveRows(TaskWriter<RowData> writer) {
    assertThat(writer).isInstanceOf(DvOnlyDeltaWriter.class);
    return ((DvOnlyDeltaWriter) writer).liveRows();
  }

  private static Set<SerializedEqualityValues> deleteKeys(TaskWriter<RowData> writer) {
    assertThat(writer).isInstanceOf(DvOnlyDeltaWriter.class);
    return ((DvOnlyDeltaWriter) writer).deleteKeys();
  }

  private static RowData row(int id, String data) {
    return GenericRowData.of(id, StringData.fromString(data));
  }

  /**
   * A delete of the given key. The {@code data} column still has to be present, since a delete
   * carries every column of the row it removes.
   */
  private static RowData deleteOf(int id, String data) {
    return kind(RowKind.DELETE, id, data);
  }

  private static RowData kind(RowKind kind, int id, String data) {
    return GenericRowData.ofKind(kind, id, StringData.fromString(data));
  }
}
