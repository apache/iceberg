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
package org.apache.iceberg;

import static org.apache.iceberg.V4TestHelpers.FILE_SIZE_IN_BYTES;
import static org.apache.iceberg.V4TestHelpers.FORMAT_VERSION_V4;
import static org.apache.iceberg.V4TestHelpers.RECORD_COUNT;
import static org.apache.iceberg.V4TestHelpers.SNAPSHOT_ID;
import static org.apache.iceberg.V4TestHelpers.dataFile;
import static org.apache.iceberg.V4TestHelpers.deletionVector;
import static org.apache.iceberg.V4TestHelpers.writeTrackedFiles;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.mumbling.MumblingTestUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

class TestV4ChangelogPlanner {
  private static final long CREATION_SNAPSHOT_ID = 40L;
  private static final long SEQUENCE_NUMBER = 7L;
  private static final long FIRST_ROW_ID = 1000L;
  private static final String TABLE_LOCATION = "s3://bucket/db/table";
  private static final String FILE_A = TABLE_LOCATION + "/a.parquet";
  private static final String FILE_B = TABLE_LOCATION + "/b.parquet";
  private static final String FILE_C = TABLE_LOCATION + "/c.parquet";
  private static final String FILE_D = TABLE_LOCATION + "/d.parquet";
  private static final DeletionVector OLD_DV = deletionVector(TABLE_LOCATION + "/old-dv.puffin");
  private static final DeletionVector NEW_DV =
      deletionVector(TABLE_LOCATION + "/new-dv.puffin", 200L, 75L, 10L);

  private static final Schema TABLE_SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));
  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();
  private static final Map<Integer, PartitionSpec> SPECS = ImmutableMap.of(SPEC.specId(), SPEC);
  private static final Types.StructType STATS_TYPE =
      StatsUtil.statsWriteSchema(
          TABLE_SCHEMA, MetricsConfig.from(ImmutableMap.of(), TABLE_SCHEMA, null));
  private static final FileFormat MANIFEST_FORMAT = FileFormat.AVRO;

  private static final Tracking ADDED_TRACKING =
      new TrackingStruct(
          EntryStatus.ADDED,
          SNAPSHOT_ID,
          SEQUENCE_NUMBER,
          SEQUENCE_NUMBER,
          null,
          FIRST_ROW_ID,
          null,
          null);
  private static final Tracking REMOVED_TRACKING =
      new TrackingStruct(EntryStatus.DELETED, SNAPSHOT_ID, 5L, 5L, null, FIRST_ROW_ID, null, null);
  private static final Tracking OLD_TRACKING =
      new TrackingStruct(
          EntryStatus.ADDED, CREATION_SNAPSHOT_ID, 5L, 5L, null, FIRST_ROW_ID, null, null);

  private final InMemoryFileIO fileIO = new InMemoryFileIO();

  @Test
  void rootWithOnlyDataFiles() throws IOException {
    Tracking unchanged =
        new TrackingStruct(
            EntryStatus.EXISTING, CREATION_SNAPSHOT_ID, 5L, 5L, null, FIRST_ROW_ID, null, null);
    ManifestFile root =
        writeManifest(
            dataFile(ADDED_TRACKING, FILE_A),
            dataFile(REMOVED_TRACKING, FILE_B),
            dataFile(unchanged, FILE_C));

    SnapshotFileChanges changes = planner(root).planChanges();

    assertThat(changes.addedDataFiles()).extracting(DataFile::location).containsExactly(FILE_A);
    assertThat(changes.removedDataFiles()).extracting(DataFile::location).containsExactly(FILE_B);
    assertThat(changes.modifiedDataFiles()).isEmpty();
    assertThat(changes.replacedDataFiles()).isEmpty();
  }

  @Test
  void rootWithOnlyLeafManifests() throws IOException {
    ManifestFile addedLeaf = writeManifest(dataFile(ADDED_TRACKING, FILE_A));
    ManifestFile removedLeaf = writeManifest(dataFile(REMOVED_TRACKING, FILE_B));
    ManifestFile root =
        writeManifest(
            addedLeafReference(addedLeaf, 1),
            leafReference(
                removedLeaf,
                ADDED_TRACKING,
                manifestInfo().deletedFilesCount(1).deletedRowsCount(RECORD_COUNT).build()));

    SnapshotFileChanges changes = planner(root).planChanges();

    assertThat(changes.addedDataFiles()).extracting(DataFile::location).containsExactly(FILE_A);
    assertThat(changes.removedDataFiles()).extracting(DataFile::location).containsExactly(FILE_B);
    assertThat(changes.modifiedDataFiles()).isEmpty();
    assertThat(changes.replacedDataFiles()).isEmpty();
  }

  @Test
  void rootWithMixedDataAndLeafManifests() throws IOException {
    ManifestFile leaf1 = writeManifest(dataFile(ADDED_TRACKING, FILE_C));
    ManifestFile leaf2 = writeManifest(dataFile(ADDED_TRACKING, FILE_D));
    ManifestFile root =
        writeManifest(
            dataFile(ADDED_TRACKING, FILE_A),
            dataFile(REMOVED_TRACKING, FILE_B),
            addedLeafReference(leaf1, 1),
            addedLeafReference(leaf2, 1));

    SnapshotFileChanges changes = planner(root).planChanges();

    assertThat(changes.addedDataFiles())
        .extracting(DataFile::location)
        .containsExactlyInAnyOrder(FILE_A, FILE_C, FILE_D);
    assertThat(changes.removedDataFiles()).extracting(DataFile::location).containsExactly(FILE_B);
  }

  @Test
  void modifiedLeafUsesModificationSnapshot() throws IOException {
    ManifestFile leaf = writeManifest(dataFile(OLD_TRACKING, FILE_A));
    byte[] deleted = positions(0);
    Tracking modification =
        new TrackingStruct(
            EntryStatus.MODIFIED,
            CREATION_SNAPSHOT_ID,
            5L,
            5L,
            SNAPSHOT_ID,
            FIRST_ROW_ID,
            deleted,
            null);
    ManifestFile root =
        writeManifest(
            leafReference(
                leaf,
                modification,
                manifestInfo()
                    .deletedFilesCount(1)
                    .deletedRowsCount(RECORD_COUNT)
                    .dv(ByteBuffer.wrap(deleted))
                    .build()));

    SnapshotFileChanges changes = planner(root).planChanges();

    assertThat(changes.removedDataFiles()).extracting(DataFile::location).containsExactly(FILE_A);
    assertThat(changes.addedDataFiles()).isEmpty();
    assertThat(changes.modifiedDataFiles()).isEmpty();
    assertThat(changes.replacedDataFiles()).isEmpty();
  }

  @Test
  void modifiedAndReplacedVersionsAcrossLeaves() throws IOException {
    Tracking oldVersion =
        new TrackingStruct(
            EntryStatus.REPLACED, SNAPSHOT_ID, 5L, 5L, null, FIRST_ROW_ID, null, null);
    Tracking newVersion =
        new TrackingStruct(
            EntryStatus.MODIFIED,
            CREATION_SNAPSHOT_ID,
            5L,
            5L,
            SNAPSHOT_ID,
            FIRST_ROW_ID,
            null,
            null);
    ManifestFile oldLeaf = writeManifest(dataFileWithMetadata(oldVersion, FILE_A, null, OLD_DV));
    ManifestFile newLeaf = writeManifest(dataFileWithMetadata(newVersion, FILE_A, null, NEW_DV));
    ManifestFile root =
        writeManifest(
            leafReference(
                oldLeaf,
                ADDED_TRACKING,
                manifestInfo().replacedFilesCount(1).replacedRowsCount(RECORD_COUNT).build()),
            leafReference(
                newLeaf,
                ADDED_TRACKING,
                manifestInfo().modifiedFilesCount(1).modifiedRowsCount(RECORD_COUNT).build()));

    SnapshotFileChanges changes = planner(root).planChanges();

    assertThat(changes.modifiedDataFiles())
        .singleElement()
        .satisfies(file -> assertDataFileWithDV(file, FILE_A, NEW_DV));
    assertThat(changes.replacedDataFiles())
        .singleElement()
        .satisfies(file -> assertDataFileWithDV(file, FILE_A, OLD_DV));
    assertThat(changes.addedDataFiles()).isEmpty();
    assertThat(changes.removedDataFiles()).isEmpty();
  }

  @ParameterizedTest
  @EnumSource(
      value = EntryStatus.class,
      names = {"DELETED", "REPLACED"})
  void deletedAndReplacedLeafReferencesAreNotOpened(EntryStatus status) throws IOException {
    ManifestFile unreadLeaf =
        new RootManifestFile(
            TABLE_LOCATION + "/" + MANIFEST_FORMAT.addExtension("unread"),
            FILE_SIZE_IN_BYTES,
            CREATION_SNAPSHOT_ID,
            5L,
            FIRST_ROW_ID,
            null);
    Tracking removedReference =
        new TrackingStruct(status, SNAPSHOT_ID, 5L, 5L, null, FIRST_ROW_ID, null, null);
    ManifestFile root =
        writeManifest(
            dataFile(ADDED_TRACKING, FILE_A),
            leafReference(
                unreadLeaf,
                removedReference,
                manifestInfo().addedFilesCount(1).addedRowsCount(RECORD_COUNT).build()));
    FileIO io = Mockito.spy(fileIO);

    SnapshotFileChanges changes =
        new V4ChangelogPlanner(io, root, TABLE_SCHEMA, SPECS).planChanges();

    assertThat(changes.addedDataFiles()).extracting(DataFile::location).containsExactly(FILE_A);
    assertThat(changes.removedDataFiles()).isEmpty();
    assertThat(changes.modifiedDataFiles()).isEmpty();
    assertThat(changes.replacedDataFiles()).isEmpty();
    Mockito.verify(io, Mockito.never()).newInputFile(unreadLeaf.path());
  }

  @Test
  void filterForwardedToRootAndLeafReaders() throws IOException {
    ManifestFile leaf =
        writeManifest(
            dataFileWithMetadata(ADDED_TRACKING, FILE_C, idStats(1, 10), null),
            dataFileWithMetadata(ADDED_TRACKING, FILE_D, idStats(20, 30), null));
    ManifestFile root =
        writeManifest(
            dataFileWithMetadata(ADDED_TRACKING, FILE_A, idStats(1, 10), null),
            dataFileWithMetadata(ADDED_TRACKING, FILE_B, idStats(20, 30), null),
            addedLeafReference(leaf, 2));

    SnapshotFileChanges changes =
        planner(root).filterData(Expressions.equal("id", 5)).planChanges();

    assertThat(changes.addedDataFiles())
        .extracting(DataFile::location)
        .containsExactlyInAnyOrder(FILE_A, FILE_C);
  }

  @Test
  void filterHonorsCaseSensitivity() throws IOException {
    ManifestFile leaf =
        writeManifest(dataFileWithMetadata(ADDED_TRACKING, FILE_B, idStats(1, 10), null));
    ManifestFile root =
        writeManifest(
            dataFileWithMetadata(ADDED_TRACKING, FILE_A, idStats(1, 10), null),
            addedLeafReference(leaf, 1));

    assertThatThrownBy(() -> planner(root).filterData(Expressions.equal("ID", 5)).planChanges())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'ID'");

    SnapshotFileChanges changes =
        planner(root).filterData(Expressions.equal("ID", 5)).caseSensitive(false).planChanges();

    assertThat(changes.addedDataFiles())
        .extracting(DataFile::location)
        .containsExactlyInAnyOrder(FILE_A, FILE_B);
  }

  @Test
  void projectStatsForwardedToRootAndLeafReaders() throws IOException {
    ContentStatsStruct stats = idStats(1, 10);
    stats.setStats(2, fieldStats("data", "a", "z"));
    ManifestFile leaf = writeManifest(dataFileWithMetadata(REMOVED_TRACKING, FILE_B, stats, null));
    ManifestFile root =
        writeManifest(
            dataFileWithMetadata(ADDED_TRACKING, FILE_A, stats, null),
            leafReference(
                leaf,
                ADDED_TRACKING,
                manifestInfo().deletedFilesCount(1).deletedRowsCount(RECORD_COUNT).build()));
    int idFieldId = TABLE_SCHEMA.findField("id").fieldId();

    SnapshotFileChanges changes = planner(root).projectStats(idFieldId).planChanges();

    assertThat(changes.addedDataFiles())
        .singleElement()
        .satisfies(file -> assertProjectedStats(file, idFieldId, stats));
    assertThat(changes.removedDataFiles())
        .singleElement()
        .satisfies(file -> assertProjectedStats(file, idFieldId, stats));
  }

  @Test
  void tableLocationForwardedToRootAndLeafReaders() throws IOException {
    String relativeLeaf = "metadata/" + MANIFEST_FORMAT.addExtension("relative-leaf");
    DeletionVector rootDV = deletionVector("deletes/root.puffin");
    DeletionVector leafDV = deletionVector("deletes/leaf.puffin");
    ManifestFile leaf =
        writeManifestAt(
            TABLE_LOCATION + "/" + relativeLeaf,
            dataFileWithMetadata(ADDED_TRACKING, "data/leaf.parquet", null, leafDV));
    ManifestFile relativeReference =
        new RootManifestFile(
            relativeLeaf, leaf.length(), SNAPSHOT_ID, SEQUENCE_NUMBER, FIRST_ROW_ID, null);
    ManifestFile root =
        writeManifest(
            dataFileWithMetadata(ADDED_TRACKING, "data/root.parquet", null, rootDV),
            addedLeafReference(relativeReference, 1));

    SnapshotFileChanges changes = planner(root).tableLocation(TABLE_LOCATION).planChanges();

    assertThat(changes.addedDataFiles())
        .extracting(DataFile::location)
        .containsExactlyInAnyOrder(
            TABLE_LOCATION + "/data/root.parquet", TABLE_LOCATION + "/data/leaf.parquet");
    assertThat(changes.addedDataFiles())
        .extracting(file -> file.deletionVector().location())
        .containsExactlyInAnyOrder(
            TABLE_LOCATION + "/" + rootDV.location(), TABLE_LOCATION + "/" + leafDV.location());
  }

  @Test
  void planningOpensOnlyRoot() throws IOException {
    ManifestFile leaf = writeManifest(dataFile(ADDED_TRACKING, FILE_A));
    ManifestFile root = writeManifest(addedLeafReference(leaf, 1));
    List<SeekableInputStream> streams = Lists.newArrayList();
    FileIO io = recordingIO(streams);

    new V4ChangelogPlanner(io, root, TABLE_SCHEMA, SPECS).planWith(null).planChanges();

    Mockito.verify(io).newInputFile(root.path());
    Mockito.verify(io, Mockito.never()).newInputFile(leaf.path());
    assertStreamsClosed(streams);
  }

  @Test
  void firstViewAccessClosesManifestReaders() throws IOException {
    ManifestFile leaf1 = writeManifest(dataFile(ADDED_TRACKING, FILE_A));
    ManifestFile leaf2 = writeManifest(dataFile(ADDED_TRACKING, FILE_B));
    ManifestFile root = writeManifest(addedLeafReference(leaf1, 1), addedLeafReference(leaf2, 1));
    List<SeekableInputStream> streams = Lists.newArrayList();
    FileIO io = recordingIO(streams);
    SnapshotFileChanges changes =
        new V4ChangelogPlanner(io, root, TABLE_SCHEMA, SPECS).planWith(null).planChanges();

    Iterable<DataFile> files = changes.addedDataFiles();

    Mockito.verify(io).newInputFile(leaf1.path());
    Mockito.verify(io).newInputFile(leaf2.path());
    assertStreamsClosed(streams);
    int openedStreams = streams.size();
    assertThat(files.iterator().next().location()).isIn(FILE_A, FILE_B);
    assertThat(files).extracting(DataFile::location).containsExactlyInAnyOrder(FILE_A, FILE_B);
    assertThat(streams).hasSize(openedStreams);
  }

  @Test
  void deleteManifestInRootUnsupported() throws IOException {
    String location = TABLE_LOCATION + "/legacy-deletes.avro";
    TrackedFile deletes =
        new TrackedFileStruct(
            ADDED_TRACKING,
            FileContent.DELETE_MANIFEST,
            location,
            FileFormat.AVRO,
            1L,
            FILE_SIZE_IN_BYTES,
            null,
            null,
            null,
            null,
            null,
            manifestInfo().addedFilesCount(1).addedRowsCount(RECORD_COUNT).formatVersion(0).build(),
            null,
            null,
            null);
    ManifestFile root = writeManifest(deletes);

    assertThatThrownBy(() -> planner(root).planChanges())
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Legacy delete manifests are not supported: " + location);
  }

  @ParameterizedTest
  @EnumSource(
      value = FileContent.class,
      names = {"POSITION_DELETES", "EQUALITY_DELETES"})
  void standaloneDeleteFileInRootUnsupported(FileContent content) throws IOException {
    String location = TABLE_LOCATION + "/standalone-deletes.parquet";
    TrackedFile deletes =
        new TrackedFileStruct(
            ADDED_TRACKING,
            content,
            location,
            FileFormat.PARQUET,
            RECORD_COUNT,
            FILE_SIZE_IN_BYTES,
            SPEC.specId(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    ManifestFile root = writeManifest(deletes);

    assertThatThrownBy(() -> planner(root).planChanges())
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Unsupported file type in root manifest: " + content + ": " + location);
  }

  private V4ChangelogPlanner planner(ManifestFile root) {
    return new V4ChangelogPlanner(fileIO, root, TABLE_SCHEMA, SPECS);
  }

  private ManifestFile writeManifest(TrackedFile... files) throws IOException {
    return writeManifestAt(
        TABLE_LOCATION
            + "/metadata/"
            + MANIFEST_FORMAT.addExtension("manifest-" + System.nanoTime()),
        files);
  }

  private ManifestFile writeManifestAt(String location, TrackedFile... files) throws IOException {
    OutputFile out = fileIO.newOutputFile(location);
    InputFile written =
        writeTrackedFiles(
                out, MANIFEST_FORMAT, SPEC.partitionType(), STATS_TYPE, ImmutableList.copyOf(files))
            .toInputFile();
    return new RootManifestFile(
        written.location(), written.getLength(), SNAPSHOT_ID, SEQUENCE_NUMBER, FIRST_ROW_ID, null);
  }

  private static TrackedFile dataFileWithMetadata(
      Tracking tracking, String location, ContentStats stats, DeletionVector dv) {
    return new TrackedFileStruct(
        tracking,
        FileContent.DATA,
        location,
        FileFormat.PARQUET,
        RECORD_COUNT,
        FILE_SIZE_IN_BYTES,
        SPEC.specId(),
        null,
        stats,
        null,
        dv,
        null,
        null,
        null,
        null);
  }

  private static TrackedFile addedLeafReference(ManifestFile leaf, int fileCount) {
    return leafReference(
        leaf,
        ADDED_TRACKING,
        manifestInfo().addedFilesCount(fileCount).addedRowsCount(fileCount * RECORD_COUNT).build());
  }

  private static TrackedFile leafReference(
      ManifestFile leaf, Tracking tracking, ManifestInfo info) {
    long entries =
        info.addedFilesCount()
            + info.existingFilesCount()
            + info.modifiedFilesCount()
            + info.deletedFilesCount()
            + info.replacedFilesCount();
    return new TrackedFileStruct(
        tracking,
        FileContent.DATA_MANIFEST,
        leaf.path(),
        FileFormat.fromFileName(leaf.path()),
        entries,
        leaf.length(),
        null,
        null,
        null,
        null,
        null,
        info,
        null,
        null,
        null);
  }

  private static ManifestInfoStruct.Builder manifestInfo() {
    return ManifestInfoStruct.builder()
        .addedFilesCount(0)
        .existingFilesCount(0)
        .deletedFilesCount(0)
        .replacedFilesCount(0)
        .modifiedFilesCount(0)
        .addedRowsCount(0)
        .existingRowsCount(0)
        .deletedRowsCount(0)
        .replacedRowsCount(0)
        .modifiedRowsCount(0)
        .minSequenceNumber(0L)
        .formatVersion(FORMAT_VERSION_V4);
  }

  private static ContentStatsStruct idStats(int lower, int upper) {
    ContentStatsStruct stats = new ContentStatsStruct(STATS_TYPE);
    stats.setStats(TABLE_SCHEMA.findField("id").fieldId(), fieldStats("id", lower, upper));
    return stats;
  }

  private static <T> FieldStatsStruct<T> fieldStats(String fieldName, T lower, T upper) {
    return new FieldStatsStruct<>(
        STATS_TYPE.fieldType(fieldName).asStructType(), lower, upper, true, RECORD_COUNT, 0, 0, 0L);
  }

  private static byte[] positions(int... positions) {
    return ByteBuffers.toByteArray(MumblingTestUtil.build(MumblingTestUtil.sparse(positions)));
  }

  private static void assertDataFileWithDV(DataFile file, String location, DeletionVector dv) {
    assertThat(file.location()).isEqualTo(location);
    assertThat(file.deletionVector())
        .extracting(
            DeletionVector::location,
            DeletionVector::offset,
            DeletionVector::sizeInBytes,
            DeletionVector::cardinality)
        .containsExactly(dv.location(), dv.offset(), dv.sizeInBytes(), dv.cardinality());
  }

  private static void assertProjectedStats(DataFile file, int fieldId, ContentStats stats) {
    assertThat(file.lowerBounds())
        .containsOnlyKeys(fieldId)
        .containsEntry(
            fieldId,
            Conversions.toByteBuffer(
                Types.IntegerType.get(), stats.statsFor(fieldId).lowerBound()));
    assertThat(file.upperBounds())
        .containsOnlyKeys(fieldId)
        .containsEntry(
            fieldId,
            Conversions.toByteBuffer(
                Types.IntegerType.get(), stats.statsFor(fieldId).upperBound()));
  }

  private FileIO recordingIO(List<SeekableInputStream> streams) {
    FileIO io = Mockito.spy(fileIO);
    Mockito.doAnswer(
            invocation -> {
              InputFile input =
                  Mockito.spy(fileIO.newInputFile(invocation.getArgument(0, String.class)));
              Mockito.doAnswer(
                      open -> {
                        SeekableInputStream stream =
                            Mockito.spy((SeekableInputStream) open.callRealMethod());
                        streams.add(stream);
                        return stream;
                      })
                  .when(input)
                  .newStream();
              return input;
            })
        .when(io)
        .newInputFile(Mockito.anyString());
    return io;
  }

  private static void assertStreamsClosed(List<SeekableInputStream> streams) throws IOException {
    assertThat(streams).isNotEmpty();
    for (SeekableInputStream stream : streams) {
      Mockito.verify(stream, Mockito.atLeastOnce()).close();
    }
  }
}
