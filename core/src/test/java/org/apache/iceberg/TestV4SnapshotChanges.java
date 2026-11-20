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
import static org.apache.iceberg.V4TestHelpers.RECORD_COUNT;
import static org.apache.iceberg.V4TestHelpers.SNAPSHOT_ID;
import static org.apache.iceberg.V4TestHelpers.dataFile;
import static org.apache.iceberg.V4TestHelpers.deletionVector;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

class TestV4SnapshotChanges {
  private static final String FILE_A = "s3://bucket/table/a.parquet";
  private static final String FILE_B = "s3://bucket/table/b.parquet";
  private static final String FILE_C = "s3://bucket/table/c.parquet";
  private static final String FILE_D = "s3://bucket/table/d.parquet";
  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();
  private static final Map<Integer, PartitionSpec> SPECS = ImmutableMap.of(SPEC.specId(), SPEC);
  private static final Schema TABLE_SCHEMA = new Schema(optional(1, "id", Types.IntegerType.get()));
  private static final Types.StructType STATS_TYPE =
      StatsUtil.statsWriteSchema(
          TABLE_SCHEMA, MetricsConfig.from(ImmutableMap.of(), TABLE_SCHEMA, null));

  @Test
  void dataChangesAreGroupedByStatus() {
    SnapshotFileChanges changes =
        changes(
            dataFile(tracking(EntryStatus.ADDED), FILE_A),
            dataFile(tracking(EntryStatus.DELETED), FILE_B),
            dataFile(tracking(EntryStatus.MODIFIED), FILE_C),
            dataFile(tracking(EntryStatus.REPLACED), FILE_D));

    assertThat(changes.addedDataFiles()).extracting(DataFile::location).containsExactly(FILE_A);
    assertThat(changes.removedDataFiles()).extracting(DataFile::location).containsExactly(FILE_B);
    assertThat(changes.modifiedDataFiles()).extracting(DataFile::location).containsExactly(FILE_C);
    assertThat(changes.replacedDataFiles()).extracting(DataFile::location).containsExactly(FILE_D);
  }

  @ParameterizedTest
  @EnumSource(
      value = EntryStatus.class,
      names = {"DELETED", "REPLACED"})
  void removedVersionsRetainStats(EntryStatus status) {
    int lower = 10;
    int upper = 20;
    ContentStatsStruct stats = new ContentStatsStruct(STATS_TYPE);
    stats.setStats(
        TABLE_SCHEMA.findField("id").fieldId(),
        new FieldStatsStruct<>(
            STATS_TYPE.fieldType("id").asStructType(), lower, upper, true, RECORD_COUNT, 0, 0, 0L));
    SnapshotFileChanges changes = changes(fileWithMetadata(status, stats, null));

    DataFile removed =
        Iterables.getOnlyElement(
            status == EntryStatus.DELETED
                ? changes.removedDataFiles()
                : changes.replacedDataFiles());

    assertThat(removed.lowerBounds())
        .containsEntry(1, Conversions.toByteBuffer(Types.IntegerType.get(), lower));
    assertThat(removed.upperBounds())
        .containsEntry(1, Conversions.toByteBuffer(Types.IntegerType.get(), upper));
    assertThat(removed.valueCounts()).containsEntry(1, RECORD_COUNT);
  }

  @Test
  void colocatedDVsAreNotStandaloneDeleteFiles() {
    DeletionVector dv = deletionVector("s3://bucket/table/deletes.puffin");
    SnapshotFileChanges changes = changes(fileWithMetadata(EntryStatus.MODIFIED, null, dv));

    assertThat(changes.addedDeleteFiles()).isEmpty();
    assertThat(changes.removedDeleteFiles()).isEmpty();
    assertThat(changes.modifiedDataFiles())
        .singleElement()
        .extracting(
            file -> file.deletionVector().location(),
            file -> file.deletionVector().offset(),
            file -> file.deletionVector().sizeInBytes(),
            file -> file.deletionVector().cardinality())
        .containsExactly(dv.location(), dv.offset(), dv.sizeInBytes(), dv.cardinality());
  }

  @ParameterizedTest
  @MethodSource("changeAccessors")
  void changesAreLoadedOnce(Consumer<SnapshotFileChanges> firstAccessor) {
    List<TrackedFile> files =
        ImmutableList.of(
            dataFile(tracking(EntryStatus.ADDED), FILE_A),
            dataFile(tracking(EntryStatus.DELETED), FILE_B),
            dataFile(tracking(EntryStatus.MODIFIED), FILE_C),
            dataFile(tracking(EntryStatus.REPLACED), FILE_D));
    AtomicInteger reads = new AtomicInteger();
    AtomicInteger closes = new AtomicInteger();
    CloseableIterable<TrackedFile> input =
        CloseableIterable.whenComplete(
            CloseableIterable.transform(
                CloseableIterable.withNoopClose(files),
                file -> {
                  reads.incrementAndGet();
                  return file;
                }),
            closes::incrementAndGet);
    SnapshotFileChanges changes = new V4SnapshotChanges(input, SPECS);

    assertThat(reads).hasValue(0);
    firstAccessor.accept(changes);
    assertThat(reads).hasValue(files.size());
    assertThat(closes).hasValue(1);

    for (int iteration = 0; iteration < 2; iteration += 1) {
      assertThat(changes.addedDataFiles()).extracting(DataFile::location).containsExactly(FILE_A);
      assertThat(changes.removedDataFiles()).extracting(DataFile::location).containsExactly(FILE_B);
      assertThat(changes.modifiedDataFiles())
          .extracting(DataFile::location)
          .containsExactly(FILE_C);
      assertThat(changes.replacedDataFiles())
          .extracting(DataFile::location)
          .containsExactly(FILE_D);
      assertThat(changes.addedDeleteFiles()).isEmpty();
      assertThat(changes.removedDeleteFiles()).isEmpty();
    }

    assertThat(reads).hasValue(files.size());
    assertThat(closes).hasValue(1);
  }

  @Test
  void failedLoadsAreNotRetried() {
    AtomicInteger reads = new AtomicInteger();
    AtomicInteger closes = new AtomicInteger();
    IllegalStateException failure = new IllegalStateException("Cannot read remaining changes");
    CloseableIterable<TrackedFile> input =
        CloseableIterable.whenComplete(
            CloseableIterable.transform(
                CloseableIterable.withNoopClose(
                    ImmutableList.of(
                        dataFile(tracking(EntryStatus.ADDED), FILE_A),
                        dataFile(tracking(EntryStatus.DELETED), FILE_B))),
                file -> {
                  reads.incrementAndGet();
                  if (file.location().equals(FILE_B)) {
                    throw failure;
                  }

                  return file;
                }),
            closes::incrementAndGet);
    SnapshotFileChanges changes = new V4SnapshotChanges(input, SPECS);

    assertThatThrownBy(changes::addedDataFiles).isSameAs(failure).hasMessage(failure.getMessage());
    assertThat(reads).hasValue(2);
    assertThat(closes).hasValue(1);

    for (Named<Consumer<SnapshotFileChanges>> accessor : changeAccessors()) {
      assertThatThrownBy(() -> accessor.getPayload().accept(changes))
          .isSameAs(failure)
          .hasMessage(failure.getMessage());
    }

    assertThat(reads).hasValue(2);
    assertThat(closes).hasValue(1);
  }

  @Test
  void failedClosesAreNotRetried() {
    AtomicInteger reads = new AtomicInteger();
    AtomicInteger closes = new AtomicInteger();
    IOException failure = new IOException("Cannot close snapshot changes");
    CloseableIterable<TrackedFile> input =
        CloseableIterable.transform(
            CloseableIterable.combine(
                ImmutableList.of(dataFile(tracking(EntryStatus.ADDED), FILE_A)),
                () -> {
                  closes.incrementAndGet();
                  throw failure;
                }),
            file -> {
              reads.incrementAndGet();
              return file;
            });
    SnapshotFileChanges changes = new V4SnapshotChanges(input, SPECS);

    assertThatThrownBy(changes::addedDataFiles)
        .isInstanceOf(UncheckedIOException.class)
        .hasMessage("Failed to close snapshot changes")
        .hasCause(failure);
    assertThat(reads).hasValue(1);
    assertThat(closes).hasValue(1);

    for (Named<Consumer<SnapshotFileChanges>> accessor : changeAccessors()) {
      assertThatThrownBy(() -> accessor.getPayload().accept(changes))
          .isInstanceOf(UncheckedIOException.class)
          .hasMessage("Failed to close snapshot changes")
          .hasCause(failure);
    }

    assertThat(reads).hasValue(1);
    assertThat(closes).hasValue(1);
  }

  private static List<Named<Consumer<SnapshotFileChanges>>> changeAccessors() {
    return ImmutableList.of(
        Named.of("added data", SnapshotFileChanges::addedDataFiles),
        Named.of("removed data", SnapshotFileChanges::removedDataFiles),
        Named.of("modified data", SnapshotFileChanges::modifiedDataFiles),
        Named.of("replaced data", SnapshotFileChanges::replacedDataFiles),
        Named.of("added deletes", SnapshotFileChanges::addedDeleteFiles),
        Named.of("removed deletes", SnapshotFileChanges::removedDeleteFiles));
  }

  private static SnapshotFileChanges changes(TrackedFile... files) {
    return new V4SnapshotChanges(CloseableIterable.of(ImmutableList.copyOf(files)), SPECS);
  }

  private static Tracking tracking(EntryStatus status) {
    return new TrackingStruct(
        status,
        status == EntryStatus.MODIFIED ? SNAPSHOT_ID - 1 : SNAPSHOT_ID, // snapshotId
        1L, // dataSequenceNumber
        1L, // fileSequenceNumber
        status == EntryStatus.MODIFIED ? SNAPSHOT_ID : null, // modifiedSnapshotId
        0L, // firstRowId
        null, // deletedPositions
        null); // replacedPositions
  }

  private static TrackedFile fileWithMetadata(
      EntryStatus status, ContentStats stats, DeletionVector dv) {
    return new TrackedFileStruct(
        tracking(status),
        FileContent.DATA,
        FILE_A,
        FileFormat.PARQUET,
        RECORD_COUNT,
        FILE_SIZE_IN_BYTES,
        SPEC.specId(),
        null, // partition
        stats,
        null, // sortOrderId
        dv,
        null, // manifestInfo
        null, // keyMetadata
        null, // splitOffsets
        null); // columnFiles
  }
}
