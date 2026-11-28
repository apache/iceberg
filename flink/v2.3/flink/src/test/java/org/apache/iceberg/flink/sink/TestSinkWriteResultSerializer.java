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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.flink.maintenance.operator.DVPosition;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.flink.maintenance.operator.StructLikeSerializer;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

class TestSinkWriteResultSerializer {

  private static final DataFile DATA_FILE =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("/path/to/data-1.parquet")
          .withFormat(FileFormat.PARQUET)
          .withFileSizeInBytes(10)
          .withRecordCount(1)
          .build();

  @Test
  void theStandardWritePathWritesTheWriteResultOnly() throws IOException {
    SinkWriteResultSerializer serializer = SinkWriteResultSerializer.standard();
    SinkWriteResult result =
        new SinkWriteResult(WriteResult.builder().addDataFiles(DATA_FILE).build());

    byte[] serialized = SimpleVersionedSerialization.writeVersionAndSerialize(serializer, result);

    assertThat(serializer.getVersion()).isEqualTo(1);
    WriteResult asWriteResult =
        SimpleVersionedSerialization.readVersionAndDeSerialize(
            new WriteResultSerializer(), serialized);
    assertThat(asWriteResult.dataFiles()).hasSize(1);
    SinkWriteResult restored =
        SimpleVersionedSerialization.readVersionAndDeSerialize(
            SinkWriteResultSerializer.dvOnly(), serialized);
    assertThat(restored.writeResult().dataFiles()).hasSize(1);
  }

  @Test
  void theStandardWritePathRejectsThePayloadOfTheDvOnlyWritePath() {
    assertThatThrownBy(() -> SinkWriteResultSerializer.standard().serialize(withPayload()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("DV-only");
    assertThatThrownBy(
            () -> SinkWriteResultSerializer.standard().serialize(SinkWriteResult.baseline(1L)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("DV-only");
  }

  @Test
  void theDvOnlyWritePathKeepsThePayload() throws IOException {
    SinkWriteResultSerializer serializer = SinkWriteResultSerializer.dvOnly();
    SinkWriteResult original = withPayload();

    SinkWriteResult restored =
        SimpleVersionedSerialization.readVersionAndDeSerialize(
            SinkWriteResultSerializer.standard(),
            SimpleVersionedSerialization.writeVersionAndSerialize(serializer, original));

    assertThat(serializer.getVersion()).isEqualTo(2);
    assertThat(restored.kind()).isEqualTo(SinkWriteResult.Kind.WRITER);
    assertThat(restored.writeResult().dataFiles()).hasSize(1);
    assertThat(restored.deleteKeys()).isEqualTo(original.deleteKeys());
    assertThat(restored.liveRows()).isEqualTo(original.liveRows());
  }

  @Test
  void theDvOnlyWritePathKeepsTheBaseline() throws IOException {
    SinkWriteResultSerializer serializer = SinkWriteResultSerializer.dvOnly();

    SinkWriteResult withSnapshot =
        serializer.deserialize(
            serializer.getVersion(), serializer.serialize(SinkWriteResult.baseline(42L)));
    SinkWriteResult emptyBranch =
        serializer.deserialize(
            serializer.getVersion(), serializer.serialize(SinkWriteResult.baseline(null)));

    assertThat(withSnapshot.kind()).isEqualTo(SinkWriteResult.Kind.BASELINE);
    assertThat(withSnapshot.snapshotId()).isEqualTo(42L);
    assertThat(emptyBranch.kind()).isEqualTo(SinkWriteResult.Kind.BASELINE);
    assertThat(emptyBranch.snapshotId()).isNull();
  }

  @Test
  void theDvOnlyWritePathKeepsTheSnapshotTheDeletionVectorsWereMergedWith() throws IOException {
    SinkWriteResultSerializer serializer = SinkWriteResultSerializer.dvOnly();
    WriteResult vectors = WriteResult.builder().addDataFiles(DATA_FILE).build();

    SinkWriteResult withSnapshot =
        serializer.deserialize(
            serializer.getVersion(),
            serializer.serialize(SinkWriteResult.deletionVectors(vectors, 42L)));
    SinkWriteResult emptyBranch =
        serializer.deserialize(
            serializer.getVersion(),
            serializer.serialize(SinkWriteResult.deletionVectors(vectors, null)));
    SinkWriteResult writerResult =
        serializer.deserialize(serializer.getVersion(), serializer.serialize(withPayload()));

    assertThat(withSnapshot.kind()).isEqualTo(SinkWriteResult.Kind.DELETION_VECTORS);
    assertThat(withSnapshot.snapshotId()).isEqualTo(42L);
    assertThat(withSnapshot.writeResult().dataFiles()).hasSize(1);
    assertThat(emptyBranch.kind()).isEqualTo(SinkWriteResult.Kind.DELETION_VECTORS);
    assertThat(emptyBranch.snapshotId()).isNull();
    assertThat(writerResult.kind()).isEqualTo(SinkWriteResult.Kind.WRITER);
  }

  @Test
  void theDvOnlyWritePathWritesTheFileOfManyRowsOnce() throws IOException {
    String path = "s3://bucket/warehouse/db/table/data/00000-0-" + "0".repeat(64) + ".parquet";
    int rowCount = 1_000;
    List<PkIndexEntry> rows = Lists.newArrayList();
    for (int i = 0; i < rowCount; i++) {
      rows.add(row(i, path, i, 0, StructLikeSerializer.EMPTY_PARTITION));
    }

    byte[] serialized =
        SinkWriteResultSerializer.dvOnly()
            .serialize(
                new SinkWriteResult(WriteResult.builder().build(), ImmutableList.of(), rows));

    assertThat(serialized.length).isLessThan(rowCount * path.length());
  }

  @Test
  void theDvOnlyWritePathRejectsAKnownDataSequenceNumber() {
    SerializedEqualityValues key = new SerializedEqualityValues(new byte[] {1});
    PkIndexEntry row =
        new PkIndexEntry(
            key,
            new DVPosition(DATA_FILE.location(), 0L, 0, StructLikeSerializer.EMPTY_PARTITION, 5L));
    SinkWriteResult result =
        new SinkWriteResult(WriteResult.builder().build(), ImmutableList.of(), List.of(row));

    assertThatThrownBy(() -> SinkWriteResultSerializer.dvOnly().serialize(result))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("data sequence number 5");
  }

  private static SinkWriteResult withPayload() {
    String otherFile = "/path/to/data-2.parquet";
    byte[] otherPartition = {7, 8, 9};
    return new SinkWriteResult(
        WriteResult.builder().addDataFiles(DATA_FILE).build(),
        ImmutableList.of(key(100), key(101)),
        ImmutableList.of(
            row(1, DATA_FILE.location(), 0L, 0, StructLikeSerializer.EMPTY_PARTITION),
            row(2, otherFile, Long.MAX_VALUE, 3, otherPartition),
            row(3, DATA_FILE.location(), 128L, 0, StructLikeSerializer.EMPTY_PARTITION)));
  }

  private static PkIndexEntry row(
      int id, String path, long position, int specId, byte[] partition) {
    return new PkIndexEntry(
        key(id), new DVPosition(path, position, specId, partition, PkIndexEntry.UNKNOWN_SEQUENCE));
  }

  private static SerializedEqualityValues key(int id) {
    return new SerializedEqualityValues(new byte[] {(byte) (id >>> 8), (byte) id});
  }
}
