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
  void theDefaultWritePathWritesTheWriteResultOnly() throws IOException {
    SinkWriteResultSerializer serializer = SinkWriteResultSerializer.filesOnly();
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
  void theDefaultWritePathRejectsThePayloadOfTheDvOnlyWritePath() {
    assertThatThrownBy(() -> SinkWriteResultSerializer.filesOnly().serialize(withPayload()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("DV-only");
    assertThatThrownBy(
            () -> SinkWriteResultSerializer.filesOnly().serialize(SinkWriteResult.baseline(1L)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("DV-only");
  }

  @Test
  void theDvOnlyWritePathKeepsThePayload() throws IOException {
    SinkWriteResultSerializer serializer = SinkWriteResultSerializer.dvOnly();

    SinkWriteResult restored =
        SimpleVersionedSerialization.readVersionAndDeSerialize(
            SinkWriteResultSerializer.filesOnly(),
            SimpleVersionedSerialization.writeVersionAndSerialize(serializer, withPayload()));

    assertThat(serializer.getVersion()).isEqualTo(2);
    assertThat(restored.writeResult().dataFiles()).hasSize(1);
    assertThat(restored.deleteKeys()).hasSize(1);
    assertThat(restored.liveRows()).hasSize(1);
    assertThat(restored.positionDeletes()).hasSize(1);

    SinkWriteResult baseline =
        serializer.deserialize(
            serializer.getVersion(), serializer.serialize(SinkWriteResult.baseline(null)));
    assertThat(baseline.isBaseline()).isTrue();
    assertThat(baseline.baselineSnapshotId()).isNull();
  }

  private static SinkWriteResult withPayload() {
    SerializedEqualityValues key = new SerializedEqualityValues(new byte[] {1, 2, 3});
    DVPosition position =
        new DVPosition(
            DATA_FILE.location(),
            0L,
            0,
            StructLikeSerializer.EMPTY_PARTITION,
            PkIndexEntry.UNKNOWN_SEQUENCE);
    return new SinkWriteResult(
        WriteResult.builder().addDataFiles(DATA_FILE).build(),
        ImmutableList.of(key),
        ImmutableList.of(new PkIndexEntry(key, position)),
        ImmutableList.of(position));
  }
}
