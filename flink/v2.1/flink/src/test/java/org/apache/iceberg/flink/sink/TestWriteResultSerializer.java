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
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.flink.util.InstantiationUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class TestWriteResultSerializer {

  private static final DataFile DATA_FILE =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("/path/to/data-1.parquet")
          .withFileSizeInBytes(0)
          .withMetrics(
              new Metrics(
                  42L,
                  null,
                  ImmutableMap.of(1, 5L),
                  ImmutableMap.of(1, 0L),
                  null,
                  ImmutableMap.of(1, ByteBuffer.allocate(1)),
                  ImmutableMap.of(1, ByteBuffer.allocate(1))))
          .build();

  @Test
  void testRoundtripWithoutMetadata() throws IOException {
    WriteResult writeResult = WriteResult.builder().addDataFiles(DATA_FILE).build();

    WriteResultSerializer serializer = new WriteResultSerializer();
    WriteResult copy =
        serializer.deserialize(serializer.getVersion(), serializer.serialize(writeResult));

    assertThat(copy.dataFiles()).hasSize(1);
    assertThat(copy.dataFiles()[0].path()).isEqualTo("/path/to/data-1.parquet");
    assertThat(copy.dataFiles()[0].recordCount()).isEqualTo(42L);
  }

  @Test
  void testRoundtripWithMetadata() throws IOException {
    WriteResult writeResult = WriteResult.builder().addDataFiles(DATA_FILE).build();
    Map<String, String> metadata = ImmutableMap.of("watermark", "1000", "quality", "high");

    WriteResultSerializer serializer = new WriteResultSerializer();

    WriteObserverMetadataHolder.set(metadata);
    byte[] serialized = serializer.serialize(writeResult);

    WriteResult copy = serializer.deserialize(serializer.getVersion(), serialized);
    Map<String, String> recoveredMetadata = WriteObserverMetadataHolder.getAndClear();

    assertThat(copy.dataFiles()).hasSize(1);
    assertThat(copy.dataFiles()[0].path()).isEqualTo("/path/to/data-1.parquet");
    assertThat(recoveredMetadata)
        .containsEntry("watermark", "1000")
        .containsEntry("quality", "high");
  }

  @Test
  void testV1BackwardCompatibility() throws IOException {
    WriteResult writeResult = WriteResult.builder().addDataFiles(DATA_FILE).build();

    byte[] v1Bytes = InstantiationUtil.serializeObject(writeResult);

    WriteResultSerializer serializer = new WriteResultSerializer();
    WriteResult copy = serializer.deserialize(1, v1Bytes);

    assertThat(copy.dataFiles()).hasSize(1);
    assertThat(copy.dataFiles()[0].path()).isEqualTo("/path/to/data-1.parquet");
    assertThat(copy.dataFiles()[0].recordCount()).isEqualTo(42L);
  }

  @Test
  void testUnsupportedVersion() {
    WriteResult writeResult = WriteResult.builder().addDataFiles(DATA_FILE).build();

    WriteResultSerializer serializer = new WriteResultSerializer();
    assertThatThrownBy(() -> serializer.deserialize(-1, serializer.serialize(writeResult)))
        .hasMessage("Unrecognized version or corrupt state: -1")
        .isInstanceOf(IOException.class);
  }
}
