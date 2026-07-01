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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class TestIcebergCommittableSerializer {

  private static final byte[] TEST_MANIFEST = new byte[] {1, 2, 3, 4};
  private static final String JOB_ID = "test-job-id";
  private static final String OPERATOR_ID = "test-operator-id";
  private static final long CHECKPOINT_ID = 42L;

  @Test
  void testRoundtripWithoutMetadata() throws IOException {
    IcebergCommittable committable =
        new IcebergCommittable(TEST_MANIFEST, JOB_ID, OPERATOR_ID, CHECKPOINT_ID);

    IcebergCommittableSerializer serializer = new IcebergCommittableSerializer();
    IcebergCommittable copy =
        serializer.deserialize(serializer.getVersion(), serializer.serialize(committable));

    assertThat(copy.manifest()).isEqualTo(TEST_MANIFEST);
    assertThat(copy.jobId()).isEqualTo(JOB_ID);
    assertThat(copy.operatorId()).isEqualTo(OPERATOR_ID);
    assertThat(copy.checkpointId()).isEqualTo(CHECKPOINT_ID);
    assertThat(copy.observerMetadata()).isEmpty();
  }

  @Test
  void testRoundtripWithMetadata() throws IOException {
    Map<String, String> metadata = ImmutableMap.of("watermark", "1000", "quality", "high");
    IcebergCommittable committable =
        new IcebergCommittable(TEST_MANIFEST, JOB_ID, OPERATOR_ID, CHECKPOINT_ID, metadata);

    IcebergCommittableSerializer serializer = new IcebergCommittableSerializer();
    IcebergCommittable copy =
        serializer.deserialize(serializer.getVersion(), serializer.serialize(committable));

    assertThat(copy.manifest()).isEqualTo(TEST_MANIFEST);
    assertThat(copy.jobId()).isEqualTo(JOB_ID);
    assertThat(copy.operatorId()).isEqualTo(OPERATOR_ID);
    assertThat(copy.checkpointId()).isEqualTo(CHECKPOINT_ID);
    assertThat(copy.observerMetadata())
        .containsEntry("watermark", "1000")
        .containsEntry("quality", "high");
  }

  @Test
  void testV1BackwardCompatibility() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
    view.writeUTF(JOB_ID);
    view.writeUTF(OPERATOR_ID);
    view.writeLong(CHECKPOINT_ID);
    view.writeInt(TEST_MANIFEST.length);
    view.write(TEST_MANIFEST);
    byte[] v1Bytes = out.toByteArray();

    IcebergCommittableSerializer serializer = new IcebergCommittableSerializer();
    IcebergCommittable copy = serializer.deserialize(1, v1Bytes);

    assertThat(copy.manifest()).isEqualTo(TEST_MANIFEST);
    assertThat(copy.jobId()).isEqualTo(JOB_ID);
    assertThat(copy.operatorId()).isEqualTo(OPERATOR_ID);
    assertThat(copy.checkpointId()).isEqualTo(CHECKPOINT_ID);
    assertThat(copy.observerMetadata()).isEmpty();
  }

  @Test
  void testUnsupportedVersion() {
    IcebergCommittable committable =
        new IcebergCommittable(TEST_MANIFEST, JOB_ID, OPERATOR_ID, CHECKPOINT_ID);

    IcebergCommittableSerializer serializer = new IcebergCommittableSerializer();
    assertThatThrownBy(() -> serializer.deserialize(-1, serializer.serialize(committable)))
        .hasMessage("Unrecognized version or corrupt state: -1")
        .isInstanceOf(IOException.class);
  }
}
