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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.junit.jupiter.api.Test;

class TestIcebergCommittableSerializer {

  private static final String JOB_ID = "jobId";
  private static final String SINK_ID = "sinkId";
  private static final String OPERATOR_ID = "operatorId";
  private static final long CHECKPOINT_ID = 5L;
  private static final byte[] MANIFEST = new byte[] {1, 2, 3};

  @Test
  void testLatestVersion() throws IOException {
    IcebergCommittable committable =
        new IcebergCommittable(MANIFEST, JOB_ID, SINK_ID, OPERATOR_ID, CHECKPOINT_ID);
    IcebergCommittableSerializer serializer = new IcebergCommittableSerializer();

    assertThat(serializer.deserialize(serializer.getVersion(), serializer.serialize(committable)))
        .isEqualTo(committable);
  }

  @Test
  void testV1UsesJobIdAsSinkId() throws IOException {
    IcebergCommittableSerializer serializer = new IcebergCommittableSerializer();

    assertThat(serializer.deserialize(1, serializeV1()))
        .isEqualTo(new IcebergCommittable(MANIFEST, JOB_ID, JOB_ID, OPERATOR_ID, CHECKPOINT_ID));
  }

  private static byte[] serializeV1() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
    view.writeUTF(JOB_ID);
    view.writeUTF(OPERATOR_ID);
    view.writeLong(CHECKPOINT_ID);
    view.writeInt(MANIFEST.length);
    view.write(MANIFEST);
    return out.toByteArray();
  }
}
