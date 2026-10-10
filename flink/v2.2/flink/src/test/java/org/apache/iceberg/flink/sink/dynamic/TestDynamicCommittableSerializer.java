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
package org.apache.iceberg.flink.sink.dynamic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Set;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.junit.jupiter.api.Test;

class TestDynamicCommittableSerializer {
  private static final DynamicCommittable COMMITTABLE =
      new DynamicCommittable(
          new TableKey("table", "branch"),
          new byte[][] {{3, 4}, {5, 6}},
          JobID.generate().toHexString(),
          new OperatorID().toHexString(),
          5);

  @Test
  void testV1() throws IOException {
    var committable =
        new DynamicCommittable(
            new TableKey("table", "branch"),
            new byte[][] {{3, 4}},
            JobID.generate().toHexString(),
            new OperatorID().toHexString(),
            5);
    DynamicCommittableSerializer serializer = new DynamicCommittableSerializer();
    assertThat(serializer.deserialize(1, serializeV1(committable))).isEqualTo(committable);
  }

  @Test
  void testLatestVersion() throws IOException {
    DynamicCommittableSerializer serializer = new DynamicCommittableSerializer();
    assertThat(serializer.deserialize(serializer.getVersion(), serializer.serialize(COMMITTABLE)))
        .isEqualTo(COMMITTABLE);
  }

  @Test
  void testUnsupportedVersion() {
    DynamicCommittableSerializer serializer = new DynamicCommittableSerializer();
    assertThatThrownBy(() -> serializer.deserialize(-1, serializer.serialize(COMMITTABLE)))
        .hasMessage("Unrecognized version or corrupt state: -1")
        .isInstanceOf(IOException.class);
  }

  byte[] serializeV1(DynamicCommittable committable) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);

    // Wrap TableKey into a testing WriteTarget to match the V1 format
    WriteTarget writeTarget =
        new WriteTarget(
            committable.key().tableName(),
            committable.key().branch(),
            -1,
            -1,
            false,
            Set.of(1, 2, 3));
    view.write(serializeV1(writeTarget));

    view.writeUTF(committable.jobId());
    view.writeUTF(committable.operatorId());
    view.writeLong(committable.checkpointId());

    Preconditions.checkArgument(
        committable.manifests().length == 1,
        "V1 serialization format must have only one manifest per committable.");
    view.writeInt(committable.manifests()[0].length);
    view.write(committable.manifests()[0]);

    return out.toByteArray();
  }

  byte[] serializeV1(WriteTarget writeTarget) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);

    view.writeUTF(writeTarget.tableName());
    view.writeUTF(writeTarget.branch());
    view.writeInt(writeTarget.schemaId());
    view.writeInt(writeTarget.specId());
    view.writeBoolean(writeTarget.upsertMode());
    view.writeInt(writeTarget.equalityFields().size());
    for (Integer equalityField : writeTarget.equalityFields()) {
      view.writeInt(equalityField);
    }

    return out.toByteArray();
  }
}
