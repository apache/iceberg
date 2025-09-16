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

import static org.apache.iceberg.TestBase.SPEC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.io.WriteResult;
import org.junit.jupiter.api.Test;

class TestDynamicCommittableSerializer {
  private static final DynamicCommittable COMMITTABLE =
      new DynamicCommittable(
          new TableKey("table", "branch"),
          WriteResult.builder()
              .addDataFiles(
                  DataFiles.builder(SPEC)
                      .withPath("/path/to/data-a.parquet")
                      .withFileSizeInBytes(10)
                      .withPartitionPath("data_bucket=0")
                      .withRecordCount(1)
                      .build())
              .build(),
          JobID.generate().toHexString(),
          new OperatorID().toHexString(),
          5);

  @Test
  void testRoundtrip() throws IOException {
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
}
