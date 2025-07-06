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

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.util.Sets;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class TestDynamicWriteResultSerializer {

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
  void testRoundtrip() throws IOException {
    DynamicWriteResult dynamicWriteResult =
        new DynamicWriteResult(
            new WriteTarget("table", "branch", 42, 23, false, Sets.newHashSet(1, 2)),
            WriteResult.builder().addDataFiles(DATA_FILE).build());

    DynamicWriteResultSerializer serializer = new DynamicWriteResultSerializer();
    DynamicWriteResult copy =
        serializer.deserialize(serializer.getVersion(), serializer.serialize(dynamicWriteResult));

    assertThat(copy.writeResult().dataFiles()).hasSize(1);
    DataFile dataFile = copy.writeResult().dataFiles()[0];
    // DataFile doesn't implement equals, but we can still do basic checks
    assertThat(dataFile.path()).isEqualTo("/path/to/data-1.parquet");
    assertThat(dataFile.recordCount()).isEqualTo(42L);
  }

  @Test
  void testUnsupportedVersion() throws IOException {
    DynamicWriteResult dynamicWriteResult =
        new DynamicWriteResult(
            new WriteTarget("table", "branch", 42, 23, false, Sets.newHashSet(1, 2)),
            WriteResult.builder().addDataFiles(DATA_FILE).build());

    DynamicWriteResultSerializer serializer = new DynamicWriteResultSerializer();
    assertThatThrownBy(() -> serializer.deserialize(-1, serializer.serialize(dynamicWriteResult)))
        .hasMessage("Unrecognized version or corrupt state: -1")
        .isInstanceOf(IOException.class);
  }
}
