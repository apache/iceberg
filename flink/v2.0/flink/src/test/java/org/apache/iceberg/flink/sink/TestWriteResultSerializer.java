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

import static org.apache.iceberg.TestBase.SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.WriteResult;
import org.junit.jupiter.api.Test;

public class TestWriteResultSerializer {
  private static final PartitionSpec SPEC_1 =
      PartitionSpec.builderFor(SCHEMA).withSpecId(0).bucket("data", 2).build();
  private static final PartitionSpec SPEC_2 =
      PartitionSpec.builderFor(SCHEMA).withSpecId(1).bucket("data", 5).build();

  private static final DataFile FILE_1 =
      DataFiles.builder(SPEC_1)
          .withPath("/path/to/data-1.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("data_bucket=0")
          .withRecordCount(1)
          .build();
  private static final DataFile FILE_2 =
      DataFiles.builder(SPEC_2)
          .withPath("/path/to/data-2.parquet")
          .withFileSizeInBytes(11)
          .withPartitionPath("data_bucket=3")
          .withRecordCount(1)
          .build();

  private static final DeleteFile FILE_1_DELETES =
      FileMetadata.deleteFileBuilder(SPEC_1)
          .ofPositionDeletes()
          .withPath("/path/to/data-1-deletes.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("data_bucket=0")
          .withRecordCount(1)
          .build();

  private static final WriteResult WRITE_RESULT =
      WriteResult.builder()
          .addDataFiles(FILE_1, FILE_2)
          .addDeleteFiles(FILE_1_DELETES)
          .addReferencedDataFiles("foo", "bar")
          .addRewrittenDeleteFiles(FILE_1_DELETES)
          .build();

  @Test
  public void testRoundTripSerialize() throws IOException {
    WriteResultSerializer serializer = new WriteResultSerializer();

    WriteResult copy =
        serializer.deserialize(serializer.getVersion(), serializer.serialize(WRITE_RESULT));
    assertThat(copy).isEqualTo(WRITE_RESULT);
  }

  @Test
  void testUnsupportedVersion() {
    WriteResultSerializer serializer = new WriteResultSerializer();

    assertThatThrownBy(() -> serializer.deserialize(-1, serializer.serialize(WRITE_RESULT)))
        .hasMessage("Unrecognized version or corrupt state: -1")
        .isInstanceOf(IOException.class);
  }
}
