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
package org.apache.iceberg.io;

import static org.apache.iceberg.TestBase.SPEC;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.TestHelpers;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestWriteResult {
  static final DataFile FILE_A =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("data_bucket=0") // easy way to set partition data for now
          .withRecordCount(1)
          .build();
  static final DeleteFile FILE_A_DELETES =
      FileMetadata.deleteFileBuilder(SPEC)
          .ofPositionDeletes()
          .withPath("/path/to/data-a-deletes.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("data_bucket=0") // easy way to set partition data for now
          .withRecordCount(1)
          .build();
  private static final WriteResult WRITE_RESULT =
      WriteResult.builder().addDataFiles(FILE_A).addDeleteFiles(FILE_A_DELETES).build();

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  public void serialization(TestHelpers.RoundTripSerializer<WriteResult> roundTripSerializer)
      throws IOException, ClassNotFoundException {
    assertThat(roundTripSerializer.apply(WRITE_RESULT)).isEqualTo(WRITE_RESULT);
  }
}
