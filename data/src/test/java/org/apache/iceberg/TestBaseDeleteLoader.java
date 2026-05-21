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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class TestBaseDeleteLoader {

  private static final String DATA_FILE = "/tmp/data.parquet";

  private static final DeleteFile VALID_DV =
      FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
          .ofPositionDeletes()
          .withFormat(FileFormat.PUFFIN)
          .withPath("/tmp/dv.puffin")
          .withFileSizeInBytes(10)
          .withRecordCount(1)
          .withReferencedDataFile(DATA_FILE)
          .withContentOffset(0L)
          .withContentSizeInBytes(10L)
          .build();

  @Test
  public void loadPositionDeletesRejectsNegativeContentOffset() {
    DeleteFile dv = tamperedDV(-1L, 10L);
    DeleteLoader loader = new BaseDeleteLoader(file -> failingInputFile());

    assertThatThrownBy(() -> loader.loadPositionDeletes(ImmutableList.of(dv), DATA_FILE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("offset must be non-negative");
  }

  @Test
  public void loadPositionDeletesRejectsNegativeContentSize() {
    DeleteFile dv = tamperedDV(0L, -1L);
    DeleteLoader loader = new BaseDeleteLoader(file -> failingInputFile());

    assertThatThrownBy(() -> loader.loadPositionDeletes(ImmutableList.of(dv), DATA_FILE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("length must be non-negative");
  }

  // Wraps a well-formed DV but reports tampered (offset, size). Simulates a corrupted manifest,
  // since the FileMetadata builder now rejects negative values at construction time.
  private static DeleteFile tamperedDV(Long offset, Long size) {
    return new Delegates.DelegatingDeleteFile(VALID_DV) {
      @Override
      public Long contentOffset() {
        return offset;
      }

      @Override
      public Long contentSizeInBytes() {
        return size;
      }
    };
  }

  // The validator must fire before any I/O attempt — if it does not, the loader will call this
  // and the test will surface that as a clear failure instead of an obscure stack trace.
  private static InputFile failingInputFile() {
    throw new AssertionError("DV validation should reject the tampered metadata before any I/O");
  }
}
