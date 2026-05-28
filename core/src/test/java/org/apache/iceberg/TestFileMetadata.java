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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class TestFileMetadata {

  @Test
  public void dvBuilderRejectsNegativeContentOffset() {
    assertThatThrownBy(
            () ->
                FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                    .ofPositionDeletes()
                    .withFormat(FileFormat.PUFFIN)
                    .withPath("/tmp/dv.puffin")
                    .withFileSizeInBytes(10)
                    .withRecordCount(1)
                    .withReferencedDataFile("/tmp/data.parquet")
                    .withContentOffset(-1L)
                    .withContentSizeInBytes(10L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Content offset must be non-negative for DV");
  }

  @Test
  public void dvBuilderRejectsNegativeContentSize() {
    assertThatThrownBy(
            () ->
                FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                    .ofPositionDeletes()
                    .withFormat(FileFormat.PUFFIN)
                    .withPath("/tmp/dv.puffin")
                    .withFileSizeInBytes(10)
                    .withRecordCount(1)
                    .withReferencedDataFile("/tmp/data.parquet")
                    .withContentOffset(0L)
                    .withContentSizeInBytes(-1L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Content size must be non-negative for DV");
  }

  @Test
  public void dvBuilderRejectsContentSizeAtIntegerMax() {
    assertThatThrownBy(
            () ->
                FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                    .ofPositionDeletes()
                    .withFormat(FileFormat.PUFFIN)
                    .withPath("/tmp/dv.puffin")
                    .withFileSizeInBytes(10)
                    .withRecordCount(1)
                    .withReferencedDataFile("/tmp/data.parquet")
                    .withContentOffset(0L)
                    .withContentSizeInBytes(Integer.MAX_VALUE)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("less than 2GB");
  }

  @Test
  public void dvBuilderAcceptsValidOffsetAndSize() {
    DeleteFile dv =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withFormat(FileFormat.PUFFIN)
            .withPath("/tmp/dv.puffin")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .withReferencedDataFile("/tmp/data.parquet")
            .withContentOffset(4L)
            .withContentSizeInBytes(4096L)
            .build();

    assertThat(dv.contentOffset()).isEqualTo(4L);
    assertThat(dv.contentSizeInBytes()).isEqualTo(4096L);
    assertThat(dv.format()).isEqualTo(FileFormat.PUFFIN);
  }

  @Test
  public void dvBuilderAcceptsZeroOffsetAndSize() {
    assertThatCode(
            () ->
                FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                    .ofPositionDeletes()
                    .withFormat(FileFormat.PUFFIN)
                    .withPath("/tmp/dv.puffin")
                    .withFileSizeInBytes(10)
                    .withRecordCount(1)
                    .withReferencedDataFile("/tmp/data.parquet")
                    .withContentOffset(0L)
                    .withContentSizeInBytes(0L)
                    .build())
        .doesNotThrowAnyException();
  }
}
