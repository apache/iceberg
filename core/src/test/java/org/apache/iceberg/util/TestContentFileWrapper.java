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
package org.apache.iceberg.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.junit.jupiter.api.Test;

public class TestContentFileWrapper {
  private static final DataFile FILE_A =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(1)
          .withRecordCount(1)
          .build();
  private static final DataFile FILE_B =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("/path/to/data-b.parquet")
          .withFileSizeInBytes(2)
          .withRecordCount(2)
          .build();
  private static final DeleteFile FILE_A_DELETES =
      FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
          .ofPositionDeletes()
          .withPath("/path/to/delete-a.parquet")
          .withFileSizeInBytes(1)
          .withRecordCount(1)
          .build();

  @Test
  public void testEqualsAndHashCodeWithNullWrapper() {
    ContentFileWrapper<DataFile> one = ContentFileWrapper.wrap(null);
    ContentFileWrapper<DataFile> two = ContentFileWrapper.wrap(null);
    // at this point hashCode is not computed yet
    assertThat(one).isEqualTo(two);

    // hashCode is lazily computed and stored
    assertThat(one.hashCode()).isEqualTo(two.hashCode()).isEqualTo(0);
    assertThat(one).isEqualTo(two);

    one.set(FILE_A);
    assertThat(one).isNotEqualTo(two);
    assertThat(one.hashCode()).isNotEqualTo(two.hashCode());
  }

  @Test
  public void testEquals() {
    assertThat(ContentFileWrapper.wrap(FILE_A)).isEqualTo(ContentFileWrapper.wrap(FILE_A));
    assertThat(ContentFileWrapper.wrap(FILE_A)).isNotEqualTo(ContentFileWrapper.wrap(FILE_B));
    assertThat(ContentFileWrapper.wrap(FILE_A))
        .isNotEqualTo(ContentFileWrapper.wrap(FILE_A_DELETES));

    assertThat(ContentFileWrapper.wrap(FILE_A_DELETES))
        .isEqualTo(ContentFileWrapper.wrap(FILE_A_DELETES));
    assertThat(ContentFileWrapper.wrap(FILE_A))
        .isEqualTo(
            ContentFileWrapper.wrap(
                FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                    .ofPositionDeletes()
                    .withPath(FILE_A.path().toString())
                    .withFileSizeInBytes(1)
                    .withRecordCount(1)
                    .build()));
  }

  @Test
  public void testHashCode() {
    assertThat(ContentFileWrapper.wrap(FILE_A).hashCode())
        .isEqualTo(ContentFileWrapper.wrap(FILE_A).hashCode());
    assertThat(ContentFileWrapper.wrap(FILE_A_DELETES).hashCode())
        .isEqualTo(ContentFileWrapper.wrap(FILE_A_DELETES).hashCode());
    assertThat(ContentFileWrapper.wrap(FILE_A).hashCode())
        .isEqualTo(
            ContentFileWrapper.wrap(
                    FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                        .ofPositionDeletes()
                        .withPath(FILE_A.path().toString())
                        .withFileSizeInBytes(1)
                        .withRecordCount(1)
                        .build())
                .hashCode());
  }

  @Test
  public void testWrapperMethods() {
    ContentFileWrapper<DataFile> wrapper = ContentFileWrapper.wrap(FILE_A);
    assertThat(wrapper.content()).isEqualTo(FILE_A.content());
    assertThat(wrapper.columnSizes()).isEqualTo(FILE_A.columnSizes());
    assertThat(wrapper.get()).isEqualTo(FILE_A);
    assertThat(wrapper.path()).isEqualTo(FILE_A.path());
    assertThat(wrapper.specId()).isEqualTo(FILE_A.specId());
    assertThat(wrapper.recordCount()).isEqualTo(FILE_A.recordCount());
    assertThat(wrapper.pos()).isEqualTo(FILE_A.pos());
  }

  @Test
  public void testHashCodeIsRecomputed() {
    ContentFileWrapper<DataFile> wrapper = ContentFileWrapper.wrap(FILE_A);
    assertThat(wrapper.hashCode()).isEqualTo(-824248163);

    wrapper.set(FILE_B);
    assertThat(wrapper.hashCode()).isEqualTo(1663264670);

    wrapper.set(FILE_A);
    assertThat(wrapper.hashCode()).isEqualTo(-824248163);

    wrapper.set(null);
    assertThat(wrapper.hashCode()).isEqualTo(0);

    wrapper.set(FILE_B);
    assertThat(wrapper.hashCode()).isEqualTo(1663264670);
  }
}
