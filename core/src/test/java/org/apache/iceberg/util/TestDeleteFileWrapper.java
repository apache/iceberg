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

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.junit.jupiter.api.Test;

/**
 * Testing {@link DeleteFileWrapper} is easier in iceberg-core since the delete file builders are
 * located here
 */
public class TestDeleteFileWrapper {

  private static final DeleteFile FILE_A_DELETES =
      FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
          .ofPositionDeletes()
          .withPath("/path/to/data-a-deletes.parquet")
          .withFileSizeInBytes(1)
          .withRecordCount(1)
          .build();
  private static final DeleteFile FILE_B_DELETES =
      FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
          .ofPositionDeletes()
          .withPath("/path/to/data-b-deletes.parquet")
          .withFileSizeInBytes(2)
          .withRecordCount(2)
          .build();

  private static DeleteFile dv(String path, long contentOffset, long contentSizeInBytes) {
    return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
        .ofPositionDeletes()
        .withPath(path)
        .withFormat(FileFormat.PUFFIN)
        .withFileSizeInBytes(100)
        .withRecordCount(1)
        .withReferencedDataFile("/path/to/data.parquet")
        .withContentOffset(contentOffset)
        .withContentSizeInBytes(contentSizeInBytes)
        .build();
  }

  @Test
  public void equalsWithSameLocationOffsetAndSize() {
    // different DeleteFile instances with the same location but differing metadata
    DeleteFile copy =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath(FILE_A_DELETES.location())
            .withFileSizeInBytes(1234)
            .withRecordCount(567)
            .build();

    DeleteFileWrapper one = DeleteFileWrapper.wrap(FILE_A_DELETES);
    DeleteFileWrapper two = DeleteFileWrapper.wrap(copy);

    assertThat(one).isEqualTo(two);
    assertThat(one.hashCode()).isEqualTo(two.hashCode());
  }

  @Test
  public void notEqualsWithDifferentLocation() {
    assertThat(DeleteFileWrapper.wrap(FILE_A_DELETES))
        .isNotEqualTo(DeleteFileWrapper.wrap(FILE_B_DELETES));
  }

  @Test
  public void notEqualsWithSameLocationButDifferentContentOffset() {
    // two deletion vectors held in the same Puffin file, differing only by content offset
    DeleteFileWrapper first = DeleteFileWrapper.wrap(dv("/path/to/dv.puffin", 0, 10));
    DeleteFileWrapper second = DeleteFileWrapper.wrap(dv("/path/to/dv.puffin", 10, 10));

    assertThat(first).isNotEqualTo(second);
    assertThat(first.hashCode()).isNotEqualTo(second.hashCode());
  }

  @Test
  public void notEqualsWithSameLocationButDifferentContentSize() {
    // two deletion vectors held in the same Puffin file, differing only by content size
    DeleteFileWrapper first = DeleteFileWrapper.wrap(dv("/path/to/dv.puffin", 0, 10));
    DeleteFileWrapper second = DeleteFileWrapper.wrap(dv("/path/to/dv.puffin", 0, 20));

    assertThat(first).isNotEqualTo(second);
    assertThat(first.hashCode()).isNotEqualTo(second.hashCode());
  }

  @Test
  public void set() {
    DeleteFileWrapper wrapper = DeleteFileWrapper.wrap(FILE_A_DELETES);
    assertThat(wrapper.get()).isEqualTo(FILE_A_DELETES);
    assertThat(wrapper).isEqualTo(DeleteFileWrapper.wrap(FILE_A_DELETES));

    // set returns the same wrapper, allowing reuse for lookups
    assertThat(wrapper.set(FILE_B_DELETES)).isSameAs(wrapper);
    assertThat(wrapper.get()).isEqualTo(FILE_B_DELETES);
    assertThat(wrapper).isEqualTo(DeleteFileWrapper.wrap(FILE_B_DELETES));
    assertThat(wrapper.hashCode()).isEqualTo(DeleteFileWrapper.wrap(FILE_B_DELETES).hashCode());
  }

  @Test
  public void toStringReturnsLocation() {
    assertThat(DeleteFileWrapper.wrap(FILE_A_DELETES)).hasToString(FILE_A_DELETES.location());
  }

  @Test
  public void notEqualsWithNonWrapperAndNull() {
    DeleteFileWrapper wrapper = DeleteFileWrapper.wrap(FILE_A_DELETES);
    assertThat(wrapper).isNotEqualTo(FILE_A_DELETES);
    assertThat(wrapper).isNotEqualTo(null);
  }
}
