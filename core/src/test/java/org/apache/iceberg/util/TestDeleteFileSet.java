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

import java.util.Collections;
import java.util.Set;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

/**
 * Testing {@link DeleteFileSet} is easier in iceberg-core since the delete file builders are
 * located here
 */
public class TestDeleteFileSet {

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
  private static final DeleteFile FILE_C_DELETES =
      FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
          .ofPositionDeletes()
          .withPath("/path/to/data-c-deletes.parquet")
          .withFileSizeInBytes(3)
          .withRecordCount(3)
          .build();
  private static final DeleteFile FILE_D_DELETES =
      FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
          .ofPositionDeletes()
          .withPath("/path/to/data-d-deletes.parquet")
          .withFileSizeInBytes(4)
          .withRecordCount(4)
          .build();

  @Test
  public void emptySet() {
    assertThat(DeleteFileSet.empty()).isEmpty();
    assertThat(DeleteFileSet.empty())
        .doesNotContain(FILE_A_DELETES, FILE_B_DELETES, FILE_C_DELETES);
  }

  @Test
  public void insertionOrderIsMaintained() {
    DeleteFileSet set = DeleteFileSet.empty();
    set.addAll(ImmutableList.of(FILE_D_DELETES, FILE_A_DELETES, FILE_C_DELETES));
    set.add(FILE_B_DELETES);
    set.add(FILE_D_DELETES);

    assertThat(set)
        .hasSize(4)
        .containsExactly(FILE_D_DELETES, FILE_A_DELETES, FILE_C_DELETES, FILE_B_DELETES);
  }

  @Test
  public void clear() {
    DeleteFileSet set = DeleteFileSet.of(ImmutableList.of(FILE_A_DELETES, FILE_B_DELETES));
    set.clear();
    assertThat(set).isEmpty();
  }

  @Test
  public void remove() {
    DeleteFileSet set = DeleteFileSet.of(ImmutableList.of(FILE_A_DELETES, FILE_B_DELETES));
    set.remove(FILE_C_DELETES);
    assertThat(set).containsExactly(FILE_A_DELETES, FILE_B_DELETES);
    set.remove(null);
    assertThat(set).containsExactly(FILE_A_DELETES, FILE_B_DELETES);
    set.remove(FILE_B_DELETES);
    assertThat(set).containsExactly(FILE_A_DELETES);
    set.remove(FILE_A_DELETES);
    assertThat(set).isEmpty();
  }

  @Test
  public void contains() {
    assertThat(DeleteFileSet.of(ImmutableList.of(FILE_A_DELETES, FILE_B_DELETES)))
        .hasSize(2)
        .containsExactly(FILE_A_DELETES, FILE_B_DELETES)
        .doesNotContain(FILE_C_DELETES)
        .doesNotContain(FILE_D_DELETES);
  }

  @Test
  public void toArray() {
    DeleteFileSet set = DeleteFileSet.of(ImmutableList.of(FILE_B_DELETES, FILE_A_DELETES));
    assertThat(set.toArray()).hasSize(2).containsExactly(FILE_B_DELETES, FILE_A_DELETES);

    DeleteFile[] array = new DeleteFile[1];
    assertThat(set.toArray(array)).hasSize(2).containsExactly(FILE_B_DELETES, FILE_A_DELETES);

    array = new DeleteFile[0];
    assertThat(set.toArray(array)).hasSize(2).containsExactly(FILE_B_DELETES, FILE_A_DELETES);

    array = new DeleteFile[5];
    assertThat(set.toArray(array))
        .hasSize(5)
        .containsExactly(FILE_B_DELETES, FILE_A_DELETES, null, null, null);

    array = new DeleteFile[2];
    assertThat(set.toArray(array)).hasSize(2).containsExactly(FILE_B_DELETES, FILE_A_DELETES);
  }

  @Test
  public void retainAll() {
    assertThat(DeleteFileSet.empty().retainAll(null)).isFalse();

    DeleteFileSet set = DeleteFileSet.of(ImmutableList.of(FILE_A_DELETES, FILE_B_DELETES));
    assertThat(set.retainAll(ImmutableList.of(FILE_C_DELETES, FILE_D_DELETES, FILE_A_DELETES)))
        .as("Set should have changed")
        .isTrue();

    assertThat(set).hasSize(1).containsExactly(FILE_A_DELETES);

    set = DeleteFileSet.of(ImmutableList.of(FILE_A_DELETES, FILE_B_DELETES));

    assertThat(set.retainAll(ImmutableList.of(FILE_B_DELETES, FILE_A_DELETES)))
        .as("Set should not have changed")
        .isFalse();

    assertThat(set.retainAll(ImmutableList.of(FILE_C_DELETES, FILE_D_DELETES)))
        .as("Set should have changed")
        .isTrue();

    assertThat(set).isEmpty();
  }

  @Test
  public void removeAll() {
    assertThat(DeleteFileSet.empty().removeAll(null)).isFalse();

    DeleteFileSet set = DeleteFileSet.of(ImmutableList.of(FILE_A_DELETES, FILE_B_DELETES));
    assertThat(set.removeAll(ImmutableList.of(FILE_C_DELETES, FILE_D_DELETES, FILE_A_DELETES)))
        .as("Set should have changed")
        .isTrue();

    assertThat(set).hasSize(1).containsExactly(FILE_B_DELETES);

    set = DeleteFileSet.of(ImmutableList.of(FILE_A_DELETES, FILE_B_DELETES));
    assertThat(set.removeAll(ImmutableList.of(FILE_C_DELETES, FILE_D_DELETES)))
        .as("Set should not have changed")
        .isFalse();

    assertThat(set.removeAll(ImmutableList.of(FILE_B_DELETES, FILE_A_DELETES)))
        .as("Set should have changed")
        .isTrue();

    assertThat(set).isEmpty();
  }

  @Test
  public void equalsAndHashCode() {
    DeleteFileSet set1 = DeleteFileSet.empty();
    DeleteFileSet set2 = DeleteFileSet.empty();

    assertThat(set1).isEqualTo(set2);
    assertThat(set1.hashCode()).isEqualTo(set2.hashCode());

    set1.add(FILE_A_DELETES);
    set1.add(FILE_B_DELETES);
    set1.add(FILE_C_DELETES);

    // different DeleteFile instances but all use the same paths as set1
    set2.add(
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath(FILE_A_DELETES.location())
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build());
    set2.add(
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath(FILE_B_DELETES.location())
            .withFileSizeInBytes(100)
            .withRecordCount(10)
            .build());
    set2.add(
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath(FILE_C_DELETES.location())
            .withFileSizeInBytes(1000)
            .withRecordCount(100)
            .build());

    Set<DeleteFile> set3 = Collections.unmodifiableSet(set2);

    assertThat(set1).isEqualTo(set2).isEqualTo(set3);
    assertThat(set1.hashCode()).isEqualTo(set2.hashCode()).isEqualTo(set3.hashCode());
  }
}
