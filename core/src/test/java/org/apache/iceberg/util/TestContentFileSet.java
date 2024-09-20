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
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

/**
 * Testing {@link ContentFileSet} is easier in iceberg-core since the data/delete file builders are
 * located here
 */
public class TestContentFileSet {

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
  private static final DataFile FILE_C =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("/path/to/data-c.parquet")
          .withFileSizeInBytes(3)
          .withRecordCount(3)
          .build();
  private static final DataFile FILE_D =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("/path/to/data-d.parquet")
          .withFileSizeInBytes(4)
          .withRecordCount(4)
          .build();
  private static final DeleteFile FILE_A_DELETES =
      FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
          .ofPositionDeletes()
          .withPath("/path/to/delete-a.parquet")
          .withFileSizeInBytes(1)
          .withRecordCount(1)
          .build();
  private static final DeleteFile FILE_B_DELETES =
      FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
          .ofPositionDeletes()
          .withPath("/path/to/delete-b.parquet")
          .withFileSizeInBytes(2)
          .withRecordCount(2)
          .build();
  private static final DeleteFile FILE_C_DELETES =
      FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
          .ofPositionDeletes()
          .withPath("/path/to/delete-c.parquet")
          .withFileSizeInBytes(3)
          .withRecordCount(3)
          .build();
  private static final DeleteFile FILE_D_DELETES =
      FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
          .ofPositionDeletes()
          .withPath("/path/to/delete-d.parquet")
          .withFileSizeInBytes(4)
          .withRecordCount(4)
          .build();

  @Test
  public void emptySet() {
    assertThat(ContentFileSet.empty()).isEmpty();
  }

  @Test
  public void testInsertionOrderIsMaintained() {
    ContentFileSet<DataFile> set = ContentFileSet.empty();
    set.addAll(ImmutableList.of(FILE_D, FILE_A, FILE_C));
    set.add(FILE_B);
    set.add(FILE_D);

    assertThat(set).hasSize(4).containsExactly(FILE_D, FILE_A, FILE_C, FILE_B);
  }

  @Test
  public void testClear() {
    ContentFileSet<DataFile> set = ContentFileSet.of(ImmutableList.of(FILE_A, FILE_B));
    set.clear();
    assertThat(set).isEmpty();
  }

  @Test
  public void testRemove() {
    ContentFileSet<DataFile> dataFiles = ContentFileSet.of(ImmutableList.of(FILE_A, FILE_B));
    dataFiles.remove(FILE_C);
    assertThat(dataFiles).containsExactly(FILE_A, FILE_B);
    dataFiles.remove(null);
    assertThat(dataFiles).containsExactly(FILE_A, FILE_B);
    dataFiles.remove(FILE_B);
    assertThat(dataFiles).containsExactly(FILE_A);
    dataFiles.remove(FILE_A);
    assertThat(dataFiles).isEmpty();
  }

  @Test
  public void testRemoveWithDeleteFiles() {
    ContentFileSet<DeleteFile> dataFiles =
        ContentFileSet.of(ImmutableList.of(FILE_A_DELETES, FILE_B_DELETES));
    dataFiles.remove(FILE_C_DELETES);
    assertThat(dataFiles).containsExactly(FILE_A_DELETES, FILE_B_DELETES);
    dataFiles.remove(null);
    assertThat(dataFiles).containsExactly(FILE_A_DELETES, FILE_B_DELETES);
    dataFiles.remove(FILE_B_DELETES);
    assertThat(dataFiles).containsExactly(FILE_A_DELETES);
    dataFiles.remove(FILE_A_DELETES);
    assertThat(dataFiles).isEmpty();
  }

  @Test
  public void testContains() {
    assertThat(ContentFileSet.of(ImmutableList.of(FILE_A, FILE_B)))
        .hasSize(2)
        .contains(FILE_A)
        .contains(FILE_B)
        .doesNotContain(FILE_C)
        .doesNotContain(FILE_D);

    assertThat(ContentFileSet.of(ImmutableList.of(FILE_A_DELETES, FILE_B_DELETES)))
        .hasSize(2)
        .contains(FILE_A_DELETES)
        .contains(FILE_B_DELETES)
        .doesNotContain(FILE_C_DELETES)
        .doesNotContain(FILE_D_DELETES);
  }

  @Test
  public void testToArray() {
    ContentFileSet<DataFile> set = ContentFileSet.of(ImmutableList.of(FILE_B, FILE_A));
    assertThat(set.toArray()).hasSize(2).containsExactly(FILE_B, FILE_A);

    ContentFile<?>[] array = new DataFile[1];
    assertThat(set.toArray(array)).hasSize(2).containsExactly(FILE_B, FILE_A);

    array = new DataFile[5];
    assertThat(set.toArray(array)).hasSize(5).containsExactly(FILE_B, FILE_A, null, null, null);

    array = new DataFile[2];
    assertThat(set.toArray(array)).hasSize(2).containsExactly(FILE_B, FILE_A);
  }

  @Test
  public void testRetainAll() {
    assertThat(ContentFileSet.empty().retainAll(null)).isFalse();

    ContentFileSet<DataFile> set = ContentFileSet.of(ImmutableList.of(FILE_A, FILE_B));
    assertThat(set.retainAll(ImmutableList.of(FILE_C, FILE_D, FILE_A)))
        .as("Set should have changed")
        .isTrue();

    assertThat(set).hasSize(1).contains(FILE_A);

    set = ContentFileSet.of(ImmutableList.of(FILE_A, FILE_B));

    assertThat(set.retainAll(ImmutableList.of(FILE_B, FILE_A)))
        .as("Set should not have changed")
        .isFalse();

    assertThat(set.retainAll(ImmutableList.of(FILE_C, FILE_D)))
        .as("Set should have changed")
        .isTrue();

    assertThat(set).isEmpty();
  }

  @Test
  public void testRetainAllWithDeleteFiles() {
    assertThat(ContentFileSet.empty().retainAll(null)).isFalse();

    ContentFileSet<DeleteFile> set =
        ContentFileSet.of(ImmutableList.of(FILE_A_DELETES, FILE_B_DELETES));
    assertThat(set.retainAll(ImmutableList.of(FILE_C_DELETES, FILE_D_DELETES, FILE_A_DELETES)))
        .as("Set should have changed")
        .isTrue();

    assertThat(set).hasSize(1).contains(FILE_A_DELETES);

    set = ContentFileSet.of(ImmutableList.of(FILE_A_DELETES, FILE_B_DELETES));

    assertThat(set.retainAll(ImmutableList.of(FILE_B_DELETES, FILE_A_DELETES)))
        .as("Set should not have changed")
        .isFalse();

    assertThat(set.retainAll(ImmutableList.of(FILE_C_DELETES, FILE_D_DELETES)))
        .as("Set should have changed")
        .isTrue();

    assertThat(set).isEmpty();
  }

  @Test
  public void testRemoveAll() {
    assertThat(ContentFileSet.empty().removeAll(null)).isFalse();

    ContentFileSet<DataFile> set = ContentFileSet.of(ImmutableList.of(FILE_A, FILE_B));
    assertThat(set.removeAll(ImmutableList.of(FILE_C, FILE_D, FILE_A)))
        .as("Set should have changed")
        .isTrue();

    assertThat(set).hasSize(1).contains(FILE_B);

    set = ContentFileSet.of(ImmutableList.of(FILE_A, FILE_B));
    assertThat(set.removeAll(ImmutableList.of(FILE_C, FILE_D)))
        .as("Set should not have changed")
        .isFalse();

    assertThat(set.removeAll(ImmutableList.of(FILE_B, FILE_A)))
        .as("Set should have changed")
        .isTrue();

    assertThat(set).isEmpty();
  }

  @Test
  public void testRemoveAllWithDeleteFiles() {
    assertThat(ContentFileSet.empty().removeAll(null)).isFalse();

    ContentFileSet<DeleteFile> set =
        ContentFileSet.of(ImmutableList.of(FILE_A_DELETES, FILE_B_DELETES));
    assertThat(set.removeAll(ImmutableList.of(FILE_C_DELETES, FILE_D_DELETES, FILE_A_DELETES)))
        .as("Set should have changed")
        .isTrue();

    assertThat(set).hasSize(1).contains(FILE_B_DELETES);

    set = ContentFileSet.of(ImmutableList.of(FILE_A_DELETES, FILE_B_DELETES));
    assertThat(set.removeAll(ImmutableList.of(FILE_C_DELETES, FILE_D_DELETES)))
        .as("Set should not have changed")
        .isFalse();

    assertThat(set.removeAll(ImmutableList.of(FILE_B_DELETES, FILE_A_DELETES)))
        .as("Set should have changed")
        .isTrue();

    assertThat(set).isEmpty();
  }

  @Test
  public void equalsAndHashCodeOnDataFileSet() {
    ContentFileSet<DataFile> set1 = ContentFileSet.empty();
    ContentFileSet<DataFile> set2 = ContentFileSet.empty();

    assertThat(set1).isEqualTo(set2);
    assertThat(set1.hashCode()).isEqualTo(set2.hashCode());

    set1.add(FILE_A);
    set1.add(FILE_B);
    set1.add(FILE_C);

    set2.add(
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(FILE_A.path().toString())
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build());
    set2.add(
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(FILE_B.path().toString())
            .withFileSizeInBytes(100)
            .withRecordCount(10)
            .build());
    set2.add(
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(FILE_C.path().toString())
            .withFileSizeInBytes(1000)
            .withRecordCount(100)
            .build());

    Set<ContentFile<?>> set3 = Collections.unmodifiableSet(set2);

    Set<ContentFileWrapper<?>> set4 =
        ImmutableSet.of(
            ContentFileWrapper.wrap(
                DataFiles.builder(PartitionSpec.unpartitioned())
                    .withPath(FILE_A.path().toString())
                    .withFileSizeInBytes(5)
                    .withRecordCount(1)
                    .build()),
            ContentFileWrapper.wrap(
                DataFiles.builder(PartitionSpec.unpartitioned())
                    .withPath(FILE_B.path().toString())
                    .withFileSizeInBytes(300)
                    .withRecordCount(2)
                    .build()),
            ContentFileWrapper.wrap(
                DataFiles.builder(PartitionSpec.unpartitioned())
                    .withPath(FILE_C.path().toString())
                    .withFileSizeInBytes(1000)
                    .withRecordCount(100)
                    .build()));

    assertThat(set1).isEqualTo(set2).isEqualTo(set3).isEqualTo(set4);
    assertThat(set1.hashCode())
        .isEqualTo(set2.hashCode())
        .isEqualTo(set3.hashCode())
        .isEqualTo(set4.hashCode());
  }

  @Test
  public void equalsAndHashCodeOnDeleteFileSet() {
    ContentFileSet<DeleteFile> set1 = ContentFileSet.empty();
    ContentFileSet<DeleteFile> set2 = ContentFileSet.empty();

    assertThat(set1).isEqualTo(set2);
    assertThat(set1.hashCode()).isEqualTo(set2.hashCode());

    set1.add(FILE_A_DELETES);
    set1.add(FILE_B_DELETES);
    set1.add(FILE_C_DELETES);

    set2.add(
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath(FILE_A_DELETES.path().toString())
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build());
    set2.add(
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath(FILE_B_DELETES.path().toString())
            .withFileSizeInBytes(100)
            .withRecordCount(10)
            .build());
    set2.add(
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath(FILE_C_DELETES.path().toString())
            .withFileSizeInBytes(1000)
            .withRecordCount(100)
            .build());

    Set<ContentFile<?>> set3 = Collections.unmodifiableSet(set2);

    Set<ContentFileWrapper<?>> set4 =
        ImmutableSet.of(
            ContentFileWrapper.wrap(
                FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                    .ofPositionDeletes()
                    .withPath(FILE_A_DELETES.path().toString())
                    .withFileSizeInBytes(5)
                    .withRecordCount(1)
                    .build()),
            ContentFileWrapper.wrap(
                FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                    .ofPositionDeletes()
                    .withPath(FILE_B_DELETES.path().toString())
                    .withFileSizeInBytes(300)
                    .withRecordCount(2)
                    .build()),
            ContentFileWrapper.wrap(
                FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                    .ofPositionDeletes()
                    .withPath(FILE_C_DELETES.path().toString())
                    .withFileSizeInBytes(1000)
                    .withRecordCount(100)
                    .build()));

    assertThat(set1).isEqualTo(set2).isEqualTo(set3).isEqualTo(set4);
    assertThat(set1.hashCode())
        .isEqualTo(set2.hashCode())
        .isEqualTo(set3.hashCode())
        .isEqualTo(set4.hashCode());
  }
}
