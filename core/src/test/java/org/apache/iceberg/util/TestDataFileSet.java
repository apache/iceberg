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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

/**
 * Testing {@link DataFileSet} is easier in iceberg-core since the data file builders are located
 * here
 */
public class TestDataFileSet {

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

  @Test
  public void emptySet() {
    assertThat(DataFileSet.create()).isEmpty();
    assertThat(DataFileSet.create()).doesNotContain(FILE_A, FILE_B, FILE_C);
  }

  @Test
  public void insertionOrderIsMaintained() {
    DataFileSet set = DataFileSet.create();
    set.addAll(ImmutableList.of(FILE_D, FILE_A, FILE_C));
    set.add(FILE_B);
    set.add(FILE_D);

    assertThat(set).hasSize(4).containsExactly(FILE_D, FILE_A, FILE_C, FILE_B);
  }

  @Test
  public void clear() {
    DataFileSet set = DataFileSet.of(ImmutableList.of(FILE_A, FILE_B));
    set.clear();
    assertThat(set).isEmpty();
  }

  @Test
  public void addAll() {
    DataFileSet empty = DataFileSet.create();
    assertThatThrownBy(() -> empty.add(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid object: null");

    assertThatThrownBy(() -> empty.addAll(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid collection: null");

    assertThatThrownBy(() -> empty.addAll(Collections.singletonList(null)))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid object: null");

    assertThatThrownBy(() -> empty.addAll(Arrays.asList(FILE_A, null)))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid object: null");

    DataFileSet set = DataFileSet.create();
    set.addAll(ImmutableList.of(FILE_B, FILE_A, FILE_C, FILE_A));
    assertThat(set).hasSize(3).containsExactly(FILE_B, FILE_A, FILE_C);
  }

  @Test
  public void contains() {
    DataFileSet set = DataFileSet.of(ImmutableList.of(FILE_A, FILE_B));
    assertThatThrownBy(() -> set.contains(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid object: null");

    assertThat(set)
        .hasSize(2)
        .containsExactly(FILE_A, FILE_B)
        .doesNotContain(FILE_C)
        .doesNotContain(FILE_D);

    assertThat(DataFileSet.of(Arrays.asList(FILE_C, FILE_B, null, FILE_A)))
        .hasSize(3)
        .containsExactly(FILE_C, FILE_B, FILE_A)
        .doesNotContain((DataFile) null);
  }

  @Test
  public void containsAll() {
    DataFileSet set = DataFileSet.of(ImmutableList.of(FILE_A, FILE_B));
    assertThatThrownBy(() -> set.containsAll(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid collection: null");

    assertThatThrownBy(() -> set.containsAll(Collections.singletonList(null)))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid object: null");

    assertThatThrownBy(() -> set.containsAll(Arrays.asList(FILE_A, null)))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid object: null");

    assertThat(set.containsAll(ImmutableList.of(FILE_B, FILE_A))).isTrue();
    assertThat(set.containsAll(ImmutableList.of(FILE_B, FILE_A, FILE_C))).isFalse();
    assertThat(set.containsAll(ImmutableList.of(FILE_B))).isTrue();
  }

  @Test
  public void toArray() {
    DataFileSet set = DataFileSet.of(ImmutableList.of(FILE_B, FILE_A));
    assertThat(set.toArray()).hasSize(2).containsExactly(FILE_B, FILE_A);

    DataFile[] array = new DataFile[1];
    assertThat(set.toArray(array)).hasSize(2).containsExactly(FILE_B, FILE_A);

    array = new DataFile[0];
    assertThat(set.toArray(array)).hasSize(2).containsExactly(FILE_B, FILE_A);

    array = new DataFile[5];
    assertThat(set.toArray(array)).hasSize(5).containsExactly(FILE_B, FILE_A, null, null, null);

    array = new DataFile[2];
    assertThat(set.toArray(array)).hasSize(2).containsExactly(FILE_B, FILE_A);
  }

  @Test
  public void retainAll() {
    DataFileSet empty = DataFileSet.create();
    assertThatThrownBy(() -> empty.retainAll(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid collection: null");

    assertThatThrownBy(() -> empty.retainAll(Collections.singletonList(null)))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid object: null");

    assertThatThrownBy(() -> empty.retainAll(Arrays.asList(FILE_A, null)))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid object: null");

    DataFileSet set = DataFileSet.of(ImmutableList.of(FILE_A, FILE_B));
    assertThat(set.retainAll(ImmutableList.of(FILE_C, FILE_D, FILE_A)))
        .as("Set should have changed")
        .isTrue();

    assertThat(set).hasSize(1).containsExactly(FILE_A);

    set = DataFileSet.of(ImmutableList.of(FILE_A, FILE_B));

    assertThat(set.retainAll(ImmutableList.of(FILE_B, FILE_A)))
        .as("Set should not have changed")
        .isFalse();

    assertThat(set.retainAll(ImmutableList.of(FILE_C, FILE_D)))
        .as("Set should have changed")
        .isTrue();

    assertThat(set).isEmpty();
  }

  @Test
  public void remove() {
    DataFileSet set = DataFileSet.of(ImmutableList.of(FILE_A, FILE_B));
    assertThatThrownBy(() -> set.remove(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid object: null");

    set.remove(FILE_C);
    assertThat(set).containsExactly(FILE_A, FILE_B);
    set.remove(FILE_B);
    assertThat(set).containsExactly(FILE_A);
    set.remove(FILE_A);
    assertThat(set).isEmpty();
  }

  @Test
  public void removeAll() {
    DataFileSet empty = DataFileSet.create();
    assertThatThrownBy(() -> empty.removeAll(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid collection: null");

    assertThatThrownBy(() -> empty.removeAll(Collections.singletonList(null)))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid object: null");

    assertThatThrownBy(() -> empty.removeAll(Arrays.asList(FILE_A, null)))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid object: null");

    DataFileSet set = DataFileSet.of(ImmutableList.of(FILE_A, FILE_B));

    assertThat(set.removeAll(ImmutableList.of(FILE_C, FILE_D, FILE_A)))
        .as("Set should have changed")
        .isTrue();

    assertThat(set).hasSize(1).containsExactly(FILE_B);

    set = DataFileSet.of(ImmutableList.of(FILE_A, FILE_B));
    assertThat(set.removeAll(ImmutableList.of(FILE_C, FILE_D)))
        .as("Set should not have changed")
        .isFalse();

    assertThat(set.removeAll(ImmutableList.of(FILE_B, FILE_A)))
        .as("Set should have changed")
        .isTrue();

    assertThat(set).isEmpty();
  }

  @Test
  public void equalsAndHashCode() {
    DataFileSet set1 = DataFileSet.create();
    DataFileSet set2 = DataFileSet.create();

    assertThat(set1).isEqualTo(set2);
    assertThat(set1.hashCode()).isEqualTo(set2.hashCode());

    set1.add(FILE_A);
    set1.add(FILE_B);
    set1.add(FILE_C);

    // different DataFile instances but all use the same paths as set1
    set2.add(
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(FILE_A.location())
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build());
    set2.add(
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(FILE_B.location())
            .withFileSizeInBytes(100)
            .withRecordCount(10)
            .build());
    set2.add(
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(FILE_C.location())
            .withFileSizeInBytes(1000)
            .withRecordCount(100)
            .build());

    Set<DataFile> set3 = Collections.unmodifiableSet(set2);

    assertThat(set1).isEqualTo(set2).isEqualTo(set3);
    assertThat(set1.hashCode()).isEqualTo(set2.hashCode()).isEqualTo(set3.hashCode());
  }

  @Test
  public void kryoSerialization() throws Exception {
    DataFileSet dataFiles = DataFileSet.of(ImmutableList.of(FILE_C, FILE_B, FILE_A));
    assertThat(TestHelpers.KryoHelpers.roundTripSerialize(dataFiles)).isEqualTo(dataFiles);
  }

  @Test
  public void javaSerialization() throws Exception {
    DataFileSet dataFiles = DataFileSet.of(ImmutableList.of(FILE_C, FILE_B, FILE_A));
    DataFileSet deserialized = TestHelpers.deserialize(TestHelpers.serialize(dataFiles));
    assertThat(deserialized).isEqualTo(dataFiles);
  }
}
