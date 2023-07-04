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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSnapshotRef {

  @Test
  public void testTagDefault() {
    SnapshotRef ref = SnapshotRef.tagBuilder(1L).build();
    assertThat(ref.snapshotId()).isEqualTo(1L);
    assertThat(ref.type()).isEqualTo(SnapshotRefType.TAG);
    assertThat(ref.minSnapshotsToKeep()).isNull();
    assertThat(ref.maxSnapshotAgeMs()).isNull();
    assertThat(ref.maxRefAgeMs()).isNull();
  }

  @Test
  public void testBranchDefault() {
    SnapshotRef ref = SnapshotRef.branchBuilder(1L).build();
    assertThat(ref.snapshotId()).isEqualTo(1L);
    assertThat(ref.type()).isEqualTo(SnapshotRefType.BRANCH);
    assertThat(ref.minSnapshotsToKeep()).isNull();
    assertThat(ref.maxSnapshotAgeMs()).isNull();
  }

  @Test
  public void testTagWithOverride() {
    SnapshotRef ref = SnapshotRef.branchBuilder(1L).maxRefAgeMs(10L).build();
    assertThat(ref.snapshotId()).isEqualTo(1L);
    assertThat(ref.type()).isEqualTo(SnapshotRefType.BRANCH);
    assertThat((long) ref.maxRefAgeMs()).isEqualTo(10L);
  }

  @Test
  public void testBranchWithOverride() {
    SnapshotRef ref =
        SnapshotRef.branchBuilder(1L)
            .minSnapshotsToKeep(10)
            .maxSnapshotAgeMs(20L)
            .maxRefAgeMs(30L)
            .build();
    assertThat(ref.snapshotId()).isEqualTo(1L);
    assertThat(ref.type()).isEqualTo(SnapshotRefType.BRANCH);
    assertThat((int) ref.minSnapshotsToKeep()).isEqualTo(10);
    assertThat((long) ref.maxSnapshotAgeMs()).isEqualTo(20L);
    assertThat((long) ref.maxRefAgeMs()).isEqualTo(30L);
  }

  @Test
  public void testNoTypeFailure() {
    Assertions.assertThatThrownBy(() -> SnapshotRef.builderFor(1L, null).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Snapshot reference type must not be null");
  }

  @Test
  public void testTagBuildFailures() {
    Assertions.assertThatThrownBy(() -> SnapshotRef.tagBuilder(1L).maxRefAgeMs(-1L).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Max reference age must be greater than 0");

    Assertions.assertThatThrownBy(() -> SnapshotRef.tagBuilder(1L).minSnapshotsToKeep(2).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Tags do not support setting minSnapshotsToKeep");

    Assertions.assertThatThrownBy(() -> SnapshotRef.tagBuilder(1L).maxSnapshotAgeMs(2L).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Tags do not support setting maxSnapshotAgeMs");
  }

  @Test
  public void testBranchBuildFailures() {
    Assertions.assertThatThrownBy(() -> SnapshotRef.branchBuilder(1L).maxSnapshotAgeMs(-1L).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Max snapshot age must be greater than 0 ms");

    Assertions.assertThatThrownBy(
            () -> SnapshotRef.branchBuilder(1L).minSnapshotsToKeep(-1).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Min snapshots to keep must be greater than 0");

    Assertions.assertThatThrownBy(() -> SnapshotRef.branchBuilder(1L).maxRefAgeMs(-1L).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Max reference age must be greater than 0");
  }
}
