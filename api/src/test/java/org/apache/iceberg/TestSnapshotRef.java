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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    assertThatThrownBy(() -> SnapshotRef.builderFor(1L, null).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Snapshot reference type must not be null");
  }

  @Test
  public void testTagBuildFailures() {
    assertThatThrownBy(() -> SnapshotRef.tagBuilder(1L).maxRefAgeMs(-1L).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Max reference age must be greater than 0");

    assertThatThrownBy(() -> SnapshotRef.tagBuilder(1L).minSnapshotsToKeep(2).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Tags do not support setting minSnapshotsToKeep");

    assertThatThrownBy(() -> SnapshotRef.tagBuilder(1L).maxSnapshotAgeMs(2L).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Tags do not support setting maxSnapshotAgeMs");
  }

  @Test
  public void testBranchBuildFailures() {
    assertThatThrownBy(() -> SnapshotRef.branchBuilder(1L).maxSnapshotAgeMs(-1L).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Max snapshot age must be greater than 0 ms");

    assertThatThrownBy(() -> SnapshotRef.branchBuilder(1L).minSnapshotsToKeep(-1).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Min snapshots to keep must be greater than 0");

    assertThatThrownBy(() -> SnapshotRef.branchBuilder(1L).maxRefAgeMs(-1L).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Max reference age must be greater than 0");
  }

  @Test
  public void testSchemaIdDefault() {
    SnapshotRef tagRef = SnapshotRef.tagBuilder(1L).build();
    assertThat(tagRef.schemaId()).isNull();

    SnapshotRef branchRef = SnapshotRef.branchBuilder(1L).build();
    assertThat(branchRef.schemaId()).isNull();
  }

  @Test
  public void testSchemaIdWithValue() {
    SnapshotRef tagRef = SnapshotRef.tagBuilder(1L).schemaId(5).build();
    assertThat(tagRef.schemaId()).isEqualTo(5);

    SnapshotRef branchRef = SnapshotRef.branchBuilder(1L).schemaId(10).build();
    assertThat(branchRef.schemaId()).isEqualTo(10);
  }

  @Test
  public void testSchemaIdValidation() {
    assertThatThrownBy(() -> SnapshotRef.branchBuilder(1L).schemaId(-1).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("schemaId must be greater than or equal to 0");

    assertThatThrownBy(() -> SnapshotRef.tagBuilder(1L).schemaId(-5).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("schemaId must be greater than or equal to 0");
  }

  @Test
  public void testBuilderFromPreservesSchemaId() {
    SnapshotRef original =
        SnapshotRef.branchBuilder(1L)
            .minSnapshotsToKeep(5)
            .maxSnapshotAgeMs(100L)
            .maxRefAgeMs(200L)
            .schemaId(3)
            .build();

    SnapshotRef copied = SnapshotRef.builderFrom(original).build();
    assertThat(copied.schemaId()).isEqualTo(3);
    assertThat(copied).isEqualTo(original);
  }

  @Test
  public void testBuilderFromWithNewSnapshotPreservesSchemaId() {
    SnapshotRef original =
        SnapshotRef.branchBuilder(1L)
            .minSnapshotsToKeep(5)
            .maxSnapshotAgeMs(100L)
            .maxRefAgeMs(200L)
            .schemaId(3)
            .build();

    SnapshotRef updated = SnapshotRef.builderFrom(original, 2L).build();
    assertThat(updated.snapshotId()).isEqualTo(2L);
    assertThat(updated.schemaId()).isEqualTo(3);
    assertThat(updated.minSnapshotsToKeep()).isEqualTo(5);
    assertThat(updated.maxSnapshotAgeMs()).isEqualTo(100L);
    assertThat(updated.maxRefAgeMs()).isEqualTo(200L);
  }

  @Test
  public void testEqualsAndHashCodeWithSchemaId() {
    SnapshotRef ref1 = SnapshotRef.branchBuilder(1L).schemaId(5).build();
    SnapshotRef ref2 = SnapshotRef.branchBuilder(1L).schemaId(5).build();
    SnapshotRef ref3 = SnapshotRef.branchBuilder(1L).schemaId(10).build();
    SnapshotRef ref4 = SnapshotRef.branchBuilder(1L).build();

    assertThat(ref1).isEqualTo(ref2);
    assertThat(ref1.hashCode()).isEqualTo(ref2.hashCode());

    assertThat(ref1).isNotEqualTo(ref3);
    assertThat(ref1).isNotEqualTo(ref4);
  }

  @Test
  public void testToStringIncludesSchemaId() {
    SnapshotRef ref = SnapshotRef.branchBuilder(1L).schemaId(5).build();
    assertThat(ref.toString()).contains("schemaId=5");

    SnapshotRef refWithoutSchema = SnapshotRef.branchBuilder(1L).build();
    assertThat(refWithoutSchema.toString()).contains("schemaId=null");
  }
}
