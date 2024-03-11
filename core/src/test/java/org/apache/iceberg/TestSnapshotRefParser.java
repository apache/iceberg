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

public class TestSnapshotRefParser {

  @Test
  public void testTagToJsonDefault() {
    String json = "{\"snapshot-id\":1,\"type\":\"tag\"}";
    SnapshotRef ref = SnapshotRef.tagBuilder(1L).build();
    assertThat(SnapshotRefParser.toJson(ref))
        .as("Should be able to serialize default tag")
        .isEqualTo(json);
  }

  @Test
  public void testTagToJsonAllFields() {
    String json = "{\"snapshot-id\":1,\"type\":\"tag\",\"max-ref-age-ms\":1}";
    SnapshotRef ref = SnapshotRef.tagBuilder(1L).maxRefAgeMs(1L).build();
    assertThat(SnapshotRefParser.toJson(ref))
        .as("Should be able to serialize tag with all fields")
        .isEqualTo(json);
  }

  @Test
  public void testBranchToJsonDefault() {
    String json = "{\"snapshot-id\":1,\"type\":\"branch\"}";
    SnapshotRef ref = SnapshotRef.branchBuilder(1L).build();
    assertThat(SnapshotRefParser.toJson(ref))
        .as("Should be able to serialize default branch")
        .isEqualTo(json);
  }

  @Test
  public void testBranchToJsonAllFields() {
    String json =
        "{\"snapshot-id\":1,\"type\":\"branch\",\"min-snapshots-to-keep\":2,"
            + "\"max-snapshot-age-ms\":3,\"max-ref-age-ms\":4}";
    SnapshotRef ref =
        SnapshotRef.branchBuilder(1L)
            .minSnapshotsToKeep(2)
            .maxSnapshotAgeMs(3L)
            .maxRefAgeMs(4L)
            .build();
    assertThat(SnapshotRefParser.toJson(ref))
        .as("Should be able to serialize branch with all fields")
        .isEqualTo(json);
  }

  @Test
  public void testTagFromJsonDefault() {
    String json = "{\"snapshot-id\":1,\"type\":\"tag\"}";
    SnapshotRef ref = SnapshotRef.tagBuilder(1L).build();
    assertThat(SnapshotRefParser.fromJson(json))
        .as("Should be able to deserialize default tag")
        .isEqualTo(ref);
  }

  @Test
  public void testTagFromJsonAllFields() {
    String json = "{\"snapshot-id\":1,\"type\":\"tag\",\"max-ref-age-ms\":1}";
    SnapshotRef ref = SnapshotRef.tagBuilder(1L).maxRefAgeMs(1L).build();
    assertThat(SnapshotRefParser.fromJson(json))
        .as("Should be able to deserialize tag with all fields")
        .isEqualTo(ref);
  }

  @Test
  public void testBranchFromJsonDefault() {
    String json = "{\"snapshot-id\":1,\"type\":\"branch\"}";
    SnapshotRef ref = SnapshotRef.branchBuilder(1L).build();
    assertThat(SnapshotRefParser.fromJson(json))
        .as("Should be able to deserialize default branch")
        .isEqualTo(ref);
  }

  @Test
  public void testBranchFromJsonAllFields() {
    String json =
        "{\"snapshot-id\":1,\"type\":\"branch\",\"min-snapshots-to-keep\":2,"
            + "\"max-snapshot-age-ms\":3,\"max-ref-age-ms\":4}";
    SnapshotRef ref =
        SnapshotRef.branchBuilder(1L)
            .minSnapshotsToKeep(2)
            .maxSnapshotAgeMs(3L)
            .maxRefAgeMs(4L)
            .build();
    assertThat(SnapshotRefParser.fromJson(json))
        .as("Should be able to deserialize branch with all fields")
        .isEqualTo(ref);
  }

  @Test
  public void testFailParsingWhenNullOrEmptyJson() {
    String nullJson = null;
    assertThatThrownBy(() -> SnapshotRefParser.fromJson(nullJson))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot parse snapshot ref from invalid JSON");

    String emptyJson = "";
    assertThatThrownBy(() -> SnapshotRefParser.fromJson(emptyJson))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot parse snapshot ref from invalid JSON");
  }

  @Test
  public void testFailParsingWhenMissingRequiredFields() {
    String refMissingType = "{\"snapshot-id\":1}";
    assertThatThrownBy(() -> SnapshotRefParser.fromJson(refMissingType))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot parse missing string");

    String refMissingSnapshotId = "{\"type\":\"branch\"}";
    assertThatThrownBy(() -> SnapshotRefParser.fromJson(refMissingSnapshotId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot parse missing long");
  }

  @Test
  public void testFailWhenFieldsHaveInvalidValues() {
    String invalidSnapshotId =
        "{\"snapshot-id\":\"invalid-snapshot-id\",\"type\":\"not-a-valid-tag-type\"}";
    assertThatThrownBy(() -> SnapshotRefParser.fromJson(invalidSnapshotId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a long value: snapshot-id: \"invalid-snapshot-id\"");

    String invalidTagType = "{\"snapshot-id\":1,\"type\":\"not-a-valid-tag-type\"}";
    assertThatThrownBy(() -> SnapshotRefParser.fromJson(invalidTagType))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid snapshot ref type: not-a-valid-tag-type");

    String invalidRefAge =
        "{\"snapshot-id\":1,\"type\":\"tag\",\"max-ref-age-ms\":\"not-a-valid-value\"}";
    assertThatThrownBy(() -> SnapshotRefParser.fromJson(invalidRefAge))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a long value: max-ref-age-ms: \"not-a-valid-value\"");

    String invalidSnapshotsToKeep =
        "{\"snapshot-id\":1,\"type\":\"branch\", "
            + "\"min-snapshots-to-keep\":\"invalid-number\"}";
    assertThatThrownBy(() -> SnapshotRefParser.fromJson(invalidSnapshotsToKeep))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to an integer value: min-snapshots-to-keep: \"invalid-number\"");

    String invalidMaxSnapshotAge =
        "{\"snapshot-id\":1,\"type\":\"branch\", " + "\"max-snapshot-age-ms\":\"invalid-age\"}";
    assertThatThrownBy(() -> SnapshotRefParser.fromJson(invalidMaxSnapshotAge))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a long value: max-snapshot-age-ms: \"invalid-age\"");
  }
}
