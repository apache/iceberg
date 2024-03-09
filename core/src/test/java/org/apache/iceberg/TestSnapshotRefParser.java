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

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class TestSnapshotRefParser {

  @Test
  public void testTagToJsonDefault() {
    String json = "{\"snapshot-id\":1,\"type\":\"tag\"}";
    SnapshotRef ref = SnapshotRef.tagBuilder(1L).build();
    Assert.assertEquals(
        "Should be able to serialize default tag", json, SnapshotRefParser.toJson(ref));
  }

  @Test
  public void testTagToJsonAllFields() {
    String json = "{\"snapshot-id\":1,\"type\":\"tag\",\"max-ref-age-ms\":1}";
    SnapshotRef ref = SnapshotRef.tagBuilder(1L).maxRefAgeMs(1L).build();
    Assert.assertEquals(
        "Should be able to serialize tag with all fields", json, SnapshotRefParser.toJson(ref));
  }

  @Test
  public void testBranchToJsonDefault() {
    String json = "{\"snapshot-id\":1,\"type\":\"branch\"}";
    SnapshotRef ref = SnapshotRef.branchBuilder(1L).build();
    Assert.assertEquals(
        "Should be able to serialize default branch", json, SnapshotRefParser.toJson(ref));
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
    Assert.assertEquals(
        "Should be able to serialize branch with all fields", json, SnapshotRefParser.toJson(ref));
  }

  @Test
  public void testTagFromJsonDefault() {
    String json = "{\"snapshot-id\":1,\"type\":\"tag\"}";
    SnapshotRef ref = SnapshotRef.tagBuilder(1L).build();
    Assert.assertEquals(
        "Should be able to deserialize default tag", ref, SnapshotRefParser.fromJson(json));
  }

  @Test
  public void testTagFromJsonAllFields() {
    String json = "{\"snapshot-id\":1,\"type\":\"tag\",\"max-ref-age-ms\":1}";
    SnapshotRef ref = SnapshotRef.tagBuilder(1L).maxRefAgeMs(1L).build();
    Assert.assertEquals(
        "Should be able to deserialize tag with all fields", ref, SnapshotRefParser.fromJson(json));
  }

  @Test
  public void testBranchFromJsonDefault() {
    String json = "{\"snapshot-id\":1,\"type\":\"branch\"}";
    SnapshotRef ref = SnapshotRef.branchBuilder(1L).build();
    Assert.assertEquals(
        "Should be able to deserialize default branch", ref, SnapshotRefParser.fromJson(json));
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
    Assert.assertEquals(
        "Should be able to deserialize branch with all fields",
        ref,
        SnapshotRefParser.fromJson(json));
  }

  @Test
  public void testFailParsingWhenNullOrEmptyJson() {
    String nullJson = null;
    Assertions.assertThatThrownBy(() -> SnapshotRefParser.fromJson(nullJson))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot parse snapshot ref from invalid JSON");

    String emptyJson = "";
    Assertions.assertThatThrownBy(() -> SnapshotRefParser.fromJson(emptyJson))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot parse snapshot ref from invalid JSON");
  }

  @Test
  public void testFailParsingWhenMissingRequiredFields() {
    String refMissingType = "{\"snapshot-id\":1}";
    Assertions.assertThatThrownBy(() -> SnapshotRefParser.fromJson(refMissingType))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot parse missing string");

    String refMissingSnapshotId = "{\"type\":\"branch\"}";
    Assertions.assertThatThrownBy(() -> SnapshotRefParser.fromJson(refMissingSnapshotId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot parse missing long");
  }

  @Test
  public void testFailWhenFieldsHaveInvalidValues() {
    String invalidSnapshotId =
        "{\"snapshot-id\":\"invalid-snapshot-id\",\"type\":\"not-a-valid-tag-type\"}";
    Assertions.assertThatThrownBy(() -> SnapshotRefParser.fromJson(invalidSnapshotId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a long value: snapshot-id: \"invalid-snapshot-id\"");

    String invalidTagType = "{\"snapshot-id\":1,\"type\":\"not-a-valid-tag-type\"}";
    Assertions.assertThatThrownBy(() -> SnapshotRefParser.fromJson(invalidTagType))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid snapshot ref type: not-a-valid-tag-type");

    String invalidRefAge =
        "{\"snapshot-id\":1,\"type\":\"tag\",\"max-ref-age-ms\":\"not-a-valid-value\"}";
    Assertions.assertThatThrownBy(() -> SnapshotRefParser.fromJson(invalidRefAge))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a long value: max-ref-age-ms: \"not-a-valid-value\"");

    String invalidSnapshotsToKeep =
        "{\"snapshot-id\":1,\"type\":\"branch\", "
            + "\"min-snapshots-to-keep\":\"invalid-number\"}";
    Assertions.assertThatThrownBy(() -> SnapshotRefParser.fromJson(invalidSnapshotsToKeep))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to an integer value: min-snapshots-to-keep: \"invalid-number\"");

    String invalidMaxSnapshotAge =
        "{\"snapshot-id\":1,\"type\":\"branch\", " + "\"max-snapshot-age-ms\":\"invalid-age\"}";
    Assertions.assertThatThrownBy(() -> SnapshotRefParser.fromJson(invalidMaxSnapshotAge))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a long value: max-snapshot-age-ms: \"invalid-age\"");
  }
}
