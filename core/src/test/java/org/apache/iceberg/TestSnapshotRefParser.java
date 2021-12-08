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

import org.junit.Assert;
import org.junit.Test;

public class TestSnapshotRefParser {

  @Test
  public void testTagToJsonDefault() {
    String json = "{\"snapshot-id\":1,\"type\":\"tag\"}";
    SnapshotRef ref = SnapshotRef.builderForTag(1L).build();
    Assert.assertEquals("Should be able to serialize default tag",
        json, SnapshotReferenceParser.toJson(ref));
  }

  @Test
  public void testTagToJsonAllFields() {
    String json = "{\"snapshot-id\":1,\"type\":\"tag\",\"max-ref-age-ms\":1}";
    SnapshotRef ref = SnapshotRef.builderForTag(1L)
        .maxRefAgeMs(1L)
        .build();
    Assert.assertEquals("Should be able to serialize tag with all fields",
        json, SnapshotReferenceParser.toJson(ref));
  }

  @Test
  public void testBranchToJsonDefault() {
    String json = "{\"snapshot-id\":1,\"type\":\"branch\"}";
    SnapshotRef ref = SnapshotRef.builderForBranch(1L).build();
    Assert.assertEquals("Should be able to serialize default branch",
        json, SnapshotReferenceParser.toJson(ref));
  }

  @Test
  public void testBranchToJsonAllFields() {
    String json = "{\"snapshot-id\":1,\"type\":\"branch\",\"min-snapshots-to-keep\":2," +
        "\"max-snapshot-age-ms\":3,\"max-ref-age-ms\":4}";
    SnapshotRef ref = SnapshotRef.builderForBranch(1L)
        .minSnapshotsToKeep(2)
        .maxSnapshotAgeMs(3L)
        .maxRefAgeMs(4L)
        .build();
    Assert.assertEquals("Should be able to serialize branch with all fields",
        json, SnapshotReferenceParser.toJson(ref));
  }

  @Test
  public void testTagFromJsonDefault() {
    String json = "{\"snapshot-id\":1,\"type\":\"tag\"}";
    SnapshotRef ref = SnapshotRef.builderForTag(1L).build();
    Assert.assertEquals("Should be able to deserialize default tag",
        ref, SnapshotReferenceParser.fromJson(json));
  }

  @Test
  public void testTagFromJsonAllFields() {
    String json = "{\"snapshot-id\":1,\"type\":\"tag\",\"max-ref-age-ms\":1}";
    SnapshotRef ref = SnapshotRef.builderForTag(1L)
        .maxRefAgeMs(1L)
        .build();
    Assert.assertEquals("Should be able to deserialize tag with all fields",
        ref, SnapshotReferenceParser.fromJson(json));
  }

  @Test
  public void testBranchFromJsonDefault() {
    String json = "{\"snapshot-id\":1,\"type\":\"branch\"}";
    SnapshotRef ref = SnapshotRef.builderForBranch(1L).build();
    Assert.assertEquals("Should be able to deserialize default branch",
        ref, SnapshotReferenceParser.fromJson(json));
  }

  @Test
  public void testBranchFromJsonAllFields() {
    String json = "{\"snapshot-id\":1,\"type\":\"branch\",\"min-snapshots-to-keep\":2," +
        "\"max-snapshot-age-ms\":3,\"max-ref-age-ms\":4}";
    SnapshotRef ref = SnapshotRef.builderForBranch(1L)
        .minSnapshotsToKeep(2)
        .maxSnapshotAgeMs(3L)
        .maxRefAgeMs(4L)
        .build();
    Assert.assertEquals("Should be able to deserialize branch with all fields",
        ref, SnapshotReferenceParser.fromJson(json));
  }

}
