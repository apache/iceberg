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

public class TestSnapshotRef {

  @Test
  public void testTagDefault() {
    SnapshotRef ref = SnapshotRef.tagBuilder(1L).build();
    Assert.assertEquals(1L, ref.snapshotId());
    Assert.assertEquals(SnapshotRefType.TAG, ref.type());
    Assert.assertNull(ref.minSnapshotsToKeep());
    Assert.assertNull(ref.maxSnapshotAgeMs());
    Assert.assertNull(ref.maxRefAgeMs());
  }

  @Test
  public void testBranchDefault() {
    SnapshotRef ref = SnapshotRef.branchBuilder(1L).build();
    Assert.assertEquals(1L, ref.snapshotId());
    Assert.assertEquals(SnapshotRefType.BRANCH, ref.type());
    Assert.assertNull(ref.minSnapshotsToKeep());
    Assert.assertNull(ref.maxSnapshotAgeMs());
  }

  @Test
  public void testTagWithOverride() {
    SnapshotRef ref = SnapshotRef.branchBuilder(1L).maxRefAgeMs(10L).build();
    Assert.assertEquals(1L, ref.snapshotId());
    Assert.assertEquals(SnapshotRefType.BRANCH, ref.type());
    Assert.assertEquals(10L, (long) ref.maxRefAgeMs());
  }

  @Test
  public void testBranchWithOverride() {
    SnapshotRef ref =
        SnapshotRef.branchBuilder(1L)
            .minSnapshotsToKeep(10)
            .maxSnapshotAgeMs(20L)
            .maxRefAgeMs(30L)
            .build();
    Assert.assertEquals(1L, ref.snapshotId());
    Assert.assertEquals(SnapshotRefType.BRANCH, ref.type());
    Assert.assertEquals(10, (int) ref.minSnapshotsToKeep());
    Assert.assertEquals(20L, (long) ref.maxSnapshotAgeMs());
    Assert.assertEquals(30L, (long) ref.maxRefAgeMs());
  }

  @Test
  public void testNoTypeFailure() {
    AssertHelpers.assertThrows(
        "Snapshot reference type must be specified",
        IllegalArgumentException.class,
        "Snapshot reference type must not be null",
        () -> SnapshotRef.builderFor(1L, null).build());
  }

  @Test
  public void testTagBuildFailures() {
    AssertHelpers.assertThrows(
        "Max reference age must be greater than 0 for tag",
        IllegalArgumentException.class,
        "Max reference age must be greater than 0",
        () -> SnapshotRef.tagBuilder(1L).maxRefAgeMs(-1L).build());

    AssertHelpers.assertThrows(
        "Tags do not support setting minSnapshotsToKeep",
        IllegalArgumentException.class,
        "Tags do not support setting minSnapshotsToKeep",
        () -> SnapshotRef.tagBuilder(1L).minSnapshotsToKeep(2).build());

    AssertHelpers.assertThrows(
        "Tags do not support setting maxSnapshotAgeMs",
        IllegalArgumentException.class,
        "Tags do not support setting maxSnapshotAgeMs",
        () -> SnapshotRef.tagBuilder(1L).maxSnapshotAgeMs(2L).build());
  }

  @Test
  public void testBranchBuildFailures() {
    AssertHelpers.assertThrows(
        "Max snapshot age must be greater than 0",
        IllegalArgumentException.class,
        "Max snapshot age must be greater than 0",
        () -> SnapshotRef.branchBuilder(1L).maxSnapshotAgeMs(-1L).build());

    AssertHelpers.assertThrows(
        "Min snapshots to keep must be greater than 0",
        IllegalArgumentException.class,
        "Min snapshots to keep must be greater than 0",
        () -> SnapshotRef.branchBuilder(1L).minSnapshotsToKeep(-1).build());

    AssertHelpers.assertThrows(
        "Max reference age must be greater than 0 for branch",
        IllegalArgumentException.class,
        "Max reference age must be greater than 0",
        () -> SnapshotRef.branchBuilder(1L).maxRefAgeMs(-1L).build());
  }
}
