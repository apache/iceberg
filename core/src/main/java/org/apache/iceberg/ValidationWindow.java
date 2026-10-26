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

import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.SnapshotUtil;

class ValidationWindow {
  private ValidationWindow() {}

  // returns newly added manifests and snapshot IDs between the starting and parent snapshots
  static Pair<List<ManifestFile>, Set<Long>> validationHistory(
      FileIO io,
      TableMetadata base,
      Long startingSnapshotId,
      Set<String> matchingOperations,
      ManifestContent content,
      Snapshot parent) {
    List<ManifestFile> manifests = Lists.newArrayList();
    Set<Long> newSnapshots = Sets.newHashSet();

    Snapshot lastSnapshot = null;
    Iterable<Snapshot> snapshots =
        SnapshotUtil.ancestorsBetween(parent.snapshotId(), startingSnapshotId, base::snapshot);
    for (Snapshot currentSnapshot : snapshots) {
      lastSnapshot = currentSnapshot;

      if (matchingOperations.contains(currentSnapshot.operation())) {
        newSnapshots.add(currentSnapshot.snapshotId());
        if (content == ManifestContent.DATA) {
          for (ManifestFile manifest : currentSnapshot.dataManifests(io)) {
            if (manifest.snapshotId() == currentSnapshot.snapshotId()) {
              manifests.add(manifest);
            }
          }
        } else {
          for (ManifestFile manifest : currentSnapshot.deleteManifests(io)) {
            if (manifest.snapshotId() == currentSnapshot.snapshotId()) {
              manifests.add(manifest);
            }
          }
        }
      }
    }

    ValidationException.check(
        lastSnapshot == null || Objects.equals(lastSnapshot.parentId(), startingSnapshotId),
        "Cannot determine history between starting snapshot %s and the last known ancestor %s",
        startingSnapshotId,
        lastSnapshot != null ? lastSnapshot.snapshotId() : null);

    return Pair.of(manifests, newSnapshots);
  }

  static long startingSequenceNumber(TableMetadata metadata, Long startingSnapshotId) {
    if (startingSnapshotId != null && metadata.snapshot(startingSnapshotId) != null) {
      Snapshot startingSnapshot = metadata.snapshot(startingSnapshotId);
      return startingSnapshot.sequenceNumber();
    } else {
      return TableMetadata.INITIAL_SEQUENCE_NUMBER;
    }
  }
}
