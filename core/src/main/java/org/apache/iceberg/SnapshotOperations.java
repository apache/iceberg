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

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.SerializableMap;
import org.apache.iceberg.util.SerializableSupplier;

/**
 * SnapshotOperations abstracts access to snapshots for table metadata. This allows a subset of
 * snapshots/refs to be loaded initially for operations that do not require all to be present (e.g.
 * table scans of the current snapshot).
 *
 * <p>In the event that snapshots/refs need to be accessed, the provided supplier will be invoked,
 * which must provide all addressable snapshots for the table.
 */
class SnapshotOperations implements Serializable {
  private List<Snapshot> snapshots;
  private final SerializableSupplier<List<Snapshot>> snapshotsSupplier;
  private Map<Long, Snapshot> snapshotsById;
  private Map<String, SnapshotRef> refs;
  private final SerializableSupplier<Map<String, SnapshotRef>> refsSupplier;
  private boolean loaded;

  private SnapshotOperations(
      List<Snapshot> snapshots,
      SerializableSupplier<List<Snapshot>> snapshotsSupplier,
      Map<String, SnapshotRef> refs,
      SerializableSupplier<Map<String, SnapshotRef>> refsSupplier) {
    this.snapshots = ImmutableList.copyOf(snapshots);
    this.snapshotsSupplier = snapshotsSupplier;
    this.refs = SerializableMap.copyOf(refs);
    this.refsSupplier = refsSupplier;

    this.snapshotsById = indexSnapshotsById(snapshots);
  }

  List<Snapshot> snapshots() {
    ensureLoaded();

    return snapshots;
  }

  Snapshot snapshot(long id) {
    if (!snapshotsById.containsKey(id)) {
      ensureLoaded();
    }

    return snapshotsById.get(id);
  }

  boolean contains(long id) {
    if (!snapshotsById.containsKey(id)) {
      ensureLoaded();
    }

    return snapshotsById.containsKey(id);
  }

  Map<String, SnapshotRef> refs() {
    return refs;
  }

  SnapshotRef ref(String name) {
    return refs.get(name);
  }

  private void ensureLoaded() {
    if (!loaded && snapshotsSupplier != null) {
      this.snapshots = ImmutableList.copyOf(snapshotsSupplier.get());
      this.snapshotsById = indexSnapshotsById(snapshots);
    }

    if (!loaded && refsSupplier != null) {
      this.refs = SerializableMap.copyOf(refsSupplier.get());
    }

    loaded = true;
  }

  void validate(long currentSnapshotId, long lastSequenceNumber) {
    validateSnapshots(lastSequenceNumber);
    validateRefs(currentSnapshotId);
  }

  private void validateSnapshots(long lastSequenceNumber) {
    for (Snapshot snap : snapshots) {
      ValidationException.check(
          snap.sequenceNumber() <= lastSequenceNumber,
          "Invalid snapshot with sequence number %s greater than last sequence number %s",
          snap.sequenceNumber(),
          lastSequenceNumber);
    }
  }

  private void validateRefs(long currentSnapshotId) {
    for (SnapshotRef ref : refs.values()) {
      Preconditions.checkArgument(
          snapshotsById.containsKey(ref.snapshotId()),
          "Snapshot for reference %s does not exist in the existing snapshots list",
          ref);
    }

    SnapshotRef main = refs.get(SnapshotRef.MAIN_BRANCH);
    if (currentSnapshotId != -1) {
      Preconditions.checkArgument(
          main == null || currentSnapshotId == main.snapshotId(),
          "Current snapshot ID does not match main branch (%s != %s)",
          currentSnapshotId,
          main != null ? main.snapshotId() : null);
    } else {
      Preconditions.checkArgument(
          main == null, "Current snapshot is not set, but main branch exists: %s", main);
    }
  }

  private static Map<Long, Snapshot> indexSnapshotsById(List<Snapshot> snapshots) {
    return SerializableMap.copyOf(
        snapshots.stream().collect(Collectors.toMap(Snapshot::snapshotId, Function.identity())));
  }

  public static Builder buildFrom(SnapshotOperations base) {
    return new Builder(base);
  }

  public static Builder buildFromEmpty() {
    return new Builder();
  }

  public static SnapshotOperations empty() {
    return SnapshotOperations.buildFromEmpty().build();
  }

  static class Builder {
    private List<Snapshot> snapshots;
    private SerializableSupplier<List<Snapshot>> snapshotsSupplier;
    private final Map<Long, Snapshot> snapshotsById;
    private Map<String, SnapshotRef> refs;
    private SerializableSupplier<Map<String, SnapshotRef>> refsSupplier;

    Builder() {
      this.snapshots = Lists.newArrayList();
      this.refs = Maps.newHashMap();

      this.snapshotsById = Maps.newHashMap();
    }

    Builder(SnapshotOperations base) {
      this.snapshots = Lists.newArrayList(base.snapshots);
      this.snapshotsSupplier = base.snapshotsSupplier;
      this.refs = Maps.newHashMap(base.refs);
      this.refsSupplier = base.refsSupplier;

      this.snapshotsById = indexSnapshotsById(snapshots);
    }

    Builder snapshots(List<Snapshot> snapshots1) {
      this.snapshots = snapshots1;
      return this;
    }

    Builder snapshotsSupplier(SerializableSupplier<List<Snapshot>> snapshotsSupplier1) {
      this.snapshotsSupplier = snapshotsSupplier1;
      return this;
    }

    Snapshot snapshot(Long id) {
      return snapshotsById.get(id);
    }

    Builder add(Snapshot snapshot) {
      ValidationException.check(
          !snapshotsById.containsKey(snapshot.snapshotId()),
          "Snapshot already exists for id: %s",
          snapshot.snapshotId());

      snapshots.add(snapshot);
      snapshotsById.put(snapshot.snapshotId(), snapshot);

      return this;
    }

    List<Snapshot> remove(Collection<Long> idsToRemove) {
      List<Snapshot> retainedSnapshots =
          Lists.newArrayListWithExpectedSize(snapshots.size() - idsToRemove.size());
      List<Snapshot> removedSnapshots = Lists.newArrayListWithExpectedSize(idsToRemove.size());

      for (Snapshot snapshot : snapshots) {
        long snapshotId = snapshot.snapshotId();
        if (idsToRemove.contains(snapshotId)) {
          Snapshot removed = snapshotsById.remove(snapshotId);
          removedSnapshots.add(removed);
        } else {
          retainedSnapshots.add(snapshot);
        }
      }

      this.snapshots = retainedSnapshots;

      // remove any refs that are no longer valid
      Set<String> danglingRefs = Sets.newHashSet();
      for (Map.Entry<String, SnapshotRef> refEntry : refs.entrySet()) {
        if (!snapshotsById.containsKey(refEntry.getValue().snapshotId())) {
          danglingRefs.add(refEntry.getKey());
        }
      }

      danglingRefs.forEach(this::removeRef);

      return removedSnapshots;
    }

    Builder refs(Map<String, SnapshotRef> refs) {
      this.refs = refs;
      return this;
    }

    SnapshotRef ref(String name) {
      return refs.get(name);
    }

    Builder addRef(String name, SnapshotRef ref) {
      refs.put(name, ref);
      return this;
    }

    SnapshotRef removeRef(String name) {
      return refs.remove(name);
    }

    SnapshotOperations build() {
      return new SnapshotOperations(snapshots, snapshotsSupplier, refs, refsSupplier);
    }
  }
}
