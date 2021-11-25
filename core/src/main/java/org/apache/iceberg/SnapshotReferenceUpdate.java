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

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

public class SnapshotReferenceUpdate implements UpdateSnapshotReference {

  private final TableOperations ops;
  private TableMetadata base;
  private final Map<String, SnapshotReference> update = Maps.newHashMap();
  private final Set<String> removals = Sets.newHashSet();

  SnapshotReferenceUpdate(TableOperations ops) {
    this.ops = ops;
    this.base = ops.current();
  }

  @Override
  public Map<String, SnapshotReference> apply() {
    this.base = ops.refresh();

    Map<String, SnapshotReference> newRefs = Maps.newHashMap();
    for (String s : base.refs().keySet()) {
      if (!removals.contains(s)) {
        newRefs.put(s, base.refs().get(s));
      }
    }

    newRefs.putAll(update);

    return newRefs;
  }

  @Override
  public void commit() {
    ops.commit(base, base.updateSnapshotReference(apply()));
  }

  @Override
  public UpdateSnapshotReference removeReference(String name) {
    ValidationException.check(name != null, "Snapshot reference name can't be null");
    ValidationException.check(base.refs().containsKey(name), "Can't find snapshot reference named %s", name);
    ValidationException.check(!removals.contains(name), "Update multiple properties for snapshot reference should use" +
        " method updateReference(SnapshotReference oldReference, SnapshotReference newReference)");

    removals.add(name);

    return this;
  }

  @Override
  public UpdateSnapshotReference setRetention(String name, Long ageMs, Integer numToKeep, Long maxRefAgeMs) {
    ValidationException.check(name != null, "Snapshot reference name can't be null");
    SnapshotReference baseReference = base.refs().get(name);
    ValidationException.check(baseReference != null, "Can't find snapshot reference named %s", name);

    SnapshotReference.Builder builder = SnapshotReference.builderFor(
        baseReference.snapshotId(),
        baseReference.type());

    Long newAgeMs = ageMs == null ? baseReference.maxSnapshotAgeMs() : ageMs;
    Integer newNumToKeep = numToKeep == null ? baseReference.minSnapshotsToKeep() : numToKeep;
    Long newMaxRefAgeMs = maxRefAgeMs == null ? baseReference.maxRefAgeMs() : maxRefAgeMs;

    builder.maxSnapshotAgeMs(newAgeMs);
    builder.minSnapshotsToKeep(newNumToKeep);
    builder.maxRefAgeMs(newMaxRefAgeMs);

    SnapshotReference newRef = builder.build();

    update.put(name, newRef);
    removals.add(name);

    return this;
  }

  @Override
  public UpdateSnapshotReference updateName(String oldName, String name) {
    ValidationException.check(oldName != null, "Old snapshot reference name can't be null");
    ValidationException.check(name != null, "new snapshot reference name can't be null");
    SnapshotReference baseReference = base.refs().get(oldName);
    ValidationException.check(baseReference != null, "Can't find snapshot reference named %s", oldName);
    ValidationException.check(
        !removals.contains(oldName),
        "Update multiple properties for snapshot reference should use" +
            " method updateReference(SnapshotReference oldReference, SnapshotReference newReference)");

    SnapshotReference newRef = SnapshotReference.builderFor(baseReference.snapshotId(), baseReference.type())
        .maxSnapshotAgeMs(baseReference.maxSnapshotAgeMs())
        .minSnapshotsToKeep(baseReference.minSnapshotsToKeep())
        .build();

    update.put(name, newRef);
    removals.add(oldName);

    return this;
  }

  @Override
  public UpdateSnapshotReference updateReference(
      String oldName, String newName, SnapshotReference newReference) {
    ValidationException.check(oldName != null, "Old snapshot reference name can't be null");
    ValidationException.check(newName != null, "New snapshot reference name can't be null");
    ValidationException.check(
        !removals.contains(oldName),
        "Update multiple properties for snapshot reference should use" +
            " method updateReference(SnapshotReference oldReference, SnapshotReference newReference)");
    ValidationException.check(
        base.refs().containsKey(oldName),
        "There is no such snapshot reference named %s", oldName);
    ValidationException.check(base.refs().get(oldName).type().equals(newReference.type()), "The type of snapshot " +
        "reference can't change");

    removals.add(oldName);
    update.put(newName, newReference);

    return this;
  }
}
