/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg;

import java.util.List;
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

public class SnapshotReferenceUpdate implements UpdateSnapshotReference {

  private final TableOperations ops;
  private TableMetadata base;
  private final List<SnapshotReference> update = Lists.newArrayList();
  private final Set<String> removals = Sets.newHashSet();

  SnapshotReferenceUpdate(TableOperations ops) {
    this.ops = ops;
    this.base = ops.current();
  }

  @Override
  public List<SnapshotReference> apply() {
    this.base = ops.refresh();

    List<SnapshotReference> newRefs = Lists.newArrayList();
    for (SnapshotReference s : base.refs()) {
      if (!removals.contains(s.snapshotName())) {
        newRefs.add(s);
      }
    }

    newRefs.addAll(update);

    return newRefs;
  }

  @Override
  public void commit() {
    ops.commit(base, base.updateSnapshotReference(apply()));
  }

  @Override
  public UpdateSnapshotReference removeSnapshotReference(String name) {
    ValidationException.check(name != null, "Snapshot reference name can't be null");
    ValidationException.check(base.ref(name) != null, "Can't find snapshot reference named %s", name);
    ValidationException.check(!removals.contains(name), "Update multiple properties for snapshot reference should use" +
        " method updateReference(SnapshotReference oldReference, SnapshotReference newReference)");

    removals.add(name);

    return this;
  }

  @Override
  public UpdateSnapshotReference updateMaxSnapshotAgeMs(long maxSnapshotAgeMs, String name) {
    ValidationException.check(maxSnapshotAgeMs > 0, "MaxSnapshotAgeMs must lager than 0");
    ValidationException.check(name != null, "Snapshot reference name can't be null");
    SnapshotReference baseReference = base.ref(name);
    ValidationException.check(baseReference != null, "Can't find snapshot reference named %s", name);
    ValidationException.check(!removals.contains(name), "Update multiple properties for snapshot reference should use" +
        " method updateReference(SnapshotReference oldReference, SnapshotReference newReference)");

    SnapshotReference newRef = SnapshotReference.builderFor(baseReference.snapshotId(), name,
        baseReference.type())
        .withMaxSnapshotAgeMs(maxSnapshotAgeMs)
        .withMinSnapshotsToKeep(baseReference.minSnapshotsToKeep())
        .build();

    update.add(newRef);
    removals.add(name);

    return this;
  }

  @Override
  public UpdateSnapshotReference updateMinSnapshotsToKeep(Integer minSnapshotsToKeep, String name) {
    ValidationException.check(minSnapshotsToKeep > 0, "MinSnapshotsToKeep must lager than 0");
    ValidationException.check(name != null, "Snapshot reference name can't be null");
    SnapshotReference baseReference = base.ref(name);
    ValidationException.check(baseReference != null, "Can't find snapshot reference named %s", name);
    ValidationException.check(!removals.contains(name), "Update multiple properties for snapshot reference should use" +
        " method updateReference(SnapshotReference oldReference, SnapshotReference newReference)");
    ValidationException.check(
        !baseReference.type().equals(SnapshotReferenceType.TAG),
        "TAG type snapshot reference does not support setting minSnapshotsToKeep");

    SnapshotReference newRef = SnapshotReference.builderFor(baseReference.snapshotId(), name,
        baseReference.type())
        .withMaxSnapshotAgeMs(baseReference.maxSnapshotAgeMs())
        .withMinSnapshotsToKeep(minSnapshotsToKeep)
        .build();

    update.add(newRef);
    removals.add(name);

    return this;
  }

  @Override
  public UpdateSnapshotReference updateName(String oldName, String name) {
    ValidationException.check(oldName != null, "Old snapshot reference name can't be null");
    ValidationException.check(name != null, "new snapshot reference name can't be null");
    SnapshotReference baseReference = base.ref(oldName);
    ValidationException.check(baseReference != null, "Can't find snapshot reference named %s", oldName);
    ValidationException.check(!removals.contains(oldName), "Update multiple properties for snapshot reference should use" +
        " method updateReference(SnapshotReference oldReference, SnapshotReference newReference)");

    SnapshotReference newRef = SnapshotReference.builderFor(baseReference.snapshotId(), name,
        baseReference.type())
        .withMaxSnapshotAgeMs(baseReference.maxSnapshotAgeMs())
        .withMinSnapshotsToKeep(baseReference.minSnapshotsToKeep())
        .build();

    update.add(newRef);
    removals.add(oldName);

    return this;
  }

  @Override
  public UpdateSnapshotReference updateReference(
      SnapshotReference oldReference, SnapshotReference newReference) {
    ValidationException.check(oldReference != null, "Old snapshot reference can't be null");
    ValidationException.check(
        !removals.contains(oldReference.snapshotName()),
        "Update multiple properties for snapshot reference should use" +
            " method updateReference(SnapshotReference oldReference, SnapshotReference newReference)");
    ValidationException.check(
        base.ref(oldReference.snapshotName()).equals(oldReference),
        "There is no such snapshot reference %s",
        oldReference.toString());

    removals.add(oldReference.snapshotName());
    update.add(newReference);

    return this;
  }
}
