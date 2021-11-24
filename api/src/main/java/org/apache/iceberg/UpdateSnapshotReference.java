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

/**
 * API for updating snapshot reference.
 * <p>
 * Apply returns the updated snapshot reference as a {@link SnapshotReference} for validation.
 * <p>
 * When committing, these changes will be applied to the current table metadata. Commit conflicts
 * will be resolved by applying the pending changes to the new table metadata.
 */
public interface UpdateSnapshotReference extends PendingUpdate<List<SnapshotReference>> {

  /**
   * remove snapshotReference.
   *
   * @param name name of snapshot reference
   * @return this
   * @throws IllegalArgumentException If there is no such snapshot reference named name
   */
  UpdateSnapshotReference removeSnapshotReference(String name);

  /**
   * Update maxSnapshotAgeMs of snapshotReference what will be search by referenceName and snapshotReferenceType.
   *
   * @param maxSnapshotAgeMs new maxSnapshotAgeMs for snapshot reference
   * @param name    name of snapshot reference what will be update
   * @return this
   */
  UpdateSnapshotReference updateMaxSnapshotAgeMs(long maxSnapshotAgeMs, String name);

  /**
   * Update minSnapshotsToKeep of snapshotReference what will be search by referenceName and snapshotReferenceType.
   *
   * @param minSnapshotsToKeep new minSnapshotsToKeep for snapshot reference
   * @param name               name of snapshot reference what will be update
   * @return this
   */
  UpdateSnapshotReference updateMinSnapshotsToKeep(Integer minSnapshotsToKeep, String name);

  /**
   * Update name of snapshotReference what will be search by referenceName and snapshotReferenceType.
   *
   * @param oldName old name of snapshot reference
   * @param name    new name for snapshot reference
   * @return this
   */
  UpdateSnapshotReference updateName(String oldName, String name);

  /**
   * replace old snapshotReference by new snapshotReference
   *
   * @param oldReference old reference
   * @param newReference new reference
   * @return this
   */
  UpdateSnapshotReference updateReference(SnapshotReference oldReference, SnapshotReference newReference);
}
