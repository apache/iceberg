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

/**
 * API for updating snapshot reference.
 * <p>
 * Apply returns the updated snapshot reference as a {@link SnapshotReference} for validation.
 * <p>
 * When committing, these changes will be applied to the current table metadata. Commit conflicts
 * will be resolved by applying the pending changes to the new table metadata.
 */
public interface UpdateSnapshotReference extends PendingUpdate<Map<String, SnapshotReference>> {

  /**
   * remove snapshotReference.
   *
   * @param name name of snapshot reference
   * @return this
   * @throws IllegalArgumentException If there is no such snapshot reference named name
   */
  UpdateSnapshotReference removeReference(String name);

  /**
   * Update minSnapshotsToKeep of snapshotReference what will be search by referenceName and snapshotReferenceType.
   *
   * @param ageMs       new maxSnapshotAgeMs for snapshot reference. If null will not update.
   * @param numToKeep   new minSnapshotsToKeep for snapshot reference. If null will not update.
   * @param name        name of snapshot reference what will be update
   * @return this
   */

  UpdateSnapshotReference setBranchRetention(String name, Long ageMs, Integer numToKeep);

  /**
   * Update minSnapshotsToKeep of snapshotReference what will be search by referenceName and snapshotReferenceType.
   *
   * @param maxRefAgeMs new maxRefAgeMs for snapshot reference. If null will not update.
   * @param name        name of snapshot reference what will be update
   * @return this
   */

  UpdateSnapshotReference setMaxRefAgeMs(String name, Long maxRefAgeMs);

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
   * @param oldName      old reference name
   * @param newName      new reference name
   * @param newReference new reference
   * @return this
   */
  UpdateSnapshotReference updateReference(String oldName, String newName, SnapshotReference newReference);
}
