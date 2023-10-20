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
package org.apache.iceberg.nessie;

import java.util.List;
import javax.annotation.Nullable;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.error.ReferenceConflicts;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.ContentKey;

@FunctionalInterface
interface NessieConflictHandler<E extends Exception> {

  /**
   * Handles a single conflict using the given handler. Returns {@code true} if there was only one
   * conflict with a known conflict type, and that conflict was successfully handled; returns {@code
   * false} otherwise. Handlers can also choose to handle the conflict by throwing if that's the
   * appropriate action.
   */
  static <E extends Exception> boolean handleSingle(
      NessieReferenceConflictException ex, NessieConflictHandler<E> handler) throws E {
    ReferenceConflicts referenceConflicts = ex.getErrorDetails();
    if (referenceConflicts == null) {
      return false;
    }

    List<Conflict> conflicts = referenceConflicts.conflicts();
    if (conflicts.size() != 1) {
      return false;
    }

    Conflict conflict = conflicts.get(0);
    Conflict.ConflictType conflictType = conflict.conflictType();
    if (conflictType != null) {
      return handler.handle(conflictType, conflict.key());
    }
    return false;
  }

  boolean handle(Conflict.ConflictType conflictType, @Nullable ContentKey conflictingKey) throws E;
}
