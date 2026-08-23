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
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;

/** Helpers for restoring deleted columns under their original field IDs. */
public final class UndeleteUtils {
  /** Sentinel index meaning no ancestor snapshot contains the field because lineage was pruned. */
  public static final int PRUNED_LINEAGE = -1;

  /** Sentinel index meaning ancestry cannot be resolved so containment cannot be verified. */
  public static final int UNRESOLVABLE_LINEAGE = Integer.MIN_VALUE;

  private static final Splitter DOT = Splitter.on('.');

  private UndeleteUtils() {}

  /**
   * Find the most recent historical definition of a deleted column by name.
   *
   * <p>Dotted names navigate structs level by level within each schema; undotted names match
   * top-level fields only. Schemas are searched from newest to oldest.
   *
   * @param creationOrderSchemas table schemas in creation order, oldest first
   * @param name name of the deleted column
   * @return the newest definition of the column, or null if no schema contains it
   */
  public static Types.NestedField findDeletedColumn(
      List<Schema> creationOrderSchemas, String name) {
    // schemas are in creation order, search newest first so the latest definition wins
    for (int index = creationOrderSchemas.size() - 1; index >= 0; index -= 1) {
      Types.NestedField candidate = findInSchema(creationOrderSchemas.get(index), name);
      if (candidate != null) {
        return candidate;
      }
    }

    return null;
  }

  /**
   * Walk snapshot ancestry of the current snapshot newest-first and return the index of the first
   * ancestor whose schema contains the field ID.
   *
   * @param metadata table metadata
   * @param fieldId a field ID to look for
   * @return 0 when the newest ancestor contains the field ID, greater than 0 when newer ancestors
   *     do not contain it, {@code -1} when no ancestor contains it, or {@link Integer#MIN_VALUE}
   *     when there is no current snapshot or an ancestor schema cannot be resolved
   */
  public static int newestContainingSnapshotIndex(TableMetadata metadata, int fieldId) {
    Snapshot currentSnapshot = metadata.currentSnapshot();
    if (currentSnapshot == null) {
      return UNRESOLVABLE_LINEAGE;
    }

    Map<Integer, Schema> schemasById = metadata.schemasById();
    List<Long> ancestorIds = SnapshotUtil.ancestorIds(currentSnapshot, metadata::snapshot);
    for (int index = 0; index < ancestorIds.size(); index += 1) {
      Schema ancestorSchema = schemaOf(schemasById, metadata.snapshot(ancestorIds.get(index)));
      if (ancestorSchema == null) {
        return UNRESOLVABLE_LINEAGE;
      }

      if (ancestorSchema.findField(fieldId) != null) {
        return index;
      }
    }

    return PRUNED_LINEAGE;
  }

  /**
   * Identify the snapshot that blocks lineage verification, for error reporting on unresolvable
   * ancestry.
   *
   * @return the ID of the first unresolvable ancestor before the first containing ancestor, or null
   */
  static Long unresolvableAncestorId(TableMetadata metadata, int fieldId) {
    Snapshot currentSnapshot = metadata.currentSnapshot();
    if (currentSnapshot == null) {
      return null;
    }

    Map<Integer, Schema> schemasById = metadata.schemasById();
    List<Long> ancestorIds = SnapshotUtil.ancestorIds(currentSnapshot, metadata::snapshot);
    for (int index = 0; index < ancestorIds.size(); index += 1) {
      Snapshot ancestor = metadata.snapshot(ancestorIds.get(index));
      Schema ancestorSchema = schemaOf(schemasById, ancestor);
      if (ancestorSchema == null) {
        return ancestor.snapshotId();
      }

      if (ancestorSchema.findField(fieldId) != null) {
        // later ancestors do not affect verification once the field is found
        return null;
      }
    }

    return null;
  }

  private static Schema schemaOf(Map<Integer, Schema> schemasById, Snapshot snapshot) {
    Integer schemaId = snapshot.schemaId();
    return schemaId != null ? schemasById.get(schemaId) : null;
  }

  private static Types.NestedField findInSchema(Schema schema, String name) {
    List<String> parts = DOT.splitToList(name);
    // undotted names can only match top-level fields because nested names are indexed by path
    Types.NestedField field = schema.findField(parts.get(0));
    for (int depth = 1; field != null && depth < parts.size(); depth += 1) {
      if (!field.type().isStructType()) {
        return null;
      }

      field = field.type().asStructType().field(parts.get(depth));
    }

    return field;
  }
}
