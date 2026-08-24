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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;

/** Helpers for restoring deleted columns under their original field IDs. */
public final class UndeleteUtils {
  /** Sentinel index meaning no ancestor snapshot contains the field because lineage was pruned. */
  public static final int PRUNED_LINEAGE = -1;

  /** Sentinel index meaning ancestry cannot be resolved so containment cannot be verified. */
  public static final int UNRESOLVABLE_LINEAGE = Integer.MIN_VALUE;

  private UndeleteUtils() {}

  /**
   * Find the most recent historical definition of a deleted column by name.
   *
   * <p>Names resolve through each schema's name index, so both nested paths and names containing
   * literal dots match. Undotted names match top-level fields only. Schemas are searched from
   * newest to oldest.
   *
   * @param creationOrderSchemas table schemas in creation order, oldest first
   * @param name name of the deleted column
   * @return the newest definition of the column, or null if no schema contains it
   * @throws IllegalArgumentException if the path crosses a list or map boundary
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
   * Find the most recent historical schema whose definition of the deleted column carries exactly
   * this field ID.
   *
   * @param creationOrderSchemas table schemas in creation order, oldest first
   * @param name name of the deleted column
   * @param fieldId the field ID returned by {@link #findDeletedColumn(List, String)}
   * @return the newest schema containing the column under this field ID, or null
   */
  public static Schema findWinningSchema(
      List<Schema> creationOrderSchemas, String name, int fieldId) {
    // schemas are in creation order, search newest first so the latest definition wins
    for (int index = creationOrderSchemas.size() - 1; index >= 0; index -= 1) {
      Types.NestedField candidate = findInSchema(creationOrderSchemas.get(index), name);
      if (candidate != null && candidate.fieldId() == fieldId) {
        return creationOrderSchemas.get(index);
      }
    }

    return null;
  }

  /**
   * Return the ancestor field IDs of a field, ordered from the top-level parent down to its
   * immediate parent.
   *
   * @param schema the schema containing the field
   * @param fieldId the field whose ancestors to collect
   * @return parent IDs excluding the field itself, empty for top-level fields
   */
  public static List<Integer> structAncestorIds(Schema schema, int fieldId) {
    Map<Integer, Integer> idToParent = TypeUtil.indexParents(schema.asStruct());
    List<Integer> chain = Lists.newArrayList();
    Integer current = idToParent.get(fieldId);
    while (current != null) {
      chain.add(current);
      current = idToParent.get(current);
    }

    java.util.Collections.reverse(chain);
    return chain;
  }

  /**
   * Walk snapshot ancestry of the current snapshot newest-first and return the index of the first
   * ancestor whose schema contains the field ID.
   *
   * <p>A table with no snapshots is provably unchanged and returns 0.
   *
   * @param metadata table metadata
   * @param fieldId a field ID to look for
   * @return 0 when the newest ancestor contains the field ID or the table has no snapshots, greater
   *     than 0 when newer ancestors do not contain it, {@code -1} when no ancestor contains it, or
   *     {@link Integer#MIN_VALUE} when an ancestor schema cannot be resolved
   */
  public static int newestContainingSnapshotIndex(TableMetadata metadata, int fieldId) {
    Snapshot currentSnapshot = metadata.currentSnapshot();
    if (currentSnapshot == null) {
      // no snapshots means no rows were ever written, so nothing can contradict the history
      return 0;
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
    Types.NestedField field = schema.findField(name);
    if (field == null || !name.contains(".")) {
      // undotted names can only match top-level fields because the index is keyed by full path
      boolean topLevel =
          field != null && schema.columns().stream().anyMatch(c -> c.fieldId() == field.fieldId());
      return topLevel ? field : null;
    }

    validateStructLineage(schema, field.fieldId(), name);
    return field;
  }

  private static void validateStructLineage(Schema schema, int fieldId, String name) {
    Map<Integer, Integer> idToParent = TypeUtil.indexParents(schema.asStruct());
    Integer parentId = idToParent.get(fieldId);
    while (parentId != null) {
      Types.NestedField parent = schema.findField(parentId);
      if (parent == null || !parent.type().isStructType()) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot undelete columns nested inside %s types: %s",
                parent == null ? "unknown" : parent.type().typeId(), name));
      }

      parentId = idToParent.get(parentId);
    }
  }
}
