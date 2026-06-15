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
package org.apache.iceberg.actions;

import java.util.Collection;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.expressions.Expression;

/**
 * An action that removes all data files matching a partition filter from every matching ref (tag
 * and/or branch) without touching the main branch or other unrelated refs.
 *
 * <p>Refs that share the same underlying snapshot are deduplicated: the delete is applied once per
 * unique snapshot and all affected refs are updated in a single {@code ManageSnapshots} commit.
 *
 * <p>The action strictly targets whole-partition files. If the filter matches only some rows within
 * a file, the commit will fail with a {@link org.apache.iceberg.exceptions.ValidationException}.
 * Use an exact partition predicate to avoid this.
 */
public interface DropPartitionFromRefs
    extends Action<DropPartitionFromRefs, DropPartitionFromRefs.Result> {

  /** Which ref types the action should target. */
  enum RefType {
    TAGS,
    BRANCHES,
    ALL
  }

  /**
   * Sets the partition filter. Only files whose partition strictly matches the expression are
   * removed. This must be set before calling {@link #execute()}.
   *
   * @param expr a partition filter expression
   * @return this for method chaining
   */
  DropPartitionFromRefs filter(Expression expr);

  /**
   * Restricts which ref types are targeted. Defaults to {@link RefType#TAGS}.
   *
   * @param type the ref type to target
   * @return this for method chaining
   */
  DropPartitionFromRefs refType(RefType type);

  /**
   * Restricts the action to a specific set of ref names. If not called, all refs of the configured
   * type are targeted.
   *
   * @param refNames ref names to target
   * @return this for method chaining
   */
  default DropPartitionFromRefs refs(Collection<String> refNames) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement refs(Collection)");
  }

  /**
   * When set to {@code true}, plans and reports what would change without committing anything.
   * Defaults to {@code false}.
   *
   * @param enabled whether to run in dry-run mode
   * @return this for method chaining
   */
  DropPartitionFromRefs dryRun(boolean enabled);

  /** The action result that contains a summary of the execution. */
  interface Result {
    /** Returns refs that were updated and the new snapshot ID each now points to. */
    Map<String, Long> updatedRefs();

    /** Returns data files removed across all rewritten manifests. */
    Iterable<DataFile> removedFiles();

    /** Returns the number of manifests rewritten (excludes manifests with no partition overlap). */
    long rewrittenManifestCount();

    /** Returns the number of unique snapshots processed (refs sharing a snapshot count once). */
    long processedSnapshotCount();
  }
}
