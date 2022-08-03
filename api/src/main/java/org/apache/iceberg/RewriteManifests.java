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

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * API for rewriting manifests for a table.
 *
 * <p>This API accumulates manifest files, produces a new {@link Snapshot} of the table described
 * only by the manifest files that were added, and commits that snapshot as the current.
 *
 * <p>This API can be used to rewrite matching manifests according to a clustering function as well
 * as to replace specific manifests. Manifests that are deleted or added directly are ignored during
 * the rewrite process. The set of active files in replaced manifests must be the same as in new
 * manifests.
 *
 * <p>When committing, these changes will be applied to the latest table snapshot. Commit conflicts
 * will be resolved by applying the changes to the new latest snapshot and reattempting the commit.
 */
public interface RewriteManifests extends SnapshotUpdate<RewriteManifests> {
  /**
   * Groups an existing {@link DataFile} by a cluster key produced by a function. The cluster key
   * will determine which data file will be associated with a particular manifest. All data files
   * with the same cluster key will be written to the same manifest (unless the file is large and
   * split into multiple files). Manifests deleted via {@link #deleteManifest(ManifestFile)} or
   * added via {@link #addManifest(ManifestFile)} are ignored during the rewrite process.
   *
   * @param func Function used to cluster data files to manifests.
   * @return this for method chaining
   */
  RewriteManifests clusterBy(Function<DataFile, Object> func);

  /**
   * Determines which existing {@link ManifestFile} for the table should be rewritten. Manifests
   * that do not match the predicate are kept as-is. If this is not called and no predicate is set,
   * then all manifests will be rewritten.
   *
   * @param predicate Predicate used to determine which manifests to rewrite. If true then the
   *     manifest file will be included for rewrite. If false then then manifest is kept as-is.
   * @return this for method chaining
   */
  RewriteManifests rewriteIf(Predicate<ManifestFile> predicate);

  /**
   * Deletes a {@link ManifestFile manifest file} from the table.
   *
   * @param manifest a manifest to delete
   * @return this for method chaining
   */
  RewriteManifests deleteManifest(ManifestFile manifest);

  /**
   * Adds a {@link ManifestFile manifest file} to the table. The added manifest cannot contain new
   * or deleted files.
   *
   * <p>By default, the manifest will be rewritten to ensure all entries have explicit snapshot IDs.
   * In that case, it is always the responsibility of the caller to manage the lifecycle of the
   * original manifest.
   *
   * <p>If manifest entries are allowed to inherit the snapshot ID assigned on commit, the manifest
   * should never be deleted manually if the commit succeeds as it will become part of the table
   * metadata and will be cleaned up on expiry. If the manifest gets merged with others while
   * preparing a new snapshot, it will be deleted automatically if this operation is successful. If
   * the commit fails, the manifest will never be deleted and it is up to the caller whether to
   * delete or reuse it.
   *
   * @param manifest a manifest to add
   * @return this for method chaining
   */
  RewriteManifests addManifest(ManifestFile manifest);
}
