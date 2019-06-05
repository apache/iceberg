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

/**
 * API for rewriting manifests for a table.
 * <p>
 * This API accumulates manifest files, produces a new {@link Snapshot} of the table
 * described only by the manifest files that were added, and commits that snapshot as the
 * current.
 * <p>
 * When committing, these changes will be applied to the latest table snapshot. Commit conflicts
 * will be resolved by applying the changes to the new latest snapshot and reattempting the commit.
 */
public interface RewriteManifests extends SnapshotUpdate<RewriteManifests> {
  /**
   * Group an existing {@link DataFile} by a partition key. The partition key will determine
   * which data file will be associated with a particular manifest. All files with the same
   * key will be written to the same manifest.
   *
   * @param func Function used to cluster data files to manifests.
   * @return this for method chaining
   */
  RewriteManifests clusterBy(Function<DataFile, Object> func);

  /**
   * Filter which existing {@link ManifestFile} for the table should be rewritten. Manifests
   * that do not match the filter are kept as-is.
   *
   * @param func Function used to filter manifests. This function should return true
   *               to include the manifest file for rewrite and false to keep it as-is
   * @return this for method chaining
   */
  RewriteManifests filter(Function<ManifestFile, Boolean> func);
}
