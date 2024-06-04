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

import org.apache.iceberg.ManifestFile;

public interface RepairManifests extends SnapshotUpdate<RepairManifests, RepairManifests.Result> {

    /**
     * Passes a location where the staged manifests should be written.
     *
     * <p>If not set, defaults to the table's metadata location.
     *
     * @param stagingLocation a staging location
     * @return this for method chaining
     */
    RepairManifests stagingLocation(String stagingLocation);

    /**
     * Toggle writing manifests or only seeing potential results
     *
     * <p>If not set, defaults false
     *
     * @param value boolean
     * @return this for method chaining
     */
    RepairManifests dryRun(boolean value);

    interface Result {
        /** Returns rewritten manifests. */
        Iterable<ManifestFile> rewrittenManifests();

        /** Returns added manifests. */
        Iterable<ManifestFile> addedManifests();

        /** Returns count of duplicate files removed */
        Long duplicateFilesRemoved();

        /** Returns count of missing files removed */
        Long missingFilesRemoved();
    }
}
