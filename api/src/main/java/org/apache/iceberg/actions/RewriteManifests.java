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

import java.util.function.Predicate;
import org.apache.iceberg.ManifestFile;
import org.immutables.value.Value;

/** An action that rewrites manifests. */
@Value.Enclosing
public interface RewriteManifests
    extends SnapshotUpdate<RewriteManifests, RewriteManifests.Result> {
  /**
   * Rewrites manifests for a given spec id.
   *
   * <p>If not set, defaults to the table's default spec ID.
   *
   * @param specId a spec id
   * @return this for method chaining
   */
  RewriteManifests specId(int specId);

  /**
   * Rewrites only manifests that match the given predicate.
   *
   * <p>If not set, all manifests will be rewritten.
   *
   * @param predicate a predicate
   * @return this for method chaining
   */
  RewriteManifests rewriteIf(Predicate<ManifestFile> predicate);

  /**
   * Passes a location where the staged manifests should be written.
   *
   * <p>If not set, defaults to the table's metadata location.
   *
   * @param stagingLocation a staging location
   * @return this for method chaining
   */
  RewriteManifests stagingLocation(String stagingLocation);

  /** The action result that contains a summary of the execution. */
  @Value.Immutable
  interface Result {
    /** Returns rewritten manifests. */
    Iterable<ManifestFile> rewrittenManifests();

    /** Returns added manifests. */
    Iterable<ManifestFile> addedManifests();
  }
}
