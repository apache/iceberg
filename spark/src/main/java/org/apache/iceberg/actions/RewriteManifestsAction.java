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

/**
 * An action that rewrites manifests in a distributed manner and co-locates metadata for partitions.
 * <p>
 * By default, this action rewrites all manifests for the current partition spec and writes the result
 * to the metadata folder. The behavior can be modified by passing a custom predicate to {@link #rewriteIf(Predicate)}
 * and a custom spec id to {@link #specId(int)}. In addition, there is a way to configure a custom location
 * for new manifests via {@link #stagingLocation}.
 *
 * @deprecated since 0.12.0, will be removed in 0.13.0; use {@link RewriteManifests} instead.
 */
@Deprecated
public class RewriteManifestsAction implements Action<RewriteManifestsAction, RewriteManifestsActionResult> {
  private final RewriteManifests delegate;

  RewriteManifestsAction(RewriteManifests delegate) {
    this.delegate = delegate;
  }

  public RewriteManifestsAction specId(int specId) {
    delegate.specId(specId);
    return this;
  }

  /**
   * Rewrites only manifests that match the given predicate.
   *
   * @param newPredicate a predicate
   * @return this for method chaining
   */
  public RewriteManifestsAction rewriteIf(Predicate<ManifestFile> newPredicate) {
    delegate.rewriteIf(newPredicate);
    return this;
  }

  /**
   * Passes a location where the manifests should be written.
   *
   * @param newStagingLocation a staging location
   * @return this for method chaining
   */
  public RewriteManifestsAction stagingLocation(String newStagingLocation) {
    delegate.stagingLocation(newStagingLocation);
    return this;
  }

  /**
   * Configures whether the action should cache manifest entries used in multiple jobs.
   *
   * @param newUseCaching a flag whether to use caching
   * @return this for method chaining
   */
  public RewriteManifestsAction useCaching(boolean newUseCaching) {
    delegate.option("use-caching", Boolean.toString(newUseCaching));
    return this;
  }

  @Override
  public RewriteManifestsActionResult execute() {
    RewriteManifests.Result result = delegate.execute();
    return RewriteManifestsActionResult.wrap(result);
  }
}
