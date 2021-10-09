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

package org.apache.iceberg.io;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

public class ExpireSnapshotResult implements Serializable {
  private Set<String> manifestsToDelete;
  private Set<String> manifestListsToDelete;

  private Set<String> filesToDelete;

  private ExpireSnapshotResult(Set<String> manifestsToDelete,
      Set<String> manifestListsToDelete,
      Set<String> filesToDelete) {
    this.manifestsToDelete = manifestsToDelete;
    this.manifestListsToDelete = manifestListsToDelete;
    this.filesToDelete = filesToDelete;
  }

  public Set<String> getManifestsToDelete() {
    return manifestsToDelete;
  }

  public Set<String> getManifestListsToDelete() {
    return manifestListsToDelete;
  }

  public Set<String> getFilesToDelete() {
    return filesToDelete;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final Set<String> manifestsToDelete;
    private final Set<String> manifestListsToDelete;

    private final Set<String> filesToDelete;

    private Builder() {
      this.manifestsToDelete = Sets.newHashSet();
      this.manifestListsToDelete = Sets.newHashSet();
      this.filesToDelete = Sets.newHashSet();

    }

    public Builder add(ExpireSnapshotResult result) {
      addManifestFiles(result.manifestsToDelete);
      addManifestListFiles(result.manifestListsToDelete);
      addDataFiles(result.filesToDelete);

      return this;
    }

    public Builder addAll(Iterable<ExpireSnapshotResult> results) {
      results.forEach(this::add);
      return this;
    }

    public Builder addManifestFiles(String... files) {
      Collections.addAll(manifestsToDelete, files);
      return this;
    }

    public Builder addManifestFiles(Iterable<String> files) {
      Iterables.addAll(manifestsToDelete, files);
      return this;
    }

    public Builder addManifestListFiles(String... files) {
      Collections.addAll(manifestListsToDelete, files);
      return this;
    }

    public Builder addManifestListFiles(Iterable<String> files) {
      Iterables.addAll(manifestListsToDelete, files);
      return this;
    }

    public Builder addDataFiles(String... files) {
      Collections.addAll(filesToDelete, files);
      return this;
    }

    public Builder addDataFiles(Iterable<String> files) {
      Iterables.addAll(filesToDelete, files);
      return this;
    }

    public ExpireSnapshotResult build() {
      return new ExpireSnapshotResult(manifestsToDelete, manifestListsToDelete, filesToDelete);
    }
  }

  public boolean isEmpty() {
    return this.filesToDelete.isEmpty() && this.manifestsToDelete.isEmpty() && this.manifestListsToDelete.isEmpty();
  }
}
