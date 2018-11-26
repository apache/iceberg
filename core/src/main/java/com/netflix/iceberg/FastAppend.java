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

package com.netflix.iceberg;

import com.google.common.collect.Lists;
import com.netflix.iceberg.exceptions.CommitFailedException;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.io.OutputFile;
import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * {@link AppendFiles Append} implementation that adds a new manifest file for the write.
 * <p>
 * This implementation will attempt to commit 5 times before throwing {@link CommitFailedException}.
 */
class FastAppend extends SnapshotUpdate implements AppendFiles {
  private final PartitionSpec spec;
  private final List<DataFile> newFiles = Lists.newArrayList();
  private String newManifestLocation = null;
  private boolean hasNewFiles = false;

  FastAppend(TableOperations ops) {
    super(ops);
    this.spec = ops.current().spec();
  }

  @Override
  public FastAppend appendFile(DataFile file) {
    this.hasNewFiles = true;
    newFiles.add(file);
    return this;
  }

  @Override
  public List<String> apply(TableMetadata base) {
    String location = writeManifest();

    List<String> newManifests = Lists.newArrayList();
    newManifests.add(location);
    if (base.currentSnapshot() != null) {
      newManifests.addAll(base.currentSnapshot().manifests());
    }

    return newManifests;
  }

  @Override
  protected void cleanUncommitted(Set<String> committed) {
    if (!committed.contains(newManifestLocation)) {
      deleteFile(newManifestLocation);
    }
  }

  private String writeManifest() {
    if (hasNewFiles && newManifestLocation != null) {
      deleteFile(newManifestLocation);
      hasNewFiles = false;
      newManifestLocation = null;
    }

    if (newManifestLocation == null) {
      OutputFile out = manifestPath(0);

      try (ManifestWriter writer = new ManifestWriter(spec, out, snapshotId())) {

        writer.addAll(newFiles);

      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to write manifest: %s", out);
      }

      this.newManifestLocation = out.location();
    }

    return newManifestLocation;
  }
}
