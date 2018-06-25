/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg;

import com.google.common.base.Preconditions;
import com.netflix.iceberg.exceptions.ValidationException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.lang.String.format;

class ReplaceFiles extends BaseReplaceFiles implements RewriteFiles {

  private final TableOperations ops;
  private final Set<CharSequence> pathsToDelete = new HashSet<>();

  ReplaceFiles(TableOperations ops) {
    super(ops);
    this.ops = ops;
  }

  /**
   * @param base TableMetadata of the base snapshot
   * @return list of manifestsToCommit that "may" get committed if commit is called on this instance.
   */
  @Override
  protected List<String> apply(TableMetadata base) {
    final List<String> manifests = super.apply(base);

    if (deletedFiles().size() != pathsToDelete.size()) {
      final String paths = pathsToDelete.stream()
              .filter(path -> !deletedFiles().contains(path))
              .collect(Collectors.joining(","));
      String msg = format("files %s are no longer available in any manifestsToCommit", paths);
      throw new ValidationException(msg);
    }

    return manifests;
  }

  @Override
  protected Predicate<ManifestEntry> shouldDelete(PartitionSpec spec) {
    return manifestEntry -> this.pathsToDelete.contains(manifestEntry.file().path());
  }

  @Override
  public RewriteFiles rewriteFiles(Set<DataFile> filesToDelete, Set<DataFile> filesToAdd) {
    Preconditions.checkArgument(filesToDelete != null && !filesToDelete.isEmpty(), "files to delete can not be null or empty");
    Preconditions.checkArgument(filesToAdd != null && !filesToAdd.isEmpty(), "files to add can not be null or empty");

    this.pathsToDelete.addAll(filesToDelete.stream().map(d -> d.path()).collect(Collectors.toList()));
    this.filesToAdd.addAll(filesToAdd);
    this.hasNewFiles = true;
    return this;
  }
}
