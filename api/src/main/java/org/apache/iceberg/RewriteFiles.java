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

import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * API for replacing files in a table.
 * <p>
 * This API accumulates file additions and deletions, produces a new {@link Snapshot} of the
 * changes, and commits that snapshot as the current.
 * <p>
 * When committing, these changes will be applied to the latest table snapshot. Commit conflicts
 * will be resolved by applying the changes to the new latest snapshot and reattempting the commit.
 * If any of the deleted files are no longer in the latest snapshot when reattempting, the commit
 * will throw a {@link ValidationException}.
 */
public interface RewriteFiles extends SnapshotUpdate<RewriteFiles> {
  /**
   * Add a rewrite that replaces one set of files with another set that contains the same data.
   *
   * @param filesToDelete files that will be replaced (deleted), cannot be null or empty.
   * @param filesToAdd    files that will be added, cannot be null or empty.
   * @return this for method chaining
   */
  default RewriteFiles rewriteFiles(Set<DataFile> filesToDelete, Set<DataFile> filesToAdd) {
    return rewriteFiles(
        FileSet.ofDataFiles(filesToDelete),
        FileSet.ofDataFiles(filesToAdd)
    );
  }

  /**
   * Add a rewrite that replaces one set of files with another set that contains the same data.
   *
   * @param filesToDelete files that will be replaced (deleted), cannot be null empty.
   * @param filesToAdd    files that will be added, cannot be null or empty.
   * @return this for method chaining.
   */
  RewriteFiles rewriteFiles(FileSet filesToDelete, FileSet filesToAdd);

  class FileSet {
    private final Set<DataFile> dataFiles = Sets.newHashSet();
    private final Set<DeleteFile> deleteFiles = Sets.newHashSet();

    public static FileSet ofDataFiles(DataFile... dataFiles) {
      return new FileSet(dataFiles, new DeleteFile[0]);
    }

    public static FileSet ofDataFiles(Iterable<DataFile> dataFiles) {
      return new FileSet(dataFiles, ImmutableSet.of());
    }

    public static FileSet ofDeleteFiles(Iterable<DeleteFile> deleteFiles) {
      return new FileSet(ImmutableSet.of(), deleteFiles);
    }

    public static FileSet ofDeleteFiles(DeleteFile... deleteFiles) {
      return new FileSet(new DataFile[0], deleteFiles);
    }

    public static FileSet of(Iterable<DataFile> dataFiles, Iterable<DeleteFile> deleteFiles) {
      return new FileSet(dataFiles, deleteFiles);
    }

    public static FileSet of(ContentFile<?>... files) {
      List<DataFile> dataFiles = Lists.newArrayList();
      List<DeleteFile> deleteFiles = Lists.newArrayList();

      for (ContentFile<?> file : files) {
        if (file instanceof DataFile) {
          dataFiles.add((DataFile) file);
        } else if (file instanceof DeleteFile) {
          deleteFiles.add((DeleteFile) file);
        } else {
          throw new IllegalArgumentException("Unknown content file: " + file);
        }
      }

      return new FileSet(dataFiles, deleteFiles);
    }

    private FileSet(DataFile[] dataFiles, DeleteFile[] deleteFiles) {
      Collections.addAll(this.dataFiles, dataFiles);
      Collections.addAll(this.deleteFiles, deleteFiles);
    }

    private FileSet(Iterable<DataFile> dataFiles, Iterable<DeleteFile> deleteFiles) {
      Iterables.addAll(this.dataFiles, dataFiles);
      Iterables.addAll(this.deleteFiles, deleteFiles);
    }

    public Set<DataFile> dataFiles() {
      return dataFiles;
    }

    public Set<DeleteFile> deleteFiles() {
      return deleteFiles;
    }

    public boolean isEmpty() {
      return dataFiles.isEmpty() && deleteFiles.isEmpty();
    }
  }
}
