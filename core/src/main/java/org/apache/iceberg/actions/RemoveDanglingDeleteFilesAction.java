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

import java.io.IOException;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.DeleteFileSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoveDanglingDeleteFilesAction
    extends BaseSnapshotUpdateAction<RemoveDanglingDeleteFiles, RemoveDanglingDeleteFiles.Result>
    implements RemoveDanglingDeleteFiles {

  private static final Logger LOG = LoggerFactory.getLogger(RemoveDanglingDeleteFilesAction.class);
  private static final Result EMPTY_RESULT =
      ImmutableRemoveDanglingDeleteFiles.Result.builder()
          .removedDeleteFiles(DeleteFileSet.create())
          .build();

  private final Table table;
  private String branch = SnapshotRef.MAIN_BRANCH;

  public RemoveDanglingDeleteFilesAction(Table table) {
    this.table = table;
  }

  @Override
  protected RemoveDanglingDeleteFilesAction self() {
    return this;
  }

  @Override
  protected Table table() {
    return table;
  }

  public RemoveDanglingDeleteFilesAction toBranch(String targetBranch) {
    Preconditions.checkArgument(targetBranch != null, "Invalid branch name: null");
    this.branch = targetBranch;
    return this;
  }

  @Override
  public Result execute() {
    Snapshot snapshot = table.snapshot(branch);
    if (snapshot == null) {
      Preconditions.checkArgument(
          branch.equals(SnapshotRef.MAIN_BRANCH),
          "Cannot remove dangling delete files from branch %s: branch does not exist",
          branch);
      return EMPTY_RESULT;
    }

    DeleteFileSet danglingDeletes = findDanglingDeletes(table, snapshot);
    if (danglingDeletes.isEmpty()) {
      return EMPTY_RESULT;
    }

    RewriteFiles rewriteFiles = table.newRewrite().validateFromSnapshot(snapshot.snapshotId());
    for (DeleteFile deleteFile : danglingDeletes) {
      LOG.debug("Removing dangling delete file {}", deleteFile.location());
      rewriteFiles.deleteFile(deleteFile);
    }

    commit(rewriteFiles.toBranch(branch));

    return ImmutableRemoveDanglingDeleteFiles.Result.builder()
        .removedDeleteFiles(danglingDeletes)
        .build();
  }

  /**
   * Dangling delete files can be identified with following steps
   *
   * <ol>
   *   <li>Make a full scan and collect delete files from all file tasks.
   *   <li>Collect all delete file entries skipping files from the previous step.
   * </ol>
   */
  private static DeleteFileSet findDanglingDeletes(Table table, Snapshot snapshot) {
    DeleteFileSet referencedDeletes = DeleteFileSet.create();
    TableScan scan = table.newScan().useSnapshot(snapshot.snapshotId());
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      for (FileScanTask task : tasks) {
        for (DeleteFile deleteFile : task.deletes()) {
          referencedDeletes.add(deleteFile.copyWithoutStats());
        }
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to scan: %s", scan);
    }

    DeleteFileSet danglingDeletes = DeleteFileSet.create();
    for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
        for (DeleteFile deleteFile : reader) {
          if (!referencedDeletes.contains(deleteFile)) {
            danglingDeletes.add(deleteFile.copyWithoutStats());
          }
        }
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to read manifest: %s", manifest);
      }
    }

    return danglingDeletes;
  }
}
