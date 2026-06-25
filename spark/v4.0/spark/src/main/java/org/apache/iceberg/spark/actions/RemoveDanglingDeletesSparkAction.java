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
package org.apache.iceberg.spark.actions;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.actions.ImmutableRemoveDanglingDeleteFiles;
import org.apache.iceberg.actions.RemoveDanglingDeleteFiles;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.util.DeleteFileSet;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An action that removes dangling delete files from the current snapshot. A delete file is dangling
 * if its deletes no longer applies to any live data files.
 */
class RemoveDanglingDeletesSparkAction
    extends BaseSnapshotUpdateSparkAction<RemoveDanglingDeletesSparkAction>
    implements RemoveDanglingDeleteFiles {

  private static final Logger LOG = LoggerFactory.getLogger(RemoveDanglingDeletesSparkAction.class);
  private final Table table;

  protected RemoveDanglingDeletesSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
  }

  @Override
  protected RemoveDanglingDeletesSparkAction self() {
    return this;
  }

  @Override
  public Result execute() {
    String desc = String.format("Removing dangling delete files in %s", table.name());
    JobGroupInfo info = newJobGroupInfo("REMOVE-DELETES", desc);
    return withJobGroupInfo(info, this::doExecute);
  }

  Result doExecute() {
    RewriteFiles rewriteFiles = table.newRewrite();
    DeleteFileSet danglingDeletes = DeleteFileSet.create();
    danglingDeletes.addAll(findDanglingDeletes());

    for (DeleteFile deleteFile : danglingDeletes) {
      LOG.debug("Removing dangling delete file {}", deleteFile.location());
      rewriteFiles.deleteFile(deleteFile);
    }

    if (!danglingDeletes.isEmpty()) {
      commit(rewriteFiles);
    }

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
  private List<DeleteFile> findDanglingDeletes() {
    TableScan scan = table.newScan();

    DeleteFileSet deletes = DeleteFileSet.create();
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      for (FileScanTask task : tasks) {
        deletes.addAll(task.deletes());
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to scan: %s", scan);
    }

    DeleteFileSet danglingDeletes = DeleteFileSet.create();
    for (ManifestFile manifest : scan.snapshot().deleteManifests(table.io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
        for (DeleteFile deleteFile : reader) {
          if (!deletes.contains(deleteFile)) {
            danglingDeletes.add(deleteFile);
          }
        }
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to read manifest: %s", manifest);
      }
    }

    return danglingDeletes.stream().toList();
  }
}
