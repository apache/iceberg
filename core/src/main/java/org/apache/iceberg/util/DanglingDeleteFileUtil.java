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
package org.apache.iceberg.util;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public class DanglingDeleteFileUtil {

  private static final List<String> DELETE_COLUMNS =
      ImmutableList.of("file_path", "content_offset", "content_size_in_bytes");

  private DanglingDeleteFileUtil() {}

  /**
   * Dangling delete files can be identified with following steps
   *
   * <ol>
   *   <li>Make a full scan and collect delete files from all file tasks.
   *   <li>Collect all delete file entries skipping files from the previous step.
   * </ol>
   */
  public static DeleteFileSet findDanglingDeletes(Table table, Snapshot snapshot) {
    DeleteFileSet deletes = DeleteFileSet.create();
    TableScan scan = table.newScan().useSnapshot(snapshot.snapshotId());
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      for (FileScanTask task : tasks) {
        deletes.addAll(task.deletes());
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to scan: %s", scan);
    }

    DeleteFileSet danglingDeletes = DeleteFileSet.create();
    for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())
              .select(DELETE_COLUMNS)) {
        for (DeleteFile deleteFile : reader) {
          if (!deletes.contains(deleteFile)) {
            danglingDeletes.add(deleteFile);
          }
        }
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to read manifest: %s", manifest);
      }
    }

    return danglingDeletes;
  }
}
