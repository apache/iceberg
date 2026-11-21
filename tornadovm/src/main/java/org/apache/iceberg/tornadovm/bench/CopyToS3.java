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
package org.apache.iceberg.tornadovm.bench;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Copies a local table of {@link GenerateTables} to an S3 location: its data files and its
 * equality-delete files are uploaded as they are, and a table at the S3 location gets the same
 * schema, format version and commits (an append, then a row delta), with the files at their new
 * paths. Position-delete files and deletion vectors name data files by path, so a table with them
 * is refused.
 *
 * <pre>
 *   CopyToS3 &lt;local table directory&gt; &lt;s3://bucket/path&gt;    (-Ds3.endpoint=... etc.)
 * </pre>
 */
public final class CopyToS3 {
  private CopyToS3() {}

  public static void main(String[] args) throws Exception {
    Table source = MorTables.load(args[0]);
    String target = args[1];
    long start = System.nanoTime();
    List<DataFile> data = Lists.newArrayList();
    Map<String, DeleteFile> deletes = Maps.newLinkedHashMap();
    try (CloseableIterable<FileScanTask> tasks = source.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        data.add(task.file());
        for (DeleteFile delete : task.deletes()) {
          if (delete.content() != FileContent.EQUALITY_DELETES) {
            throw new IllegalArgumentException(
                args[0] + " has position deletes, which name data files by path");
          }
          deletes.putIfAbsent(delete.location(), delete);
        }
      }
    }
    MorTables.dropFromCatalog(target);
    Table copy =
        MorTables.create(target, source.schema(), MorTables.formatVersion(source), Map.of());
    ExecutorService pool = Executors.newFixedThreadPool(16);
    List<Future<String>> uploads = Lists.newArrayList();
    List<DataFile> copiedData = Lists.newArrayList();
    for (DataFile file : data) {
      String path = target + "/data/" + name(file.location());
      uploads.add(pool.submit(() -> upload(copy, file.location(), path)));
      copiedData.add(DataFiles.builder(copy.spec()).copy(file).withPath(path).build());
    }
    List<DeleteFile> copiedDeletes = Lists.newArrayList();
    for (DeleteFile file : deletes.values()) {
      String path = target + "/data/" + name(file.location());
      uploads.add(pool.submit(() -> upload(copy, file.location(), path)));
      // FileMetadata.Builder.copy(DeleteFile) leaves equalityFieldIds out (Iceberg 1.12 and main):
      // without them the copy plans as an equality delete on no column, and planning fails.
      copiedDeletes.add(
          FileMetadata.deleteFileBuilder(copy.spec())
              .copy(file)
              .ofEqualityDeletes(
                  file.equalityFieldIds().stream().mapToInt(Integer::intValue).toArray())
              .withPath(path)
              .build());
    }
    long bytes = 0;
    for (Future<String> upload : uploads) {
      upload.get();
    }
    pool.shutdown();
    for (DataFile file : data) {
      bytes += file.fileSizeInBytes();
    }
    MorTables.append(copy, copiedData);
    if (!copiedDeletes.isEmpty()) {
      RowDelta delta = copy.newRowDelta();
      copiedDeletes.forEach(delta::addDeletes);
      delta.commit();
    }
    System.out.printf(
        "%s -> %s: %d data files (%.2f GB) and %d equality-delete files in %.1f s%n",
        args[0],
        target,
        copiedData.size(),
        bytes / 1e9,
        copiedDeletes.size(),
        (System.nanoTime() - start) / 1e9);
    // S3FileIO's client threads are not daemons and would keep the JVM alive.
    System.exit(0);
  }

  private static String name(String location) {
    return location.substring(location.lastIndexOf('/') + 1);
  }

  private static String upload(Table copy, String local, String path) throws Exception {
    String file = local.startsWith("file:") ? URI.create(local).getPath() : local;
    try (InputStream in = Files.newInputStream(Paths.get(file));
        OutputStream out = copy.io().newOutputFile(path).createOrOverwrite()) {
      in.transferTo(out);
    }
    return path;
  }
}
