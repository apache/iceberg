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
package org.apache.iceberg.spark.source;

import java.io.Serializable;
import java.util.Map;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.DeleteFileSet;

/** Wraps a set of rewritable delete files which are relativized to a given table location */
class RewritableDeletes implements Serializable {
  private final String tableLocation;
  private final Map<String, DeleteFileSet> relativizedRewritableDeletes;

  RewritableDeletes(String tableLocation) {
    this.tableLocation = tableLocation;
    this.relativizedRewritableDeletes = Maps.newHashMap();
  }

  /**
   * @param dataFileLocation absolute data file location
   * @param deleteFile delete file with absolute data file location
   * @param specs map from spec ID to partition spec
   */
  public void addRewritableDelete(
      String dataFileLocation, DeleteFile deleteFile, Map<Integer, PartitionSpec> specs) {
    relativizedRewritableDeletes
        .computeIfAbsent(relativizePath(dataFileLocation), ignored -> DeleteFileSet.create())
        .add(relativizeDeleteFile(deleteFile, specs));
  }

  /**
   * @return map of data to rewritable delete files with relativized paths
   */
  public Map<String, DeleteFileSet> relativizedDeletes() {
    return relativizedRewritableDeletes;
  }

  /**
   * Return an iterable of deletes for a given absolute data file location
   *
   * @param dataFileLocation absolute data file location
   * @param specs map from spec ID to partition spec
   */
  public Iterable<DeleteFile> deletesFor(
      String dataFileLocation, Map<Integer, PartitionSpec> specs) {
    DeleteFileSet relativizedDeletes =
        relativizedRewritableDeletes.get(relativizePath(dataFileLocation));
    if (relativizedDeletes == null) {
      return null;
    }

    return Iterables.transform(
        relativizedDeletes,
        deleteFile ->
            FileMetadata.deleteFileBuilder(specs.get(deleteFile.specId()))
                .copy(deleteFile)
                .withPath(absolutePath(deleteFile.location()))
                .build());
  }

  private String absolutePath(String path) {
    return tableLocation + path;
  }

  private String relativizePath(String path) {
    int indexOfRelativeLocation = path.indexOf(tableLocation);
    Preconditions.checkArgument(
        indexOfRelativeLocation != -1,
        "Cannot relativize path: %s in relative location %s",
        path,
        tableLocation);
    return path.substring(indexOfRelativeLocation + tableLocation.length());
  }

  private DeleteFile relativizeDeleteFile(
      DeleteFile deleteFile, Map<Integer, PartitionSpec> specs) {
    return FileMetadata.deleteFileBuilder(specs.get(deleteFile.specId()))
        .copy(deleteFile)
        .withPath(relativizePath(deleteFile.location()))
        .build();
  }
}
