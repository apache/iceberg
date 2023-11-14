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

import java.util.Arrays;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeWrapper;

/**
 * DeleteIndexTable helps outside access deleteFile index internals this is used to grab planning
 * internal delete metadatas
 */
public class DeleteIndexTable {

  private final DeleteFileIndex deleteFileIndex;

  private final Map<Pair<Integer, StructLikeWrapper>, Object> deleteTableByPartition =
      Maps.newConcurrentMap();

  public DeleteIndexTable(DeleteFileIndex that) {
    this.deleteFileIndex = that;
  }

  public Pair<long[], DeleteFile[]> partitionDeleteEntries(Pair<Integer, StructLike> pair) {
    DeleteFileIndex.DeleteFileGroup group =
        deleteFileIndex
            .deletesByPartition()
            .get(deleteFileIndex.partition(pair.first(), pair.second()));

    return group == null
        ? null
        : Pair.of(
            group.seqs,
            Arrays.stream(group.files)
                .map(DeleteFileIndex.IndexedDeleteFile::wrapped)
                .toArray(DeleteFile[]::new));
  }

  public Pair<long[], DeleteFile[]> globalDeleteEntries() {
    if (deleteFileIndex.globalDeletes() == null) {
      return null;
    } else {
      DeleteFileIndex.DeleteFileGroup group = deleteFileIndex.globalDeletes();
      return Pair.of(
          group.seqs,
          Arrays.stream(group.files)
              .map(DeleteFileIndex.IndexedDeleteFile::wrapped)
              .toArray(DeleteFile[]::new));
    }
  }

  public Object getDeleteTable(Pair<Integer, StructLike> pair) {
    return deleteTableByPartition.get(deleteFileIndex.partition(pair.first(), pair.second()));
  }

  public void putDeleteTable(Pair<Integer, StructLike> pair, Object deleteTable) {
    deleteTableByPartition.put(deleteFileIndex.partition(pair.first(), pair.second()), deleteTable);
  }
}
