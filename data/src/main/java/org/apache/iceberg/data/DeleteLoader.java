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
package org.apache.iceberg.data;

import java.util.List;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeSet;

/** An API for loading delete file content into in-memory data structures. */
public interface DeleteLoader {
  /**
   * Loads the content of equality delete files into a set.
   *
   * @param deleteFiles equality delete files
   * @param projection a projection of columns to load
   * @return a set of equality deletes
   */
  StructLikeSet loadEqualityDeletes(Iterable<DeleteFile> deleteFiles, Schema projection);

  /**
   * Loads the content of multiple equality delete files at once, each with its own projection,
   * keeping each delete file's content separate rather than merging it into a single set.
   *
   * <p>The default implementation calls {@link #loadEqualityDeletes(Iterable, Schema)} once per
   * delete file. Override this method to provide a more efficient implementation, e.g. one that
   * loads all delete files in a single batch.
   *
   * @param deleteFiles equality delete files, each paired with the projection to load it with
   * @return each delete file paired with its own loaded content
   */
  default Iterable<Pair<DeleteFile, Iterable<StructLike>>> loadEqualityDeletes(
      Iterable<Pair<DeleteFile, Schema>> deleteFiles) {
    List<Pair<DeleteFile, Iterable<StructLike>>> loaded = Lists.newArrayList();
    for (Pair<DeleteFile, Schema> pair : deleteFiles) {
      Iterable<StructLike> deletes =
          loadEqualityDeletes(ImmutableList.of(pair.first()), pair.second());
      loaded.add(Pair.of(pair.first(), deletes));
    }
    return loaded;
  }

  /**
   * Loads the content of a deletion vector or position delete files for a given data file path into
   * a position index.
   *
   * @param deleteFiles a deletion vector or position delete files
   * @param filePath the data file path for which to load deletes
   * @return a position delete index for the provided data file path
   */
  PositionDeleteIndex loadPositionDeletes(Iterable<DeleteFile> deleteFiles, CharSequence filePath);
}
