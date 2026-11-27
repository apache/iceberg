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

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PartitionMap;
import org.apache.iceberg.util.StructLikeWrapper;

/** Equality deletes loaded once and shared by the data files of a task. Not thread-safe. */
public class EqualityDeletes {
  private final Set<String> loadedEqDeletes;
  private final PartitionMap<Map<Set<Integer>, Map<StructLikeWrapper, Long>>> sharedEqDeletes;

  public EqualityDeletes(Map<Integer, PartitionSpec> specs) {
    this.loadedEqDeletes = Sets.newHashSet();
    this.sharedEqDeletes = PartitionMap.create(specs);
  }

  boolean contains(DeleteFile deleteFile) {
    return deleteFile.location() != null && loadedEqDeletes.contains(deleteFile.location());
  }

  Map<StructLikeWrapper, Long> get(DeleteFile deleteFile, Set<Integer> ids) {
    Map<Set<Integer>, Map<StructLikeWrapper, Long>> deleteMaps =
        sharedEqDeletes.get(deleteFile.specId(), deleteFile.partition());
    return deleteMaps == null ? null : deleteMaps.get(ids);
  }

  Map<StructLikeWrapper, Long> merge(
      DeleteFile deleteFile,
      Set<Integer> ids,
      Schema deleteSchema,
      Iterable<StructLike> deleteKeys) {
    if (deleteFile.location() != null && !loadedEqDeletes.add(deleteFile.location())) {
      return get(deleteFile, ids);
    }

    Long deleteSequenceNumber = deleteFile.dataSequenceNumber();
    Map<StructLikeWrapper, Long> deleteMap =
        sharedEqDeletes
            .computeIfAbsent(deleteFile.specId(), deleteFile.partition(), Maps::newHashMap)
            .computeIfAbsent(ids, key -> Maps.newHashMap());
    StructLikeWrapper wrapper = StructLikeWrapper.forType(deleteSchema.asStruct());
    for (StructLike key : deleteKeys) {
      deleteMap.merge(wrapper.copyFor(key), deleteSequenceNumber, EqualityDeletes::max);
    }
    return deleteMap;
  }

  // returns one of the boxed arguments to avoid boxing a new value
  private static Long max(Long first, Long second) {
    return first >= second ? first : second;
  }
}
