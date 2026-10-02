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
package org.apache.iceberg.io;

import java.util.Map;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeMap;

/** Tracks data-file positions for equality keys written by related task writers. */
public final class PositionDeleteTracker {
  private final Map<StructLike, PathOffset> insertedRowMap;

  /** Creates a tracker for the equality-key struct type. */
  public PositionDeleteTracker(Types.StructType keyType) {
    this.insertedRowMap =
        StructLikeMap.create(Preconditions.checkNotNull(keyType, "Key type cannot be null"));
  }

  Map<StructLike, PathOffset> insertedRows() {
    return insertedRowMap;
  }

  /** Clears all tracked rows. */
  public void clear() {
    insertedRowMap.clear();
  }

  static final class PathOffset {
    final CharSequence path;
    final long rowOffset;

    PathOffset(CharSequence path, long rowOffset) {
      this.path = path;
      this.rowOffset = rowOffset;
    }

    static PathOffset of(CharSequence path, long rowOffset) {
      return new PathOffset(path, rowOffset);
    }
  }
}
