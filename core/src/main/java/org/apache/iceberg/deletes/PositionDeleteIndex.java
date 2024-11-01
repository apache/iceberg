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
package org.apache.iceberg.deletes;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.function.LongConsumer;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public interface PositionDeleteIndex {
  /**
   * Set a deleted row position.
   *
   * @param position the deleted row position
   */
  void delete(long position);

  /**
   * Set a range of deleted row positions.
   *
   * @param posStart inclusive beginning of position range
   * @param posEnd exclusive ending of position range
   */
  void delete(long posStart, long posEnd);

  /**
   * Adds positions from the other index, modifying this index in place.
   *
   * @param that the other index to merge
   */
  default void merge(PositionDeleteIndex that) {
    if (!that.deleteFiles().isEmpty()) {
      throw new UnsupportedOperationException(getClass().getName() + " does not support merge");
    }
    that.forEach(this::delete);
  }

  /**
   * Checks whether a row at the position is deleted.
   *
   * @param position deleted row position
   * @return whether the position is deleted
   */
  boolean isDeleted(long position);

  /** Returns true if this collection contains no element. */
  boolean isEmpty();

  /** Returns true if this collection contains elements. */
  default boolean isNotEmpty() {
    return !isEmpty();
  }

  /**
   * Traverses all positions in the index in ascending order, applying the provided consumer.
   *
   * @param consumer a consumer for the positions
   */
  default void forEach(LongConsumer consumer) {
    if (isNotEmpty()) {
      throw new UnsupportedOperationException(getClass().getName() + " does not support forEach");
    }
  }

  /**
   * Returns delete files that this index was created from or an empty collection if unknown.
   *
   * @return delete files that this index was created from
   */
  default Collection<DeleteFile> deleteFiles() {
    return ImmutableList.of();
  }

  /**
   * Serializes this index.
   *
   * @return a buffer containing the serialized index
   */
  default ByteBuffer serialize() {
    throw new UnsupportedOperationException(getClass().getName() + " does not support serialize");
  }

  /** Returns the cardinality of this index. */
  default long cardinality() {
    throw new UnsupportedOperationException(getClass().getName() + " does not support cardinality");
  }

  /** Returns an empty immutable position delete index. */
  static PositionDeleteIndex empty() {
    return EmptyPositionDeleteIndex.get();
  }

  /**
   * Deserializes a position delete index.
   *
   * @param bytes an array containing the serialized index
   * @param deleteFile the delete file that the index is created for
   * @return the deserialized index
   */
  static PositionDeleteIndex deserialize(byte[] bytes, DeleteFile deleteFile) {
    return BitmapPositionDeleteIndex.deserialize(bytes, deleteFile);
  }
}
