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

import java.util.Comparator;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.JavaHash;
import org.apache.iceberg.types.Types;

/** Wrapper to adapt StructLike for use in maps and sets by implementing equals and hashCode. */
public class StructLikeWrapper {

  public static StructLikeWrapper forType(Types.StructType struct) {
    return new StructLikeWrapper(struct);
  }

  private final Comparator<StructLike> comparator;
  private final JavaHash<StructLike> structHash;
  private Integer hashCode;
  private StructLike struct;

  private StructLikeWrapper(Types.StructType type) {
    this(Comparators.forType(type), JavaHash.forType(type));
  }

  private StructLikeWrapper(Comparator<StructLike> comparator, JavaHash<StructLike> structHash) {
    this.comparator = comparator;
    this.structHash = structHash;
    this.hashCode = null;
  }

  /**
   * Creates a copy of this wrapper that wraps a struct.
   *
   * <p>This is equivalent to {@code new StructLikeWrapper(type).set(newStruct)} but is cheaper
   * because no analysis of the type is necessary.
   *
   * @param newStruct a {@link StructLike} row
   * @return a copy of this wrapper wrapping the give struct
   */
  public StructLikeWrapper copyFor(StructLike newStruct) {
    return new StructLikeWrapper(comparator, structHash).set(newStruct);
  }

  public StructLikeWrapper set(StructLike newStruct) {
    this.struct = newStruct;
    this.hashCode = null;
    return this;
  }

  public StructLike get() {
    return struct;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (!(other instanceof StructLikeWrapper)) {
      return false;
    }

    StructLikeWrapper that = (StructLikeWrapper) other;

    if (this.struct == that.struct) {
      return true;
    }

    if (this.struct == null ^ that.struct == null) {
      return false;
    }

    return comparator.compare(this.struct, that.struct) == 0;
  }

  @Override
  public int hashCode() {
    if (hashCode == null) {
      this.hashCode = structHash.hash(struct);
    }

    return hashCode;
  }
}
