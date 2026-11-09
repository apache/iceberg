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
package org.apache.iceberg.flink.maintenance.operator;

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Flattens an equality key struct into its primitive leaf fields so {@link RecordSerializer} can
 * serialize nested equality fields such as {@code user.id}.
 *
 * <p>Leaves are ordered depth-first in schema order. For a key made only of top-level primitive
 * fields, {@link #flatType()} has the same fields in the same order as the key struct, so the
 * serialized keyed-state key is unchanged. A leaf whose enclosing struct is null reads as null.
 */
class FlattenedEqualityKey implements StructLike {

  private final Types.StructType flatType;
  private final int[][] paths;
  private StructLike wrapped = null;

  FlattenedEqualityKey(Types.StructType keyType) {
    List<Types.NestedField> leaves = Lists.newArrayList();
    List<int[]> leafPaths = Lists.newArrayList();
    collectLeaves(keyType, new int[0], leaves, leafPaths);
    this.flatType = Types.StructType.of(leaves);
    this.paths = leafPaths.toArray(new int[0][]);
  }

  private static void collectLeaves(
      Types.StructType struct, int[] prefix, List<Types.NestedField> leaves, List<int[]> paths) {
    List<Types.NestedField> fields = struct.fields();
    for (int pos = 0; pos < fields.size(); pos++) {
      Types.NestedField field = fields.get(pos);
      int[] path = Arrays.copyOf(prefix, prefix.length + 1);
      path[prefix.length] = pos;
      Type type = field.type();
      if (type.isPrimitiveType()) {
        leaves.add(field);
        paths.add(path);
      } else {
        Preconditions.checkArgument(
            type.isStructType(),
            "Equality field %s of type %s is not supported by ConvertEqualityDeletes: "
                + "equality fields must be primitive and not nested in a list or map",
            field.name(),
            type);
        collectLeaves(type.asStructType(), path, leaves, paths);
      }
    }
  }

  /** The flat struct of primitive leaf fields, in the order {@link #get} exposes them. */
  Types.StructType flatType() {
    return flatType;
  }

  FlattenedEqualityKey wrap(StructLike key) {
    this.wrapped = key;
    return this;
  }

  @Override
  public int size() {
    return paths.length;
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    int[] path = paths[pos];
    StructLike struct = wrapped;
    for (int depth = 0; depth < path.length - 1; depth++) {
      struct = struct.get(path[depth], StructLike.class);
      if (struct == null) {
        return null;
      }
    }

    return struct.get(path[path.length - 1], javaClass);
  }

  @Override
  public <T> void set(int pos, T value) {
    throw new UnsupportedOperationException("Cannot update FlattenedEqualityKey");
  }
}
