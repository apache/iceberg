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

import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;

public class ConcatStructLike implements StructLike {
  private final Types.StructType leftType;
  private final Types.StructType rightType;
  private final int splitPos;

  private StructLike left;
  private StructLike right;

  public ConcatStructLike(Types.StructType leftType, Types.StructType rightType) {
    this.leftType = leftType;
    this.rightType = rightType;
    this.splitPos = leftType.fields().size();
  }

  public Types.StructType leftType() {
    return leftType;
  }

  public StructLike left() {
    return left;
  }

  public void setLeft(StructLike left) {
    this.left = left;
  }

  public Types.StructType rightType() {
    return rightType;
  }

  public StructLike right() {
    return right;
  }

  public void setRight(StructLike right) {
    this.right = right;
  }

  @Override
  public int size() {
    return leftType.fields().size() + rightType.fields().size();
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    if (pos < splitPos) {
      return left.get(pos, javaClass);
    } else {
      return right.get(pos - splitPos, javaClass);
    }
  }

  @Override
  public <T> void set(int pos, T value) {
    if (pos < splitPos) {
      left.set(pos, value);
    } else {
      right.set(pos, value);
    }
  }
}
