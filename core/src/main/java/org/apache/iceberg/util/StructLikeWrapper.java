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

import java.util.Objects;
import org.apache.iceberg.StructLike;

/**
 * Wrapper to adapt StructLike for use in maps and sets by implementing equals and hashCode.
 */
public class StructLikeWrapper {

  public static StructLikeWrapper wrap(StructLike struct) {
    return new StructLikeWrapper(struct);
  }

  private StructLike struct;

  private StructLikeWrapper(StructLike struct) {
    this.struct = struct;
  }

  public StructLikeWrapper set(StructLike newStruct) {
    this.struct = newStruct;
    return this;
  }

  public StructLike get() {
    return struct;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    StructLikeWrapper that = (StructLikeWrapper) other;

    if (this.struct == that.struct) {
      return true;
    }

    if (this.struct == null ^ that.struct == null) {
      return false;
    }

    int len = struct.size();
    if (len != that.struct.size()) {
      return false;
    }

    for (int i = 0; i < len; i += 1) {
      if (!Objects.equals(struct.get(i, Object.class), that.struct.get(i, Object.class))) {
        return false;
      }
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = 97;
    int len = struct.size();
    result = 41 * result + len;
    for (int i = 0; i < len; i += 1) {
      result = 41 * result + Objects.hashCode(struct.get(i, Object.class));
    }
    return result;
  }
}
