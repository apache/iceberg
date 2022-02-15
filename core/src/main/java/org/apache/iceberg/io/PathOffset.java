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

import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.JavaHashes;

public final class PathOffset {
  private final CharSequence path;
  private final long offset;

  private PathOffset(CharSequence path, long offset) {
    this.path = path;
    this.offset = offset;
  }

  public static PathOffset of(CharSequence path, long offset) {
    return new PathOffset(path, offset);
  }

  public CharSequence path() {
    return path;
  }

  public long offset() {
    return offset;
  }

  public <T> PositionDelete<T> setTo(PositionDelete<T> positionDelete) {
    return positionDelete.set(path, offset, null);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("path", path)
        .add("offset", offset)
        .toString();
  }

  @Override
  public int hashCode() {
    return (int) (31 * offset + JavaHashes.hashCode(path));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PathOffset that = (PathOffset) o;
    return this.offset == that.offset &&
        Comparators.charSequences().compare(this.path, that.path) == 0;
  }
}
