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

import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.types.Types;

public class PathOffset implements StructLike {

  private static final Schema PATH_OFFSET_SCHEMA = new Schema(
      Types.NestedField.required(0, "path", Types.StringType.get()),
      Types.NestedField.required(1, "row_offset", Types.LongType.get())
  );

  private CharSequence path;
  private long rowOffset;

  private PathOffset(CharSequence path, long rowOffset) {
    this.path = path;
    this.rowOffset = rowOffset;
  }

  public static PathOffset of(CharSequence path, long rowOffset) {
    return new PathOffset(path, rowOffset);
  }

  public static Schema schema() {
    return PATH_OFFSET_SCHEMA;
  }

  public CharSequence path() {
    return path;
  }

  public long rowOffset() {
    return rowOffset;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("path", path)
        .add("row_offset", rowOffset)
        .toString();
  }

  @Override
  public int size() {
    return 2;
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    switch (pos) {
      case 0:
        return javaClass.cast(path);
      case 1:
        return javaClass.cast(rowOffset);
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }

  @Override
  public <T> void set(int pos, T value) {
    switch (pos) {
      case 0:
        this.path = (CharSequence) value;
        break;
      case 1:
        this.rowOffset = (long) value;
        break;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }
}
