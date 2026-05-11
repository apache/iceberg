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
package org.apache.iceberg;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.avro.SupportsIndexProjection;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ArrayUtil;

/** Mutable {@link StructLike} implementation of {@link ColumnFile}. */
class ColumnFileStruct extends SupportsIndexProjection implements ColumnFile, Serializable {
  private static final Types.StructType BASE_TYPE =
      Types.StructType.of(ColumnFile.FIELD_IDS, ColumnFile.LOCATION, ColumnFile.FILE_SIZE_IN_BYTES);

  private int[] fieldIds = null;
  private String location = null;
  private long fileSizeInBytes = -1L;

  /** Used by internal readers to instantiate this class with a projection schema. */
  ColumnFileStruct(Types.StructType projection) {
    super(BASE_TYPE, projection);
  }

  private ColumnFileStruct(int[] fieldIds, String location, long fileSizeInBytes) {
    super(BASE_TYPE.fields().size());
    this.fieldIds = fieldIds;
    this.location = location;
    this.fileSizeInBytes = fileSizeInBytes;
  }

  /** Copy constructor. */
  private ColumnFileStruct(ColumnFileStruct toCopy) {
    super(toCopy);
    this.fieldIds =
        toCopy.fieldIds != null ? Arrays.copyOf(toCopy.fieldIds, toCopy.fieldIds.length) : null;
    this.location = toCopy.location;
    this.fileSizeInBytes = toCopy.fileSizeInBytes;
  }

  /** Constructor for Java serialization. */
  ColumnFileStruct() {
    super(BASE_TYPE.fields().size());
  }

  @Override
  public List<Integer> fieldIds() {
    return fieldIds != null ? ArrayUtil.toUnmodifiableIntList(fieldIds) : null;
  }

  @Override
  public String location() {
    return location;
  }

  @Override
  public long fileSizeInBytes() {
    return fileSizeInBytes;
  }

  @Override
  public ColumnFile copy() {
    return new ColumnFileStruct(this);
  }

  @Override
  protected <T> T internalGet(int pos, Class<T> javaClass) {
    return javaClass.cast(getByPos(pos));
  }

  private Object getByPos(int pos) {
    switch (pos) {
      case 0:
        return fieldIds();
      case 1:
        return location;
      case 2:
        return fileSizeInBytes;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  protected <T> void internalSet(int pos, T value) {
    switch (pos) {
      case 0:
        this.fieldIds = ArrayUtil.toIntArray((List<Integer>) value);
        break;
      case 1:
        // always coerce to String for Serializable
        this.location = value.toString();
        break;
      case 2:
        this.fileSizeInBytes = (long) value;
        break;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  static Builder builder() {
    return new Builder();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("field_ids", fieldIds == null ? "null" : fieldIds())
        .add("location", location)
        .add("file_size_in_bytes", fileSizeInBytes)
        .toString();
  }

  static class Builder {
    private int[] fieldIds = null;
    private String location = null;
    private Long fileSizeInBytes = null;

    Builder fieldIds(List<Integer> newFieldIds) {
      Preconditions.checkArgument(newFieldIds != null, "Invalid field IDs: null");
      Preconditions.checkArgument(!newFieldIds.isEmpty(), "Invalid field IDs: empty");
      this.fieldIds = ArrayUtil.toIntArray(newFieldIds);
      return this;
    }

    Builder location(String newLocation) {
      Preconditions.checkArgument(newLocation != null, "Invalid location: null");
      Preconditions.checkArgument(!newLocation.isEmpty(), "Invalid location: empty");
      this.location = newLocation;
      return this;
    }

    Builder fileSizeInBytes(long newFileSizeInBytes) {
      Preconditions.checkArgument(
          newFileSizeInBytes >= 0,
          "Invalid file size in bytes: %s (must be >= 0)",
          newFileSizeInBytes);
      this.fileSizeInBytes = newFileSizeInBytes;
      return this;
    }

    ColumnFile build() {
      Preconditions.checkArgument(fieldIds != null, "Missing required value: fieldIds");
      Preconditions.checkArgument(location != null, "Missing required value: location");
      Preconditions.checkArgument(
          fileSizeInBytes != null, "Missing required value: fileSizeInBytes");
      return new ColumnFileStruct(fieldIds, location, fileSizeInBytes);
    }
  }
}
