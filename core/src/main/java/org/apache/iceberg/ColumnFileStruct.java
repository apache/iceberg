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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.avro.SupportsIndexProjection;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.ByteBuffers;

/** Mutable {@link StructLike} implementation of {@link ColumnFile}. */
class ColumnFileStruct extends SupportsIndexProjection implements ColumnFile, Serializable {
  private static final Types.StructType BASE_TYPE =
      Types.StructType.of(
          ColumnFile.LOCATION,
          ColumnFile.FIELD_IDS,
          ColumnFile.FILE_FORMAT,
          ColumnFile.FILE_SIZE_IN_BYTES,
          ColumnFile.KEY_METADATA);

  private String location = null;
  private int[] fieldIds = null;
  private FileFormat fileFormat = null;
  private long fileSizeInBytes = -1L;
  private byte[] keyMetadata = null;

  /** Used by internal readers to instantiate this class with a projection schema. */
  ColumnFileStruct(Types.StructType projection) {
    super(BASE_TYPE, projection);
  }

  ColumnFileStruct(
      String location,
      List<Integer> fieldIds,
      FileFormat fileFormat,
      long fileSizeInBytes,
      ByteBuffer keyMetadata) {
    super(BASE_TYPE.fields().size());
    this.location = location;
    this.fieldIds = ArrayUtil.toIntArray(fieldIds);
    this.fileFormat = fileFormat;
    this.fileSizeInBytes = fileSizeInBytes;
    this.keyMetadata = ByteBuffers.toByteArray(keyMetadata);
  }

  /** Copy constructor. */
  private ColumnFileStruct(ColumnFileStruct toCopy) {
    super(toCopy);
    this.location = toCopy.location;
    this.fieldIds =
        toCopy.fieldIds != null ? Arrays.copyOf(toCopy.fieldIds, toCopy.fieldIds.length) : null;
    this.fileFormat = toCopy.fileFormat;
    this.fileSizeInBytes = toCopy.fileSizeInBytes;
    this.keyMetadata =
        toCopy.keyMetadata != null
            ? Arrays.copyOf(toCopy.keyMetadata, toCopy.keyMetadata.length)
            : null;
  }

  /** Constructor for Java serialization. */
  ColumnFileStruct() {
    super(BASE_TYPE.fields().size());
  }

  @Override
  public String location() {
    return location;
  }

  @Override
  public List<Integer> fieldIds() {
    return fieldIds != null ? ArrayUtil.toUnmodifiableIntList(fieldIds) : null;
  }

  @Override
  public FileFormat fileFormat() {
    return fileFormat;
  }

  @Override
  public long fileSizeInBytes() {
    return fileSizeInBytes;
  }

  @Override
  public ByteBuffer keyMetadata() {
    return keyMetadata != null ? ByteBuffer.wrap(keyMetadata) : null;
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
    return switch (pos) {
      case 0 -> location;
      case 1 -> fieldIds();
      case 2 -> fileFormat != null ? fileFormat.toString() : null;
      case 3 -> fileSizeInBytes;
      case 4 -> keyMetadata();
      default -> throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    };
  }

  @Override
  @SuppressWarnings("unchecked")
  protected <T> void internalSet(int pos, T value) {
    switch (pos) {
        // always coerce to String for Serializable
      case 0 -> this.location = value.toString();
      case 1 -> this.fieldIds = ArrayUtil.toIntArray((List<Integer>) value);
      case 2 -> this.fileFormat = FileFormat.fromString(value.toString());
      case 3 -> this.fileSizeInBytes = (long) value;
      case 4 -> this.keyMetadata = ByteBuffers.toByteArray((ByteBuffer) value);
      default -> {
        // ignore the object, it must be from a newer version of the format
      }
    }
  }

  static Builder builder() {
    return new Builder();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("location", location)
        .add("field_ids", fieldIds)
        .add("file_format", fileFormat)
        .add("file_size_in_bytes", fileSizeInBytes)
        .add("key_metadata", keyMetadata == null ? "null" : "(redacted)")
        .toString();
  }

  static class Builder {
    private String location = null;
    private List<Integer> fieldIds = null;
    private FileFormat fileFormat = null;
    private Long fileSizeInBytes = null;
    private ByteBuffer keyMetadata = null;

    Builder location(String newLocation) {
      Preconditions.checkArgument(newLocation != null, "Invalid location: null");
      Preconditions.checkArgument(!newLocation.isEmpty(), "Invalid location: empty");
      this.location = newLocation;
      return this;
    }

    Builder fieldIds(List<Integer> newFieldIds) {
      Preconditions.checkArgument(newFieldIds != null, "Invalid field IDs: null");
      Preconditions.checkArgument(!newFieldIds.isEmpty(), "Invalid field IDs: empty");
      Preconditions.checkArgument(
          Sets.newHashSet(newFieldIds).size() == newFieldIds.size(),
          "Invalid field IDs: duplicated IDs found in: %s",
          newFieldIds);
      this.fieldIds = newFieldIds;
      return this;
    }

    Builder fileFormat(FileFormat newFileFormat) {
      Preconditions.checkArgument(newFileFormat != null, "Invalid file format: null");
      this.fileFormat = newFileFormat;
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

    Builder keyMetadata(ByteBuffer newKeyMetadata) {
      this.keyMetadata = newKeyMetadata;
      return this;
    }

    ColumnFile build() {
      Preconditions.checkArgument(location != null, "Missing required value: location");
      Preconditions.checkArgument(fieldIds != null, "Missing required value: field IDs");
      Preconditions.checkArgument(fileFormat != null, "Missing required value: file format");
      Preconditions.checkArgument(
          fileSizeInBytes != null, "Missing required value: file size in bytes");
      return new ColumnFileStruct(location, fieldIds, fileFormat, fileSizeInBytes, keyMetadata);
    }
  }
}
