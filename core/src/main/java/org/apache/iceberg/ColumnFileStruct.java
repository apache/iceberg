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
          ColumnFile.FORMAT_VERSION,
          ColumnFile.FIELD_IDS,
          ColumnFile.LOCATION,
          ColumnFile.FILE_FORMAT,
          ColumnFile.FILE_SIZE_IN_BYTES,
          ColumnFile.KEY_METADATA,
          ColumnFile.SPLIT_OFFSETS);

  private int formatVersion = -1;
  private int[] fieldIds = null;
  private String location = null;
  private FileFormat fileFormat = null;
  private long fileSizeInBytes = -1L;
  private byte[] keyMetadata = null;
  private long[] splitOffsets = null;

  /** Used by internal readers to instantiate this class with a projection schema. */
  ColumnFileStruct(Types.StructType projection) {
    super(BASE_TYPE, projection);
  }

  ColumnFileStruct(
      int formatVersion,
      List<Integer> fieldIds,
      String location,
      FileFormat fileFormat,
      long fileSizeInBytes,
      ByteBuffer keyMetadata,
      List<Long> splitOffsets) {
    super(BASE_TYPE.fields().size());
    this.formatVersion = formatVersion;
    this.fieldIds = ArrayUtil.toIntArray(fieldIds);
    this.location = location;
    this.fileFormat = fileFormat;
    this.fileSizeInBytes = fileSizeInBytes;
    this.keyMetadata = ByteBuffers.toByteArray(keyMetadata);
    this.splitOffsets = ArrayUtil.toLongArray(splitOffsets);
  }

  /** Copy constructor. */
  private ColumnFileStruct(ColumnFileStruct toCopy) {
    super(toCopy);
    this.formatVersion = toCopy.formatVersion;
    this.fieldIds =
        toCopy.fieldIds != null ? Arrays.copyOf(toCopy.fieldIds, toCopy.fieldIds.length) : null;
    this.location = toCopy.location;
    this.fileFormat = toCopy.fileFormat;
    this.fileSizeInBytes = toCopy.fileSizeInBytes;
    this.keyMetadata =
        toCopy.keyMetadata != null
            ? Arrays.copyOf(toCopy.keyMetadata, toCopy.keyMetadata.length)
            : null;
    this.splitOffsets =
        toCopy.splitOffsets != null
            ? Arrays.copyOf(toCopy.splitOffsets, toCopy.splitOffsets.length)
            : null;
  }

  /** Constructor for Java serialization. */
  ColumnFileStruct() {
    super(BASE_TYPE.fields().size());
  }

  @Override
  public int formatVersion() {
    return formatVersion;
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
  public List<Long> splitOffsets() {
    return splitOffsets != null ? ArrayUtil.toUnmodifiableLongList(splitOffsets) : null;
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
      case 0 -> formatVersion;
      case 1 -> fieldIds();
      case 2 -> location;
      case 3 -> fileFormat != null ? fileFormat.toString() : null;
      case 4 -> fileSizeInBytes;
      case 5 -> keyMetadata();
      case 6 -> splitOffsets();
      default -> throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    };
  }

  @Override
  @SuppressWarnings("unchecked")
  protected <T> void internalSet(int pos, T value) {
    switch (pos) {
      case 0 -> this.formatVersion = (int) value;
      case 1 -> this.fieldIds = ArrayUtil.toIntArray((List<Integer>) value);
        // always coerce to String for Serializable
      case 2 -> this.location = value.toString();
      case 3 -> this.fileFormat = FileFormat.fromString(value.toString());
      case 4 -> this.fileSizeInBytes = (long) value;
      case 5 -> this.keyMetadata = ByteBuffers.toByteArray((ByteBuffer) value);
      case 6 -> this.splitOffsets = ArrayUtil.toLongArray((List<Long>) value);
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
        .add("format_version", formatVersion)
        .add("field_ids", fieldIds)
        .add("location", location)
        .add("file_format", fileFormat)
        .add("file_size_in_bytes", fileSizeInBytes)
        .add("key_metadata", keyMetadata == null ? "null" : "(redacted)")
        .add("split_offsets", splitOffsets == null ? "null" : splitOffsets())
        .toString();
  }

  static class Builder {
    private Integer formatVersion = null;
    private List<Integer> fieldIds = null;
    private String location = null;
    private FileFormat fileFormat = null;
    private Long fileSizeInBytes = null;
    private ByteBuffer keyMetadata = null;
    private List<Long> splitOffsets = null;

    Builder formatVersion(int newFormatVersion) {
      Preconditions.checkArgument(
          newFormatVersion >= 0, "Invalid format version: %s (must be >= 0)", newFormatVersion);
      this.formatVersion = newFormatVersion;
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

    Builder location(String newLocation) {
      Preconditions.checkArgument(newLocation != null, "Invalid location: null");
      Preconditions.checkArgument(!newLocation.isEmpty(), "Invalid location: empty");
      this.location = newLocation;
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
      Preconditions.checkArgument(newKeyMetadata != null, "Invalid key metadata: null");
      this.keyMetadata = newKeyMetadata;
      return this;
    }

    Builder splitOffsets(List<Long> newSplitOffsets) {
      Preconditions.checkArgument(newSplitOffsets != null, "Invalid split offsets: null");
      this.splitOffsets = newSplitOffsets;
      return this;
    }

    ColumnFile build() {
      Preconditions.checkArgument(formatVersion != null, "Missing required value: formatVersion");
      Preconditions.checkArgument(fieldIds != null, "Missing required value: fieldIds");
      Preconditions.checkArgument(location != null, "Missing required value: location");
      Preconditions.checkArgument(fileFormat != null, "Missing required value: fileFormat");
      Preconditions.checkArgument(
          fileSizeInBytes != null, "Missing required value: fileSizeInBytes");
      return new ColumnFileStruct(
          formatVersion,
          fieldIds,
          location,
          fileFormat,
          fileSizeInBytes,
          keyMetadata,
          splitOffsets);
    }
  }
}
