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
import org.apache.iceberg.avro.SupportsIndexProjection;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;

/** Mutable {@link StructLike} implementation of {@link ManifestInfo}. */
class ManifestInfoStruct extends SupportsIndexProjection implements ManifestInfo, Serializable {
  private static final Types.StructType BASE_TYPE =
      Types.StructType.of(
          ManifestInfo.ADDED_FILES_COUNT,
          ManifestInfo.EXISTING_FILES_COUNT,
          ManifestInfo.DELETED_FILES_COUNT,
          ManifestInfo.REPLACED_FILES_COUNT,
          ManifestInfo.ADDED_ROWS_COUNT,
          ManifestInfo.EXISTING_ROWS_COUNT,
          ManifestInfo.DELETED_ROWS_COUNT,
          ManifestInfo.REPLACED_ROWS_COUNT,
          ManifestInfo.MIN_SEQUENCE_NUMBER,
          ManifestInfo.DV,
          ManifestInfo.DV_CARDINALITY);

  private int addedFilesCount = -1;
  private int existingFilesCount = -1;
  private int deletedFilesCount = -1;
  private int replacedFilesCount = -1;
  private long addedRowsCount = -1L;
  private long existingRowsCount = -1L;
  private long deletedRowsCount = -1L;
  private long replacedRowsCount = -1L;
  private long minSequenceNumber = -1L;
  private byte[] dv = null;
  private Long dvCardinality = null;

  ManifestInfoStruct(Types.StructType type) {
    super(BASE_TYPE, type);
  }

  private ManifestInfoStruct(ManifestInfoStruct toCopy) {
    super(toCopy);
    this.addedFilesCount = toCopy.addedFilesCount;
    this.existingFilesCount = toCopy.existingFilesCount;
    this.deletedFilesCount = toCopy.deletedFilesCount;
    this.replacedFilesCount = toCopy.replacedFilesCount;
    this.addedRowsCount = toCopy.addedRowsCount;
    this.existingRowsCount = toCopy.existingRowsCount;
    this.deletedRowsCount = toCopy.deletedRowsCount;
    this.replacedRowsCount = toCopy.replacedRowsCount;
    this.minSequenceNumber = toCopy.minSequenceNumber;
    this.dv = toCopy.dv != null ? Arrays.copyOf(toCopy.dv, toCopy.dv.length) : null;
    this.dvCardinality = toCopy.dvCardinality;
  }

  ManifestInfoStruct(
      int addedFilesCount,
      int existingFilesCount,
      int deletedFilesCount,
      int replacedFilesCount,
      long addedRowsCount,
      long existingRowsCount,
      long deletedRowsCount,
      long replacedRowsCount,
      long minSequenceNumber,
      byte[] dv,
      Long dvCardinality) {
    super(BASE_TYPE.fields().size());
    this.addedFilesCount = addedFilesCount;
    this.existingFilesCount = existingFilesCount;
    this.deletedFilesCount = deletedFilesCount;
    this.replacedFilesCount = replacedFilesCount;
    this.addedRowsCount = addedRowsCount;
    this.existingRowsCount = existingRowsCount;
    this.deletedRowsCount = deletedRowsCount;
    this.replacedRowsCount = replacedRowsCount;
    this.minSequenceNumber = minSequenceNumber;
    this.dv = dv;
    this.dvCardinality = dvCardinality;
  }

  @Override
  public int addedFilesCount() {
    return addedFilesCount;
  }

  @Override
  public int existingFilesCount() {
    return existingFilesCount;
  }

  @Override
  public int deletedFilesCount() {
    return deletedFilesCount;
  }

  @Override
  public int replacedFilesCount() {
    return replacedFilesCount;
  }

  @Override
  public long addedRowsCount() {
    return addedRowsCount;
  }

  @Override
  public long existingRowsCount() {
    return existingRowsCount;
  }

  @Override
  public long deletedRowsCount() {
    return deletedRowsCount;
  }

  @Override
  public long replacedRowsCount() {
    return replacedRowsCount;
  }

  @Override
  public long minSequenceNumber() {
    return minSequenceNumber;
  }

  @Override
  public ByteBuffer dv() {
    return dv != null ? ByteBuffer.wrap(dv) : null;
  }

  @Override
  public Long dvCardinality() {
    return dvCardinality;
  }

  @Override
  public ManifestInfoStruct copy() {
    return new ManifestInfoStruct(this);
  }

  @Override
  protected <T> T internalGet(int pos, Class<T> javaClass) {
    return javaClass.cast(getByPos(pos));
  }

  private Object getByPos(int pos) {
    switch (pos) {
      case 0:
        return addedFilesCount;
      case 1:
        return existingFilesCount;
      case 2:
        return deletedFilesCount;
      case 3:
        return replacedFilesCount;
      case 4:
        return addedRowsCount;
      case 5:
        return existingRowsCount;
      case 6:
        return deletedRowsCount;
      case 7:
        return replacedRowsCount;
      case 8:
        return minSequenceNumber;
      case 9:
        return dv();
      case 10:
        return dvCardinality;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }

  @Override
  protected <T> void internalSet(int pos, T value) {
    switch (pos) {
      case 0:
        this.addedFilesCount = (Integer) value;
        break;
      case 1:
        this.existingFilesCount = (Integer) value;
        break;
      case 2:
        this.deletedFilesCount = (Integer) value;
        break;
      case 3:
        this.replacedFilesCount = (Integer) value;
        break;
      case 4:
        this.addedRowsCount = (Long) value;
        break;
      case 5:
        this.existingRowsCount = (Long) value;
        break;
      case 6:
        this.deletedRowsCount = (Long) value;
        break;
      case 7:
        this.replacedRowsCount = (Long) value;
        break;
      case 8:
        this.minSequenceNumber = (Long) value;
        break;
      case 9:
        this.dv = ByteBuffers.toByteArray((ByteBuffer) value);
        break;
      case 10:
        this.dvCardinality = (Long) value;
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
        .add("added_files_count", addedFilesCount)
        .add("existing_files_count", existingFilesCount)
        .add("deleted_files_count", deletedFilesCount)
        .add("replaced_files_count", replacedFilesCount)
        .add("added_rows_count", addedRowsCount)
        .add("existing_rows_count", existingRowsCount)
        .add("deleted_rows_count", deletedRowsCount)
        .add("replaced_rows_count", replacedRowsCount)
        .add("min_sequence_number", minSequenceNumber)
        .add("dv", dv == null ? "null" : "(binary)")
        .add("dv_cardinality", dvCardinality)
        .toString();
  }

  static class Builder {
    private Integer addedFilesCount = null;
    private Integer existingFilesCount = null;
    private Integer deletedFilesCount = null;
    private Integer replacedFilesCount = null;
    private Long addedRowsCount = null;
    private Long existingRowsCount = null;
    private Long deletedRowsCount = null;
    private Long replacedRowsCount = null;
    private Long minSequenceNumber = null;
    private byte[] dv = null;
    private Long dvCardinality = null;

    Builder addedFilesCount(int count) {
      Preconditions.checkArgument(
          count >= 0, "Invalid added files count: %s (must be >= 0)", count);
      this.addedFilesCount = count;
      return this;
    }

    Builder existingFilesCount(int count) {
      Preconditions.checkArgument(
          count >= 0, "Invalid existing files count: %s (must be >= 0)", count);
      this.existingFilesCount = count;
      return this;
    }

    Builder deletedFilesCount(int count) {
      Preconditions.checkArgument(
          count >= 0, "Invalid deleted files count: %s (must be >= 0)", count);
      this.deletedFilesCount = count;
      return this;
    }

    Builder replacedFilesCount(int count) {
      Preconditions.checkArgument(
          count >= 0, "Invalid replaced files count: %s (must be >= 0)", count);
      this.replacedFilesCount = count;
      return this;
    }

    Builder addedRowsCount(long count) {
      Preconditions.checkArgument(count >= 0, "Invalid added rows count: %s (must be >= 0)", count);
      this.addedRowsCount = count;
      return this;
    }

    Builder existingRowsCount(long count) {
      Preconditions.checkArgument(
          count >= 0, "Invalid existing rows count: %s (must be >= 0)", count);
      this.existingRowsCount = count;
      return this;
    }

    Builder deletedRowsCount(long count) {
      Preconditions.checkArgument(
          count >= 0, "Invalid deleted rows count: %s (must be >= 0)", count);
      this.deletedRowsCount = count;
      return this;
    }

    Builder replacedRowsCount(long count) {
      Preconditions.checkArgument(
          count >= 0, "Invalid replaced rows count: %s (must be >= 0)", count);
      this.replacedRowsCount = count;
      return this;
    }

    Builder minSequenceNumber(long sequenceNumber) {
      Preconditions.checkArgument(
          sequenceNumber >= 0, "Invalid min sequence number: %s (must be >= 0)", sequenceNumber);
      this.minSequenceNumber = sequenceNumber;
      return this;
    }

    Builder dv(ByteBuffer buffer) {
      Preconditions.checkArgument(buffer != null, "Invalid DV: null");
      this.dv = ByteBuffers.toByteArray(buffer);
      return this;
    }

    Builder dvCardinality(long cardinality) {
      Preconditions.checkArgument(
          cardinality >= 0, "Invalid DV cardinality: %s (must be >= 0)", cardinality);
      this.dvCardinality = cardinality;
      return this;
    }

    ManifestInfoStruct build() {
      Preconditions.checkArgument(
          addedFilesCount != null, "Missing required value: added files count");
      Preconditions.checkArgument(
          existingFilesCount != null, "Missing required value: existing files count");
      Preconditions.checkArgument(
          deletedFilesCount != null, "Missing required value: deleted files count");
      Preconditions.checkArgument(
          replacedFilesCount != null, "Missing required value: replaced files count");
      Preconditions.checkArgument(
          addedRowsCount != null, "Missing required value: added rows count");
      Preconditions.checkArgument(
          existingRowsCount != null, "Missing required value: existing rows count");
      Preconditions.checkArgument(
          deletedRowsCount != null, "Missing required value: deleted rows count");
      Preconditions.checkArgument(
          replacedRowsCount != null, "Missing required value: replaced rows count");
      Preconditions.checkArgument(
          minSequenceNumber != null, "Missing required value: min sequence number");
      Preconditions.checkArgument(
          addedRowsCount == 0 || addedFilesCount > 0,
          "Invalid added counts: %s rows in %s files",
          addedRowsCount,
          addedFilesCount);
      Preconditions.checkArgument(
          existingRowsCount == 0 || existingFilesCount > 0,
          "Invalid existing counts: %s rows in %s files",
          existingRowsCount,
          existingFilesCount);
      Preconditions.checkArgument(
          deletedRowsCount == 0 || deletedFilesCount > 0,
          "Invalid deleted counts: %s rows in %s files",
          deletedRowsCount,
          deletedFilesCount);
      Preconditions.checkArgument(
          replacedRowsCount == 0 || replacedFilesCount > 0,
          "Invalid replaced counts: %s rows in %s files",
          replacedRowsCount,
          replacedFilesCount);
      Preconditions.checkArgument(
          (dv == null) == (dvCardinality == null),
          "Invalid DV and cardinality: must both be null or non-null");
      return new ManifestInfoStruct(
          addedFilesCount,
          existingFilesCount,
          deletedFilesCount,
          replacedFilesCount,
          addedRowsCount,
          existingRowsCount,
          deletedRowsCount,
          replacedRowsCount,
          minSequenceNumber,
          dv,
          dvCardinality);
    }
  }
}
