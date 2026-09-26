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
package org.apache.iceberg.index;

import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;

/**
 * A single row of a SCALAR index leaf file: an indexed key value mapped to its exact source row
 * location.
 *
 * <p>Leaf files are standard Iceberg data files (Parquet). Each row's key value keeps the
 * original field ID from the source table's schema; {@link #keyValue()} can be a {@code String},
 * {@code Long}, or {@code Integer}, matching the types {@link HashTransform} supports.
 */
public class LeafFileEntry {

  // IDs Integer.MAX_VALUE - (101-103) are reserved for SCALAR leaf-file synthetic columns.
  // Chosen to sit just past Iceberg's own reserved metadata-column range
  // (Integer.MAX_VALUE - (1-100), see MetadataColumns) so these never collide with either a
  // source table's real field IDs or Iceberg's own metadata columns.
  public static final int TRANSFORM_VALUE_FIELD_ID = Integer.MAX_VALUE - 101;
  public static final String TRANSFORM_VALUE_FIELD_NAME = "transform_value";
  public static final int FILE_PATH_FIELD_ID = Integer.MAX_VALUE - 102;
  public static final String FILE_PATH_FIELD_NAME = "file_path";
  public static final int POSITION_FIELD_ID = Integer.MAX_VALUE - 103;
  public static final String POSITION_FIELD_NAME = "position";

  /**
   * Build the leaf-file Parquet schema: the source table's key column (original field ID and
   * type preserved), followed by {@code transform_value}, {@code file_path}, and {@code
   * position}.
   *
   * <p>The writer and reader must always build this schema from the same {@code keyField} to stay
   * in sync — the field IDs and names for the three synthetic columns are fixed constants above,
   * not derived from anything caller-supplied.
   */
  public static Schema schema(Types.NestedField keyField) {
    Preconditions.checkNotNull(keyField, "keyField is required");
    return new Schema(
        keyField,
        Types.NestedField.required(
            TRANSFORM_VALUE_FIELD_ID, TRANSFORM_VALUE_FIELD_NAME, Types.LongType.get()),
        Types.NestedField.required(
            FILE_PATH_FIELD_ID, FILE_PATH_FIELD_NAME, Types.StringType.get()),
        Types.NestedField.required(POSITION_FIELD_ID, POSITION_FIELD_NAME, Types.LongType.get()));
  }

  private final Object keyValue;
  private final long transformValue;
  private final String filePath;
  private final long position;

  private LeafFileEntry(Object keyValue, long transformValue, String filePath, long position) {
    this.keyValue = keyValue;
    this.transformValue = transformValue;
    this.filePath = filePath;
    this.position = position;
  }

  /** The indexed key value, in the source table's original type ({@code String}, {@code Long},
   * or {@code Integer}). */
  public Object keyValue() {
    return keyValue;
  }

  /** The hash bucket (HASH) or the key value itself (IDENTITY). */
  public long transformValue() {
    return transformValue;
  }

  /** The source data file this row lives in. */
  public String filePath() {
    return filePath;
  }

  /** The row's ordinal position within {@link #filePath()}, matching Iceberg's position-delete
   * semantics (0-indexed, in file-scan order). */
  public long position() {
    return position;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private Object keyValue;
    private long transformValue;
    private String filePath;
    private long position;

    public Builder keyValue(Object value) {
      this.keyValue = value;
      return this;
    }

    public Builder transformValue(long value) {
      this.transformValue = value;
      return this;
    }

    public Builder filePath(String path) {
      this.filePath = path;
      return this;
    }

    public Builder position(long pos) {
      this.position = pos;
      return this;
    }

    public LeafFileEntry build() {
      Preconditions.checkArgument(keyValue != null, "keyValue is required");
      Preconditions.checkArgument(
          filePath != null && !filePath.isEmpty(), "filePath is required");
      Preconditions.checkArgument(position >= 0, "position must be >= 0, got: %s", position);
      return new LeafFileEntry(keyValue, transformValue, filePath, position);
    }
  }
}
