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
package org.apache.iceberg.deletes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

public class EqualityDeleteWriter<T> implements FileWriter<T, DeleteWriteResult> {
  private final FileAppender<T> appender;
  private final FileFormat format;
  private final String location;
  private final PartitionSpec spec;
  private final StructLike partition;
  private final ByteBuffer keyMetadata;
  private final int[] equalityFieldIds;
  private final SortOrder sortOrder;
  private DeleteFile deleteFile = null;

  public EqualityDeleteWriter(
      FileAppender<T> appender,
      FileFormat format,
      String location,
      PartitionSpec spec,
      StructLike partition,
      EncryptionKeyMetadata keyMetadata,
      SortOrder sortOrder,
      int... equalityFieldIds) {
    this.appender = appender;
    this.format = format;
    this.location = location;
    this.spec = spec;
    this.partition = partition;
    this.keyMetadata = keyMetadata != null ? keyMetadata.buffer() : null;
    this.sortOrder = sortOrder;
    this.equalityFieldIds = equalityFieldIds;
  }

  /**
   * Validates that the given equality delete field IDs are valid for the provided schema.
   *
   * <p>This rejects null or empty ID lists, duplicate IDs, and IDs that do not exist in the schema.
   *
   * @param equalityFieldIds field IDs to use for equality deletes
   * @param schema the schema to validate the field IDs against
   * @throws IllegalArgumentException if the field IDs are invalid
   */
  public static void validateEqualityFieldIds(int[] equalityFieldIds, Schema schema) {
    Preconditions.checkArgument(
        equalityFieldIds != null && equalityFieldIds.length > 0,
        "Equality delete field ids must not be null or empty");
    Preconditions.checkNotNull(schema, "Schema must not be null");

    Set<Integer> seen = Sets.newHashSetWithExpectedSize(equalityFieldIds.length);
    for (int fieldId : equalityFieldIds) {
      Preconditions.checkArgument(
          seen.add(fieldId), "Duplicate equality delete field id: %s", fieldId);
      Preconditions.checkArgument(
          schema.findField(fieldId) != null, "Invalid equality delete field id: %s", fieldId);
    }
  }

  @Override
  public void write(T row) {
    appender.add(row);
  }

  @Override
  public long length() {
    return appender.length();
  }

  @Override
  public void close() throws IOException {
    if (deleteFile == null) {
      appender.close();
      this.deleteFile =
          FileMetadata.deleteFileBuilder(spec)
              .ofEqualityDeletes(equalityFieldIds)
              .withFormat(format)
              .withPath(location)
              .withPartition(partition)
              .withEncryptionKeyMetadata(
                  EncryptionUtil.setFileLength(keyMetadata, appender.length()))
              .withFileSizeInBytes(appender.length())
              .withMetrics(appender.metrics())
              .withSplitOffsets(appender.splitOffsets())
              .withSortOrder(sortOrder)
              .build();
    }
  }

  public DeleteFile toDeleteFile() {
    Preconditions.checkState(deleteFile != null, "Cannot create delete file from unclosed writer");
    return deleteFile;
  }

  @Override
  public DeleteWriteResult result() {
    return new DeleteWriteResult(toDeleteFile());
  }
}
