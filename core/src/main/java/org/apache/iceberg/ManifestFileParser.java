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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.io.BaseEncoding;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.JsonUtil;

class ManifestFileParser {
  private static final String PATH = "path";
  private static final String LENGTH = "length";
  private static final String SPEC_ID = "partition-spec-id";
  private static final String CONTENT = "content";
  private static final String SEQUENCE_NUMBER = "sequence-number";
  private static final String MIN_SEQUENCE_NUMBER = "min-sequence-number";
  private static final String ADDED_SNAPSHOT_ID = "added-snapshot-id";
  private static final String ADDED_FILES_COUNT = "added-files-count";
  private static final String EXISTING_FILES_COUNT = "existing-files-count";
  private static final String DELETED_FILES_COUNT = "deleted-files-count";
  private static final String ADDED_ROWS_COUNT = "added-rows-count";
  private static final String EXISTING_ROWS_COUNT = "existing-rows-count";
  private static final String DELETED_ROWS_COUNT = "deleted-rows-count";
  private static final String PARTITION_FIELD_SUMMARY = "partition-field-summary";
  private static final String KEY_METADATA = "key-metadata";

  private ManifestFileParser() {}

  static void toJson(ManifestFile manifestFile, JsonGenerator generator) throws IOException {
    Preconditions.checkArgument(manifestFile != null, "Invalid manifest file: null");
    Preconditions.checkArgument(generator != null, "Invalid JSON generator: null");

    generator.writeStartObject();

    generator.writeStringField(PATH, manifestFile.path());
    generator.writeNumberField(LENGTH, manifestFile.length());
    generator.writeNumberField(SPEC_ID, manifestFile.partitionSpecId());

    if (manifestFile.content() != null) {
      generator.writeNumberField(CONTENT, manifestFile.content().id());
    }

    generator.writeNumberField(SEQUENCE_NUMBER, manifestFile.sequenceNumber());
    generator.writeNumberField(MIN_SEQUENCE_NUMBER, manifestFile.minSequenceNumber());

    if (manifestFile.snapshotId() != null) {
      generator.writeNumberField(ADDED_SNAPSHOT_ID, manifestFile.snapshotId());
    }

    if (manifestFile.addedFilesCount() != null) {
      generator.writeNumberField(ADDED_FILES_COUNT, manifestFile.addedFilesCount());
    }

    if (manifestFile.existingFilesCount() != null) {
      generator.writeNumberField(EXISTING_FILES_COUNT, manifestFile.existingFilesCount());
    }

    if (manifestFile.deletedFilesCount() != null) {
      generator.writeNumberField(DELETED_FILES_COUNT, manifestFile.deletedFilesCount());
    }

    if (manifestFile.addedRowsCount() != null) {
      generator.writeNumberField(ADDED_ROWS_COUNT, manifestFile.addedRowsCount());
    }

    if (manifestFile.existingRowsCount() != null) {
      generator.writeNumberField(EXISTING_ROWS_COUNT, manifestFile.existingRowsCount());
    }

    if (manifestFile.deletedRowsCount() != null) {
      generator.writeNumberField(DELETED_ROWS_COUNT, manifestFile.deletedRowsCount());
    }

    if (manifestFile.partitions() != null) {
      generator.writeArrayFieldStart(PARTITION_FIELD_SUMMARY);
      for (ManifestFile.PartitionFieldSummary summary : manifestFile.partitions()) {
        PartitionFieldSummaryParser.toJson(summary, generator);
      }

      generator.writeEndArray();
    }

    if (manifestFile.keyMetadata() != null) {
      generator.writeStringField(
          KEY_METADATA,
          BaseEncoding.base16().encode(ByteBuffers.toByteArray(manifestFile.keyMetadata())));
    }

    generator.writeEndObject();
  }

  static ManifestFile fromJson(JsonNode jsonNode) {
    Preconditions.checkArgument(jsonNode != null, "Invalid JSON node for manifest file: null");
    Preconditions.checkArgument(
        jsonNode.isObject(), "Invalid JSON node for manifest file: non-object (%s)", jsonNode);

    String path = JsonUtil.getString(PATH, jsonNode);
    long length = JsonUtil.getLong(LENGTH, jsonNode);
    int specId = JsonUtil.getInt(SPEC_ID, jsonNode);

    ManifestContent manifestContent = null;
    if (jsonNode.has(CONTENT)) {
      manifestContent = ManifestContent.fromId(JsonUtil.getInt(CONTENT, jsonNode));
    }

    long sequenceNumber = JsonUtil.getLong(SEQUENCE_NUMBER, jsonNode);
    long minSequenceNumber = JsonUtil.getLong(MIN_SEQUENCE_NUMBER, jsonNode);

    Long addedSnapshotId = null;
    if (jsonNode.has(ADDED_SNAPSHOT_ID)) {
      addedSnapshotId = JsonUtil.getLong(ADDED_SNAPSHOT_ID, jsonNode);
    }

    Integer addedFilesCount = null;
    if (jsonNode.has(ADDED_FILES_COUNT)) {
      addedFilesCount = JsonUtil.getInt(ADDED_FILES_COUNT, jsonNode);
    }

    Integer existingFilesCount = null;
    if (jsonNode.has(EXISTING_FILES_COUNT)) {
      existingFilesCount = JsonUtil.getInt(EXISTING_FILES_COUNT, jsonNode);
    }

    Integer deletedFilesCount = null;
    if (jsonNode.has(DELETED_FILES_COUNT)) {
      deletedFilesCount = JsonUtil.getInt(DELETED_FILES_COUNT, jsonNode);
    }

    Long addedRowsCount = null;
    if (jsonNode.has(ADDED_ROWS_COUNT)) {
      addedRowsCount = JsonUtil.getLong(ADDED_ROWS_COUNT, jsonNode);
    }

    Long existingRowsCount = null;
    if (jsonNode.has(EXISTING_ROWS_COUNT)) {
      existingRowsCount = JsonUtil.getLong(EXISTING_ROWS_COUNT, jsonNode);
    }

    Long deletedRowsCount = null;
    if (jsonNode.has(DELETED_ROWS_COUNT)) {
      deletedRowsCount = JsonUtil.getLong(DELETED_ROWS_COUNT, jsonNode);
    }

    List<ManifestFile.PartitionFieldSummary> partitionFieldSummaries = null;
    if (jsonNode.has(PARTITION_FIELD_SUMMARY)) {
      JsonNode summaryArray = jsonNode.get(PARTITION_FIELD_SUMMARY);
      Preconditions.checkArgument(
          summaryArray.isArray(),
          "Invalid JSON node for partition field summaries: non-array (%s)",
          summaryArray);

      ImmutableList.Builder<ManifestFile.PartitionFieldSummary> builder = ImmutableList.builder();
      for (JsonNode summaryNode : summaryArray) {
        ManifestFile.PartitionFieldSummary summary =
            PartitionFieldSummaryParser.fromJson(summaryNode);
        builder.add(summary);
      }

      partitionFieldSummaries = builder.build();
    }

    ByteBuffer keyMetadata = null;
    if (jsonNode.has(KEY_METADATA)) {
      String hexStr = JsonUtil.getString(KEY_METADATA, jsonNode);
      keyMetadata = ByteBuffer.wrap(BaseEncoding.base16().decode(hexStr));
    }

    return new GenericManifestFile(
        path,
        length,
        specId,
        manifestContent,
        sequenceNumber,
        minSequenceNumber,
        addedSnapshotId,
        partitionFieldSummaries,
        keyMetadata,
        addedFilesCount,
        addedRowsCount,
        existingFilesCount,
        existingRowsCount,
        deletedFilesCount,
        deletedRowsCount);
  }

  private static class PartitionFieldSummaryParser {
    private static final String CONTAINS_NULL = "contains-null";
    private static final String CONTAINS_NAN = "contains-nan";
    private static final String LOWER_BOUND = "lower-bound";
    private static final String UPPER_BOUND = "upper-bound";

    private PartitionFieldSummaryParser() {}

    static void toJson(ManifestFile.PartitionFieldSummary summary, JsonGenerator generator)
        throws IOException {
      Preconditions.checkArgument(summary != null, "Invalid partition field summary: null");
      Preconditions.checkArgument(generator != null, "Invalid JSON generator: null");

      generator.writeStartObject();

      generator.writeBooleanField(CONTAINS_NULL, summary.containsNull());

      if (summary.containsNaN() != null) {
        generator.writeBooleanField(CONTAINS_NAN, summary.containsNaN());
      }

      if (summary.lowerBound() != null) {
        generator.writeStringField(
            LOWER_BOUND,
            BaseEncoding.base16().encode(ByteBuffers.toByteArray(summary.lowerBound())));
      }

      if (summary.upperBound() != null) {
        generator.writeStringField(
            UPPER_BOUND,
            BaseEncoding.base16().encode(ByteBuffers.toByteArray(summary.upperBound())));
      }

      generator.writeEndObject();
    }

    static ManifestFile.PartitionFieldSummary fromJson(JsonNode jsonNode) {
      Preconditions.checkArgument(
          jsonNode != null, "Invalid JSON node for partition field summary: null");
      Preconditions.checkArgument(
          jsonNode.isObject(),
          "Invalid JSON node for partition field summary: non-object (%s)",
          jsonNode);

      boolean containsNull = JsonUtil.getBool(CONTAINS_NULL, jsonNode);
      Boolean containsNaN = null;
      if (jsonNode.has(CONTAINS_NAN)) {
        containsNaN = JsonUtil.getBool(CONTAINS_NAN, jsonNode);
      }

      ByteBuffer lowerBound = null;
      if (jsonNode.has(LOWER_BOUND)) {
        String hexStr = JsonUtil.getString(LOWER_BOUND, jsonNode);
        lowerBound = ByteBuffer.wrap(BaseEncoding.base16().decode(hexStr));
      }

      ByteBuffer upperBound = null;
      if (jsonNode.has(UPPER_BOUND)) {
        String hexStr = JsonUtil.getString(UPPER_BOUND, jsonNode);
        upperBound = ByteBuffer.wrap(BaseEncoding.base16().decode(hexStr));
      }

      if (containsNaN != null) {
        return new GenericPartitionFieldSummary(containsNull, containsNaN, lowerBound, upperBound);
      } else {
        return new GenericPartitionFieldSummary(containsNull, lowerBound, upperBound);
      }
    }
  }
}
