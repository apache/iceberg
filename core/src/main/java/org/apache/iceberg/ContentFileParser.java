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
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

class ContentFileParser {
  private static final String SPEC_ID = "spec-id";
  private static final String CONTENT = "content";
  private static final String FILE_PATH = "file-path";
  private static final String FILE_FORMAT = "file-format";
  private static final String PARTITION = "partition";
  private static final String RECORD_COUNT = "record-count";
  private static final String FILE_SIZE = "file-size-in-bytes";
  private static final String COLUMN_SIZES = "column-sizes";
  private static final String VALUE_COUNTS = "value-counts";
  private static final String NULL_VALUE_COUNTS = "null-value-counts";
  private static final String NAN_VALUE_COUNTS = "nan-value-counts";
  private static final String LOWER_BOUNDS = "lower-bounds";
  private static final String UPPER_BOUNDS = "upper-bounds";
  private static final String KEY_METADATA = "key-metadata";
  private static final String SPLIT_OFFSETS = "split-offsets";
  private static final String EQUALITY_IDS = "equality-ids";
  private static final String SORT_ORDER_ID = "sort-order-id";

  private ContentFileParser() {}

  private static boolean hasPartitionData(StructLike partitionData) {
    return partitionData != null && partitionData.size() > 0;
  }

  static String toJson(ContentFile<?> contentFile, PartitionSpec spec) {
    return JsonUtil.generate(
        generator -> ContentFileParser.toJson(contentFile, spec, generator), false);
  }

  static void toJson(ContentFile<?> contentFile, PartitionSpec spec, JsonGenerator generator)
      throws IOException {
    Preconditions.checkArgument(contentFile != null, "Invalid content file: null");
    Preconditions.checkArgument(spec != null, "Invalid partition spec: null");
    Preconditions.checkArgument(generator != null, "Invalid JSON generator: null");
    Preconditions.checkArgument(
        contentFile.specId() == spec.specId(),
        "Invalid partition spec id from content file: expected = %s, actual = %s",
        spec.specId(),
        contentFile.specId());
    Preconditions.checkArgument(
        spec.isPartitioned() == hasPartitionData(contentFile.partition()),
        "Invalid partition data from content file: expected = %s, actual = %s",
        spec.isPartitioned() ? "partitioned" : "unpartitioned",
        hasPartitionData(contentFile.partition()) ? "partitioned" : "unpartitioned");

    generator.writeStartObject();

    // ignore the ordinal position (ContentFile#pos) of the file in a manifest,
    // as it isn't used and BaseFile constructor doesn't support it.

    generator.writeNumberField(SPEC_ID, contentFile.specId());
    generator.writeStringField(CONTENT, contentFile.content().name());
    generator.writeStringField(FILE_PATH, contentFile.path().toString());
    generator.writeStringField(FILE_FORMAT, contentFile.format().name());

    if (contentFile.partition() != null) {
      generator.writeFieldName(PARTITION);
      SingleValueParser.toJson(spec.partitionType(), contentFile.partition(), generator);
    }

    generator.writeNumberField(FILE_SIZE, contentFile.fileSizeInBytes());

    metricsToJson(contentFile, generator);

    if (contentFile.keyMetadata() != null) {
      generator.writeFieldName(KEY_METADATA);
      SingleValueParser.toJson(DataFile.KEY_METADATA.type(), contentFile.keyMetadata(), generator);
    }

    if (contentFile.splitOffsets() != null) {
      JsonUtil.writeLongArray(SPLIT_OFFSETS, contentFile.splitOffsets(), generator);
    }

    if (contentFile.equalityFieldIds() != null) {
      JsonUtil.writeIntegerArray(EQUALITY_IDS, contentFile.equalityFieldIds(), generator);
    }

    if (contentFile.sortOrderId() != null) {
      generator.writeNumberField(SORT_ORDER_ID, contentFile.sortOrderId());
    }

    generator.writeEndObject();
  }

  static ContentFile<?> fromJson(JsonNode jsonNode, PartitionSpec spec) {
    Preconditions.checkArgument(jsonNode != null, "Invalid JSON node for content file: null");
    Preconditions.checkArgument(
        jsonNode.isObject(), "Invalid JSON node for content file: non-object (%s)", jsonNode);
    Preconditions.checkArgument(spec != null, "Invalid partition spec: null");

    int specId = JsonUtil.getInt(SPEC_ID, jsonNode);
    FileContent fileContent = FileContent.valueOf(JsonUtil.getString(CONTENT, jsonNode));
    String filePath = JsonUtil.getString(FILE_PATH, jsonNode);
    FileFormat fileFormat = FileFormat.fromString(JsonUtil.getString(FILE_FORMAT, jsonNode));

    PartitionData partitionData = null;
    if (jsonNode.has(PARTITION)) {
      partitionData = new PartitionData(spec.partitionType());
      StructLike structLike =
          (StructLike) SingleValueParser.fromJson(spec.partitionType(), jsonNode.get(PARTITION));
      Preconditions.checkState(
          partitionData.size() == structLike.size(),
          "Invalid partition data size: expected = %s, actual = %s",
          partitionData.size(),
          structLike.size());
      for (int pos = 0; pos < partitionData.size(); ++pos) {
        Class<?> javaClass = spec.partitionType().fields().get(pos).type().typeId().javaClass();
        partitionData.set(pos, structLike.get(pos, javaClass));
      }
    }

    long fileSizeInBytes = JsonUtil.getLong(FILE_SIZE, jsonNode);
    Metrics metrics = metricsFromJson(jsonNode);
    ByteBuffer keyMetadata = JsonUtil.getByteBufferOrNull(KEY_METADATA, jsonNode);
    List<Long> splitOffsets = JsonUtil.getLongListOrNull(SPLIT_OFFSETS, jsonNode);
    int[] equalityFieldIds = JsonUtil.getIntArrayOrNull(EQUALITY_IDS, jsonNode);
    Integer sortOrderId = JsonUtil.getIntOrNull(SORT_ORDER_ID, jsonNode);

    if (fileContent == FileContent.DATA) {
      return new GenericDataFile(
          specId,
          filePath,
          fileFormat,
          partitionData,
          fileSizeInBytes,
          metrics,
          keyMetadata,
          splitOffsets,
          sortOrderId);
    } else {
      return new GenericDeleteFile(
          specId,
          fileContent,
          filePath,
          fileFormat,
          partitionData,
          fileSizeInBytes,
          metrics,
          equalityFieldIds,
          sortOrderId,
          splitOffsets,
          keyMetadata);
    }
  }

  private static void metricsToJson(ContentFile<?> contentFile, JsonGenerator generator)
      throws IOException {
    generator.writeNumberField(RECORD_COUNT, contentFile.recordCount());

    if (contentFile.columnSizes() != null) {
      generator.writeFieldName(COLUMN_SIZES);
      SingleValueParser.toJson(DataFile.COLUMN_SIZES.type(), contentFile.columnSizes(), generator);
    }

    if (contentFile.valueCounts() != null) {
      generator.writeFieldName(VALUE_COUNTS);
      SingleValueParser.toJson(DataFile.VALUE_COUNTS.type(), contentFile.valueCounts(), generator);
    }

    if (contentFile.nullValueCounts() != null) {
      generator.writeFieldName(NULL_VALUE_COUNTS);
      SingleValueParser.toJson(
          DataFile.NULL_VALUE_COUNTS.type(), contentFile.nullValueCounts(), generator);
    }

    if (contentFile.nullValueCounts() != null) {
      generator.writeFieldName(NAN_VALUE_COUNTS);
      SingleValueParser.toJson(
          DataFile.NAN_VALUE_COUNTS.type(), contentFile.nanValueCounts(), generator);
    }

    if (contentFile.lowerBounds() != null) {
      generator.writeFieldName(LOWER_BOUNDS);
      SingleValueParser.toJson(DataFile.LOWER_BOUNDS.type(), contentFile.lowerBounds(), generator);
    }

    if (contentFile.upperBounds() != null) {
      generator.writeFieldName(UPPER_BOUNDS);
      SingleValueParser.toJson(DataFile.UPPER_BOUNDS.type(), contentFile.upperBounds(), generator);
    }
  }

  private static Metrics metricsFromJson(JsonNode jsonNode) {
    long recordCount = JsonUtil.getLong(RECORD_COUNT, jsonNode);

    Map<Integer, Long> columnSizes = null;
    if (jsonNode.has(COLUMN_SIZES)) {
      columnSizes =
          (Map<Integer, Long>)
              SingleValueParser.fromJson(DataFile.COLUMN_SIZES.type(), jsonNode.get(COLUMN_SIZES));
    }

    Map<Integer, Long> valueCounts = null;
    if (jsonNode.has(VALUE_COUNTS)) {
      valueCounts =
          (Map<Integer, Long>)
              SingleValueParser.fromJson(DataFile.VALUE_COUNTS.type(), jsonNode.get(VALUE_COUNTS));
    }

    Map<Integer, Long> nullValueCounts = null;
    if (jsonNode.has(NULL_VALUE_COUNTS)) {
      nullValueCounts =
          (Map<Integer, Long>)
              SingleValueParser.fromJson(
                  DataFile.NULL_VALUE_COUNTS.type(), jsonNode.get(NULL_VALUE_COUNTS));
    }

    Map<Integer, Long> nanValueCounts = null;
    if (jsonNode.has(NAN_VALUE_COUNTS)) {
      nanValueCounts =
          (Map<Integer, Long>)
              SingleValueParser.fromJson(
                  DataFile.NAN_VALUE_COUNTS.type(), jsonNode.get(NAN_VALUE_COUNTS));
    }

    Map<Integer, ByteBuffer> lowerBounds = null;
    if (jsonNode.has(LOWER_BOUNDS)) {
      lowerBounds =
          (Map<Integer, ByteBuffer>)
              SingleValueParser.fromJson(DataFile.LOWER_BOUNDS.type(), jsonNode.get(LOWER_BOUNDS));
    }

    Map<Integer, ByteBuffer> upperBounds = null;
    if (jsonNode.has(UPPER_BOUNDS)) {
      upperBounds =
          (Map<Integer, ByteBuffer>)
              SingleValueParser.fromJson(DataFile.UPPER_BOUNDS.type(), jsonNode.get(UPPER_BOUNDS));
    }

    return new Metrics(
        recordCount,
        columnSizes,
        valueCounts,
        nullValueCounts,
        nanValueCounts,
        lowerBounds,
        upperBounds);
  }
}
