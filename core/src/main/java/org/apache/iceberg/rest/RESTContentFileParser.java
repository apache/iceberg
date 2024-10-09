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
package org.apache.iceberg.rest;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.GenericDataFile;
import org.apache.iceberg.GenericDeleteFile;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.SingleValueParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class RESTContentFileParser {
  private static final String SPEC_ID = "spec-id";
  private static final String CONTENT = "content";
  private static final String FILE_PATH = "file-path";
  private static final String FILE_FORMAT = "file-format";
  private static final String PARTITION = "partition";
  private static final String RECORD_COUNT = "record-count";
  private static final String FILE_SIZE_IN_BYTES = "file-size-in-bytes";
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

  private RESTContentFileParser() {}

  public static String toJson(ContentFile<?> contentFile) {
    return JsonUtil.generate(
        generator -> RESTContentFileParser.toJson(contentFile, generator), false);
  }

  public static void toJson(ContentFile<?> contentFile, JsonGenerator generator)
      throws IOException {
    Preconditions.checkArgument(contentFile != null, "Invalid content file: null");
    Preconditions.checkArgument(generator != null, "Invalid JSON generator: null");

    generator.writeStartObject();

    generator.writeNumberField(SPEC_ID, contentFile.specId());
    generator.writeStringField(CONTENT, contentFile.content().name());
    generator.writeStringField(FILE_PATH, contentFile.path().toString());
    generator.writeStringField(FILE_FORMAT, contentFile.format().name());

    generator.writeFieldName(PARTITION);

    // TODO at the time of serialization we dont have the partition spec we just have spec id.
    // we will need to get the spec from table metadata using spec id.
    // or we will need to send parition spec, put null here for now until refresh
    SingleValueParser.toJson(null, contentFile.partition(), generator);

    generator.writeNumberField(FILE_SIZE_IN_BYTES, contentFile.fileSizeInBytes());

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

  public static ContentFile<?> fromJson(JsonNode jsonNode) {
    Preconditions.checkArgument(jsonNode != null, "Invalid JSON node for content file: null");
    Preconditions.checkArgument(
        jsonNode.isObject(), "Invalid JSON node for content file: non-object (%s)", jsonNode);

    int specId = JsonUtil.getInt(SPEC_ID, jsonNode);
    FileContent fileContent = FileContent.valueOf(JsonUtil.getString(CONTENT, jsonNode));
    String filePath = JsonUtil.getString(FILE_PATH, jsonNode);
    FileFormat fileFormat = FileFormat.fromString(JsonUtil.getString(FILE_FORMAT, jsonNode));

    // TODO at the time of deserialization we dont have the partition spec we just have spec id.
    // we will need to get the spec from table metadata using spec id.
    // we wil need to send parition spec i believe, put some placeholder here for now null
    PartitionData partitionData = new PartitionData();

    long fileSizeInBytes = JsonUtil.getLong(FILE_SIZE_IN_BYTES, jsonNode);
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
