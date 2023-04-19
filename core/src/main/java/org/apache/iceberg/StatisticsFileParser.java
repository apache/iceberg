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
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.JsonUtil;

public class StatisticsFileParser {

  private static final String SNAPSHOT_ID = "snapshot-id";
  private static final String STATISTICS_PATH = "statistics-path";
  private static final String FILE_SIZE_IN_BYTES = "file-size-in-bytes";
  private static final String FILE_FOOTER_SIZE_IN_BYTES = "file-footer-size-in-bytes";
  private static final String BLOB_METADATA = "blob-metadata";
  private static final String TYPE = "type";
  private static final String SEQUENCE_NUMBER = "sequence-number";
  private static final String FIELDS = "fields";
  private static final String PROPERTIES = "properties";

  private StatisticsFileParser() {}

  public static String toJson(StatisticsFile statisticsFile) {
    return toJson(statisticsFile, false);
  }

  public static String toJson(StatisticsFile statisticsFile, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(statisticsFile, gen), pretty);
  }

  public static void toJson(StatisticsFile statisticsFile, JsonGenerator generator)
      throws IOException {
    generator.writeStartObject();
    generator.writeNumberField(SNAPSHOT_ID, statisticsFile.snapshotId());
    generator.writeStringField(STATISTICS_PATH, statisticsFile.path());
    generator.writeNumberField(FILE_SIZE_IN_BYTES, statisticsFile.fileSizeInBytes());
    generator.writeNumberField(FILE_FOOTER_SIZE_IN_BYTES, statisticsFile.fileFooterSizeInBytes());
    generator.writeArrayFieldStart(BLOB_METADATA);
    for (BlobMetadata blobMetadata : statisticsFile.blobMetadata()) {
      toJson(blobMetadata, generator);
    }
    generator.writeEndArray();
    generator.writeEndObject();
  }

  static StatisticsFile fromJson(JsonNode node) {
    long snapshotId = JsonUtil.getLong(SNAPSHOT_ID, node);
    String path = JsonUtil.getString(STATISTICS_PATH, node);
    long fileSizeInBytes = JsonUtil.getLong(FILE_SIZE_IN_BYTES, node);
    long fileFooterSizeInBytes = JsonUtil.getLong(FILE_FOOTER_SIZE_IN_BYTES, node);
    ImmutableList.Builder<BlobMetadata> blobMetadata = ImmutableList.builder();
    JsonNode blobsJson = node.get(BLOB_METADATA);
    Preconditions.checkArgument(
        blobsJson != null && blobsJson.isArray(),
        "Cannot parse blob metadata from non-array: %s",
        blobsJson);
    for (JsonNode blobJson : blobsJson) {
      blobMetadata.add(blobMetadataFromJson(blobJson));
    }
    return new GenericStatisticsFile(
        snapshotId, path, fileSizeInBytes, fileFooterSizeInBytes, blobMetadata.build());
  }

  private static void toJson(BlobMetadata blobMetadata, JsonGenerator generator)
      throws IOException {
    generator.writeStartObject();
    generator.writeStringField(TYPE, blobMetadata.type());
    generator.writeNumberField(SNAPSHOT_ID, blobMetadata.sourceSnapshotId());
    generator.writeNumberField(SEQUENCE_NUMBER, blobMetadata.sourceSnapshotSequenceNumber());
    generator.writeArrayFieldStart(FIELDS);
    for (int field : blobMetadata.fields()) {
      generator.writeNumber(field);
    }
    generator.writeEndArray();

    if (!blobMetadata.properties().isEmpty()) {
      JsonUtil.writeStringMap(PROPERTIES, blobMetadata.properties(), generator);
    }
    generator.writeEndObject();
  }

  private static BlobMetadata blobMetadataFromJson(JsonNode node) {
    String type = JsonUtil.getString(TYPE, node);
    long sourceSnapshotId = JsonUtil.getLong(SNAPSHOT_ID, node);
    long sourceSnapshotSequenceNumber = JsonUtil.getLong(SEQUENCE_NUMBER, node);
    List<Integer> fields = JsonUtil.getIntegerList(FIELDS, node);
    Map<String, String> properties;
    if (node.has(PROPERTIES)) {
      properties = JsonUtil.getStringMap(PROPERTIES, node);
    } else {
      properties = ImmutableMap.of();
    }
    return new GenericBlobMetadata(
        type, sourceSnapshotId, sourceSnapshotSequenceNumber, fields, properties);
  }
}
