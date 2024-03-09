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
package org.apache.iceberg.puffin;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.JsonUtil;

public final class FileMetadataParser {

  private FileMetadataParser() {}

  private static final String BLOBS = "blobs";
  private static final String PROPERTIES = "properties";

  private static final String TYPE = "type";
  private static final String FIELDS = "fields";
  private static final String SNAPSHOT_ID = "snapshot-id";
  private static final String SEQUENCE_NUMBER = "sequence-number";
  private static final String OFFSET = "offset";
  private static final String LENGTH = "length";
  private static final String COMPRESSION_CODEC = "compression-codec";

  public static String toJson(FileMetadata fileMetadata, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(fileMetadata, gen), pretty);
  }

  public static FileMetadata fromJson(String json) {
    return JsonUtil.parse(json, FileMetadataParser::fromJson);
  }

  static FileMetadata fromJson(JsonNode json) {
    return fileMetadataFromJson(json);
  }

  static void toJson(FileMetadata fileMetadata, JsonGenerator generator) throws IOException {
    generator.writeStartObject();

    generator.writeArrayFieldStart(BLOBS);
    for (BlobMetadata blobMetadata : fileMetadata.blobs()) {
      toJson(blobMetadata, generator);
    }
    generator.writeEndArray();

    if (!fileMetadata.properties().isEmpty()) {
      JsonUtil.writeStringMap(PROPERTIES, fileMetadata.properties(), generator);
    }

    generator.writeEndObject();
  }

  static FileMetadata fileMetadataFromJson(JsonNode json) {

    ImmutableList.Builder<BlobMetadata> blobs = ImmutableList.builder();
    JsonNode blobsJson = JsonUtil.get(BLOBS, json);
    Preconditions.checkArgument(
        blobsJson.isArray(), "Cannot parse blobs from non-array: %s", blobsJson);
    for (JsonNode blobJson : blobsJson) {
      blobs.add(blobMetadataFromJson(blobJson));
    }

    Map<String, String> properties = ImmutableMap.of();
    JsonNode propertiesJson = json.get(PROPERTIES);
    if (propertiesJson != null) {
      properties = JsonUtil.getStringMap(PROPERTIES, json);
    }

    return new FileMetadata(blobs.build(), properties);
  }

  static void toJson(BlobMetadata blobMetadata, JsonGenerator generator) throws IOException {
    generator.writeStartObject();

    generator.writeStringField(TYPE, blobMetadata.type());

    JsonUtil.writeIntegerArray(FIELDS, blobMetadata.inputFields(), generator);
    generator.writeNumberField(SNAPSHOT_ID, blobMetadata.snapshotId());
    generator.writeNumberField(SEQUENCE_NUMBER, blobMetadata.sequenceNumber());

    generator.writeNumberField(OFFSET, blobMetadata.offset());
    generator.writeNumberField(LENGTH, blobMetadata.length());

    if (blobMetadata.compressionCodec() != null) {
      generator.writeStringField(COMPRESSION_CODEC, blobMetadata.compressionCodec());
    }

    if (!blobMetadata.properties().isEmpty()) {
      JsonUtil.writeStringMap(PROPERTIES, blobMetadata.properties(), generator);
    }

    generator.writeEndObject();
  }

  static BlobMetadata blobMetadataFromJson(JsonNode json) {
    String type = JsonUtil.getString(TYPE, json);
    List<Integer> fields = JsonUtil.getIntegerList(FIELDS, json);
    long snapshotId = JsonUtil.getLong(SNAPSHOT_ID, json);
    long sequenceNumber = JsonUtil.getLong(SEQUENCE_NUMBER, json);
    long offset = JsonUtil.getLong(OFFSET, json);
    long length = JsonUtil.getLong(LENGTH, json);
    String compressionCodec = JsonUtil.getStringOrNull(COMPRESSION_CODEC, json);
    Map<String, String> properties = ImmutableMap.of();
    JsonNode propertiesJson = json.get(PROPERTIES);
    if (propertiesJson != null) {
      properties = JsonUtil.getStringMap(PROPERTIES, json);
    }

    return new BlobMetadata(
        type, fields, snapshotId, sequenceNumber, offset, length, compressionCodec, properties);
  }
}
