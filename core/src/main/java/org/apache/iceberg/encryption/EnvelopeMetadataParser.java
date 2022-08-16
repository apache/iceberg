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
package org.apache.iceberg.encryption;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class EnvelopeMetadataParser {

  private EnvelopeMetadataParser() {}

  private static final String KEK_ID = "kek-id";
  private static final String WRAPPED_DEK = "wrapped-dek";
  private static final String ALGORITHM = "algorithm";
  // TODO remove in OSS version
  private static final String APPLE_VERSION = "apple-version";
  private static final String APPLE_V1 = "av1";

  public static void toJson(EnvelopeMetadata metadata, JsonGenerator generator) throws IOException {
    generator.writeStartObject();

    generator.writeStringField(KEK_ID, metadata.kekId());
    generator.writeStringField(WRAPPED_DEK, metadata.wrappedDek());
    JsonUtil.writeStringIf(
        null != metadata.algorithm(), ALGORITHM, metadata.algorithm().name(), generator);

    // TODO remove in OSS version
    generator.writeStringField(APPLE_VERSION, APPLE_V1);

    generator.writeEndObject();
  }

  public static ByteBuffer toJson(EnvelopeMetadata metadata) {
    try {
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      JsonGenerator generator = JsonUtil.factory().createGenerator(stream);
      toJson(metadata, generator);
      generator.flush();
      return ByteBuffer.wrap(stream.toByteArray());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static EnvelopeMetadata fromJson(JsonNode json) throws IOException {
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse envelope metadata from non-object: %s", json);
    String kekId = JsonUtil.getStringOrNull(KEK_ID, json);
    String wrappedDek = JsonUtil.getStringOrNull(WRAPPED_DEK, json);
    String algorithmName = JsonUtil.getStringOrNull(ALGORITHM, json);
    EncryptionAlgorithm algorithm =
        null == algorithmName ? null : EncryptionAlgorithm.valueOf(algorithmName);

    EnvelopeMetadata metadata = new EnvelopeMetadata(kekId, wrappedDek, algorithm);
    return metadata;
  }

  public static EnvelopeMetadata fromJson(ByteBuffer buffer) {
    try {
      return fromJson(JsonUtil.mapper().readValue(buffer.array(), JsonNode.class));
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse envelope encryption metadata", e);
    }
  }
}
