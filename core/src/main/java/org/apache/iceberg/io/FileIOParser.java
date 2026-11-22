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
package org.apache.iceberg.io;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.JsonUtil;

public class FileIOParser {
  private FileIOParser() {}

  private static final String FILE_IO_IMPL = "io-impl";
  private static final String PROPERTIES = "properties";
  private static final String STORAGE_CREDENTIALS = "storage-credentials";

  public static String toJson(FileIO io) {
    return toJson(io, false);
  }

  public static String toJson(FileIO io, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(io, gen), pretty);
  }

  public static void toJson(FileIO io, JsonGenerator generator) throws IOException {
    Preconditions.checkArgument(io != null, "Invalid FileIO: null");
    Preconditions.checkArgument(generator != null, "Invalid JSON generator: null");

    String impl = io.getClass().getName();
    Map<String, String> properties;
    try {
      properties = io.properties();
    } catch (UnsupportedOperationException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot serialize FileIO: %s does not expose configuration properties", impl));
    }

    Preconditions.checkArgument(
        properties != null,
        "Cannot serialize FileIO: invalid configuration properties (null)",
        impl);

    generator.writeStartObject();

    generator.writeStringField(FILE_IO_IMPL, impl);
    JsonUtil.writeStringMap(PROPERTIES, properties, generator);

    if (io instanceof SupportsStorageCredentials) {
      List<StorageCredential> credentials = ((SupportsStorageCredentials) io).credentials();
      if (credentials != null && !credentials.isEmpty()) {
        generator.writeArrayFieldStart(STORAGE_CREDENTIALS);
        for (StorageCredential credential : credentials) {
          StorageCredentialParser.toJson(credential, generator);
        }
        generator.writeEndArray();
      }
    }

    generator.writeEndObject();
  }

  public static FileIO fromJson(String json) {
    return fromJson(json, null);
  }

  public static FileIO fromJson(String json, Object conf) {
    return JsonUtil.parse(json, node -> fromJson(node, conf));
  }

  public static FileIO fromJson(JsonNode json, Object conf) {
    Preconditions.checkArgument(json != null, "Cannot parse FileIO from null object");
    Preconditions.checkArgument(json.isObject(), "Cannot parse FileIO from non-object: %s", json);
    String impl = JsonUtil.getString(FILE_IO_IMPL, json);
    Map<String, String> properties = JsonUtil.getStringMap(PROPERTIES, json);

    List<StorageCredential> credentials = ImmutableList.of();
    if (json.has(STORAGE_CREDENTIALS)) {
      JsonNode credentialsNode = json.get(STORAGE_CREDENTIALS);
      Preconditions.checkArgument(
          credentialsNode != null && credentialsNode.isArray(),
          "Cannot parse storage credentials from non-array: %s",
          credentialsNode);
      ImmutableList.Builder<StorageCredential> builder = ImmutableList.builder();
      for (JsonNode credentialNode : credentialsNode) {
        builder.add(StorageCredentialParser.fromJson(credentialNode));
      }
      credentials = builder.build();
    }

    return CatalogUtil.loadFileIO(impl, properties, conf, credentials);
  }
}
