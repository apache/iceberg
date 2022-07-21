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

package org.apache.iceberg.rest.responses;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.util.JsonUtil;

/**
 * A REST response that is used when a table is successfully loaded.
 * <p>
 * This class is used whenever the response to a request is a table's requested metadata and the associated location
 * of its metadata, to reduce code duplication. This includes using this class as the response for
 * {@link org.apache.iceberg.rest.requests.CreateTableRequest}, including when that request is used to commit
 * an already staged table creation as part of a transaction.
 */
public class LoadTableResponse implements RESTResponse {

  private static final String METADATA_LOCATION = "metadata-location";
  private static final String METADATA = "metadata";
  private static final String CONFIG = "config";

  public static String toJson(LoadTableResponse response) {
    try (StringWriter writer = new StringWriter()) {
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      toJson(response, generator);
      generator.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  public static void toJson(LoadTableResponse loadTable, JsonGenerator generator) throws IOException {
    generator.writeStartObject();

    generator.writeStringField(METADATA_LOCATION, loadTable.metadataLocation);

    generator.writeFieldName(METADATA);
    TableMetadataParser.toJson(loadTable.metadata, generator);

    generator.writeObjectFieldStart(CONFIG);
    for (Map.Entry<String, String> keyValue : loadTable.config.entrySet()) {
      generator.writeStringField(keyValue.getKey(), keyValue.getValue());
    }
    generator.writeEndObject();

    generator.writeEndObject();
  }

  public static LoadTableResponse fromJson(String json) {
    try {
      return fromJson(JsonUtil.mapper().readValue(json, JsonNode.class));
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  public static LoadTableResponse fromJson(JsonNode node) {
    Preconditions.checkArgument(node.isObject(),
            "Cannot parse table response from a non-object: %s", node);

    String metadataLocation = JsonUtil.getStringOrNull(METADATA_LOCATION, node);
    TableMetadata metadata = TableMetadataParser.fromJson(node.get(METADATA));

    JsonNode config = node.get(CONFIG);
    ImmutableMap.Builder<String, String> configEntries = ImmutableMap.builder();

    if (config != null) {
      Preconditions.checkArgument(config.isObject(),
              "Cannot parse config from a non-object: %s", node);
      Iterator<String> configNames = config.fieldNames();
      while (configNames.hasNext()) {
        String configKey = configNames.next();
        JsonNode configValue = config.get(configKey);
        Preconditions.checkArgument(
                configValue.isTextual(), "Cannot parse config %s from non-string: %s", configKey, configValue);
        configEntries.put(configKey, configValue.textValue());
      }
    }

    return LoadTableResponse
            .builder()
            .withMetadataLocation(metadataLocation)
            .withTableMetadata(metadata)
            .addAllConfig(configEntries.build())
            .build();
  }

  private String metadataLocation;
  private TableMetadata metadata;
  private Map<String, String> config;

  public LoadTableResponse() {
    // Required for Jackson deserialization
  }

  private LoadTableResponse(String metadataLocation, TableMetadata metadata, Map<String, String> config) {
    this.metadataLocation = metadataLocation;
    this.metadata = metadata;
    this.config = config;
  }

  @Override
  public void validate() {
    Preconditions.checkNotNull(metadata, "Invalid metadata: null");
  }

  public String metadataLocation() {
    return metadataLocation;
  }

  public TableMetadata tableMetadata() {
    return TableMetadata.buildFrom(metadata).withMetadataLocation(metadataLocation).build();
  }

  public Map<String, String> config() {
    return config != null ? config : ImmutableMap.of();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("metadataLocation", metadataLocation)
        .add("metadata", metadata)
        .add("config", config)
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String metadataLocation;
    private TableMetadata metadata;
    private Map<String, String> config = Maps.newHashMap();

    private Builder() {
    }

    public Builder withMetadataLocation(String location) {
      this.metadataLocation = location;
      return this;
    }

    public Builder withTableMetadata(TableMetadata tableMetadata) {
      if (this.metadataLocation == null) {
        this.metadataLocation = tableMetadata.metadataFileLocation();
      }
      this.metadata = tableMetadata;
      return this;
    }

    public Builder addConfig(String property, String value) {
      config.put(property, value);
      return this;
    }

    public Builder addAllConfig(Map<String, String> properties) {
      config.putAll(properties);
      return this;
    }

    public LoadTableResponse build() {
      Preconditions.checkNotNull(metadata, "Invalid metadata: null");
      return new LoadTableResponse(metadataLocation, metadata, config);
    }
  }
}
