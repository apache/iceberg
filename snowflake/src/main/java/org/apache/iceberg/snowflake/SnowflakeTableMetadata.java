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
package org.apache.iceberg.snowflake;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

class SnowflakeTableMetadata {
  public static final Pattern SNOWFLAKE_AZURE_PATTERN =
      Pattern.compile("azure://([^/]+)/([^/]+)/(.*)");

  private final String snowflakeMetadataLocation;
  private final String icebergMetadataLocation;
  private final String status;

  // Note: Since not all sources will necessarily come from a raw JSON representation, this raw
  // JSON should only be considered a convenient debugging field. Equality of two
  // SnowflakeTableMetadata instances should not depend on equality of this field.
  private final String rawJsonVal;

  SnowflakeTableMetadata(
      String snowflakeMetadataLocation,
      String icebergMetadataLocation,
      String status,
      String rawJsonVal) {
    this.snowflakeMetadataLocation = snowflakeMetadataLocation;
    this.icebergMetadataLocation = icebergMetadataLocation;
    this.status = status;
    this.rawJsonVal = rawJsonVal;
  }

  /** Storage location of table metadata in Snowflake's path syntax. */
  public String snowflakeMetadataLocation() {
    return snowflakeMetadataLocation;
  }

  /** Storage location of table metadata in Iceberg's path syntax. */
  public String icebergMetadataLocation() {
    return icebergMetadataLocation;
  }

  public String getStatus() {
    return status;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof SnowflakeTableMetadata)) {
      return false;
    }

    // Only consider parsed fields, not the raw JSON that may or may not be the original source of
    // this instance.
    SnowflakeTableMetadata that = (SnowflakeTableMetadata) o;
    return Objects.equal(this.snowflakeMetadataLocation, that.snowflakeMetadataLocation)
        && Objects.equal(this.icebergMetadataLocation, that.icebergMetadataLocation)
        && Objects.equal(this.status, that.status);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(snowflakeMetadataLocation, icebergMetadataLocation, status);
  }

  @Override
  public String toString() {
    return String.format(
        "snowflakeMetadataLocation: '%s', icebergMetadataLocation: '%s', status: '%s'",
        snowflakeMetadataLocation, icebergMetadataLocation, status);
  }

  public String toDebugString() {
    return String.format("%s, rawJsonVal: %s", toString(), rawJsonVal);
  }

  /**
   * Translates from Snowflake's path syntax to Iceberg's path syntax for paths matching known
   * non-compatible Snowflake paths. Throws IllegalArgumentException if the prefix of the
   * snowflakeLocation is a known non-compatible path syntax but fails to match the expected path
   * components for a successful translation.
   */
  public static String snowflakeLocationToIcebergLocation(String snowflakeLocation) {
    if (snowflakeLocation.startsWith("azure://")) {
      // Convert from expected path of the form:
      // azure://account.blob.core.windows.net/container/volumepath
      // to:
      // wasbs://container@account.blob.core.windows.net/volumepath
      Matcher matcher = SNOWFLAKE_AZURE_PATTERN.matcher(snowflakeLocation);
      Preconditions.checkArgument(
          matcher.matches(),
          "Location '%s' failed to match pattern '%s'",
          snowflakeLocation,
          SNOWFLAKE_AZURE_PATTERN);
      return String.format(
          "wasbs://%s@%s/%s", matcher.group(2), matcher.group(1), matcher.group(3));
    } else if (snowflakeLocation.startsWith("gcs://")) {
      // Convert from expected path of the form:
      // gcs://bucket/path
      // to:
      // gs://bucket/path
      return "gs" + snowflakeLocation.substring(3);
    }

    return snowflakeLocation;
  }

  /**
   * Factory method for parsing a JSON string containing expected Snowflake table metadata into a
   * SnowflakeTableMetadata object.
   */
  public static SnowflakeTableMetadata parseJson(String json) {
    JsonNode parsedVal;
    try {
      parsedVal = JsonUtil.mapper().readValue(json, JsonNode.class);
    } catch (IOException ioe) {
      throw new IllegalArgumentException(String.format("Malformed JSON: %s", json), ioe);
    }

    String snowflakeMetadataLocation = JsonUtil.getString("metadataLocation", parsedVal);
    String status = JsonUtil.getStringOrNull("status", parsedVal);

    String icebergMetadataLocation = snowflakeLocationToIcebergLocation(snowflakeMetadataLocation);

    return new SnowflakeTableMetadata(
        snowflakeMetadataLocation, icebergMetadataLocation, status, json);
  }
}
