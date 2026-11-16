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
package org.apache.iceberg.io.canonicaluri;

import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.LocationProvider;

/** A location provider that only produces locations in a configured canonical namespace. */
public final class CanonicalLocationProvider implements LocationProvider {
  private final String dataLocation;
  private final ParsedLocation canonicalPrefix;
  private final String canonicalBasePath;

  /**
   * Creates a provider for an Iceberg table.
   *
   * @param tableLocation canonical table location
   * @param properties Iceberg table properties
   */
  public CanonicalLocationProvider(String tableLocation, Map<String, String> properties) {
    String canonicalPrefixLocation =
        properties.getOrDefault(
            PathAliasProperties.LOCATION_PROVIDER_CANONICAL_PREFIX, tableLocation);
    this.canonicalPrefix =
        ParsedLocation.parse(canonicalPrefixLocation, "location-provider canonical prefix");
    this.canonicalBasePath = stripTrailingSlash(canonicalPrefix.rawPath());
    this.dataLocation =
        stripTrailingSlash(
            properties.getOrDefault(
                TableProperties.WRITE_DATA_LOCATION, stripTrailingSlash(tableLocation) + "/data"));
    validateCanonical(tableLocation);
    validateCanonical(dataLocation);
    String metadataLocation = properties.get(TableProperties.WRITE_METADATA_LOCATION);
    if (metadataLocation != null) {
      validateCanonical(metadataLocation);
    }
  }

  @Override
  public String newDataLocation(String filename) {
    validateFilename(filename);
    String location = dataLocation + "/" + filename;
    validateCanonical(location);
    return location;
  }

  @Override
  public String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename) {
    String partitionPath = spec.partitionToPath(partitionData);
    return partitionPath.isEmpty()
        ? newDataLocation(filename)
        : newDataLocation(partitionPath + "/" + filename);
  }

  private void validateCanonical(String location) {
    ParsedLocation parsed = ParsedLocation.parse(location, "data location");
    String path = parsed.rawPath();
    boolean matchesRoot = canonicalBasePath.isEmpty();
    boolean matchesPath =
        path.equals(canonicalBasePath) || path.startsWith(canonicalBasePath + "/");
    if (!canonicalPrefix.hasSameEndpoint(parsed) || (!matchesRoot && !matchesPath)) {
      throw new IllegalArgumentException(
          "Location is outside the configured canonical namespace: " + location);
    }
  }

  private static void validateFilename(String filename) {
    if (filename == null || filename.isEmpty() || filename.startsWith("/")) {
      throw new IllegalArgumentException("Invalid data filename: " + filename);
    }
  }

  private static String stripTrailingSlash(String location) {
    int end = location.length();
    while (end > 0 && location.charAt(end - 1) == '/') {
      end -= 1;
    }

    return location.substring(0, end);
  }
}
