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

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** This class contains utility methods for working with metadata of a table. */
public class MetadataUtil {
  private static final String METADATA_FOLDER_NAME = "metadata";

  private MetadataUtil() {}

  /**
   * Returns the metadata location for a table, based on the specified table location and
   * properties. If the {@code TableProperties.WRITE_METADATA_LOCATION} property is set in the
   * properties map, the value of that property will be returned. Otherwise, the metadata location
   * will be constructed by appending the metadata folder name to the table location.
   *
   * @param table the table for which the metadata location is to be determined
   * @return the metadata location for the table
   * @throws IllegalArgumentException if the table or its location or properties map is null
   */
  public static String metadataLocation(Table table) {
    Preconditions.checkArgument(table != null, "Invalid table: null");
    return metadataLocation(table.location(), table.properties());
  }

  /**
   * Returns the metadata location for a table metadata object, based on the specified table
   * location and properties. If the {@code TableProperties.WRITE_METADATA_LOCATION} property is set
   * in the properties map, the value of that property will be returned. Otherwise, the metadata
   * location will be constructed by appending the metadata folder name to the table location.
   *
   * @param metadata the table metadata object for which the metadata location is to be determined
   * @return the metadata location for the table metadata object
   * @throws IllegalArgumentException if the metadata or its location or properties map is null
   */
  public static String metadataLocation(TableMetadata metadata) {
    Preconditions.checkArgument(metadata != null, "Invalid metadata: null");
    return metadataLocation(metadata.location(), metadata.properties());
  }

  /**
   * Returns a new metadata location based on the provided table location, properties, and file
   * name.
   *
   * @param table the table for which the new metadata location is to be determined
   * @param filename the name of the file to be included in the metadata location
   * @return a new metadata location as a string
   * @throws IllegalArgumentException if the table or its location or properties map is null, or if
   *     the filename is null
   */
  public static String newMetadataLocation(Table table, String filename) {
    Preconditions.checkArgument(table != null, "Invalid table: null");
    return newMetadataLocation(table.location(), table.properties(), filename);
  }

  /**
   * Returns a new metadata location based on the provided for a table metadata object and file
   * name.
   *
   * @param metadata the table metadata object for which the new metadata location is to be
   *     determined
   * @param filename the name of the file to be included in the metadata location
   * @return a new metadata location as a string
   * @throws IllegalArgumentException if the table or its location or properties map is null, or if
   *     the filename is null
   */
  public static String newMetadataLocation(TableMetadata metadata, String filename) {
    return newMetadataLocation(metadata.location(), metadata.properties(), filename);
  }

  private static String metadataLocation(String tableLocation, Map<String, String> properties) {
    Preconditions.checkArgument(tableLocation != null, "Invalid table location: null");
    Preconditions.checkArgument(properties != null, "Invalid properties: null");

    String metadataLocation = properties.get(TableProperties.WRITE_METADATA_LOCATION);
    if (metadataLocation != null) {
      return metadataLocation;
    } else {
      return String.format("%s/%s", tableLocation, METADATA_FOLDER_NAME);
    }
  }

  private static String newMetadataLocation(
      String tableLocation, Map<String, String> properties, String filename) {
    Preconditions.checkArgument(filename != null, "Invalid filename: null");
    return String.format("%s/%s", metadataLocation(tableLocation, properties), filename);
  }
}
