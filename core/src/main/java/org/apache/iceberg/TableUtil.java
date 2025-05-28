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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class TableUtil {
  private TableUtil() {}

  /** Returns the format version of the given table */
  public static int formatVersion(Table table) {
    Preconditions.checkArgument(null != table, "Invalid table: null");

    if (table instanceof SerializableTable) {
      SerializableTable serializableTable = (SerializableTable) table;
      return serializableTable.formatVersion();
    } else if (table instanceof HasTableOperations) {
      HasTableOperations ops = (HasTableOperations) table;
      return ops.operations().current().formatVersion();
    } else if (table instanceof BaseMetadataTable) {
      BaseMetadataTable metadataTable = (BaseMetadataTable) table;
      return metadataTable.table().operations().current().formatVersion();
    } else {
      throw new IllegalArgumentException(
          String.format("%s does not have a format version", table.getClass().getSimpleName()));
    }
  }

  /** Returns the metadata file location of the given table */
  public static String metadataFileLocation(Table table) {
    Preconditions.checkArgument(null != table, "Invalid table: null");

    if (table instanceof SerializableTable) {
      SerializableTable serializableTable = (SerializableTable) table;
      return serializableTable.metadataFileLocation();
    } else if (table instanceof HasTableOperations) {
      HasTableOperations ops = (HasTableOperations) table;
      return ops.operations().current().metadataFileLocation();
    } else if (table instanceof BaseMetadataTable) {
      return ((BaseMetadataTable) table).table().operations().current().metadataFileLocation();
    } else {
      throw new IllegalArgumentException(
          String.format(
              "%s does not have a metadata file location", table.getClass().getSimpleName()));
    }
  }

  public static boolean supportsRowLineage(Table table) {
    Preconditions.checkArgument(null != table, "Invalid table: null");
    if (table instanceof BaseMetadataTable) {
      return false;
    }

    return formatVersion(table) >= TableMetadata.MIN_FORMAT_VERSION_ROW_LINEAGE;
  }
}
