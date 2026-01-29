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

import java.util.Locale;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;

public class MetadataTableUtils {
  private MetadataTableUtils() {}

  public static boolean hasMetadataTableName(TableIdentifier identifier) {
    return MetadataTableType.from(identifier.name()) != null;
  }

  public static Table createMetadataTableInstance(Table table, MetadataTableType type) {
    if (table instanceof BaseTable) {
      return createMetadataTableInstance(table, metadataTableName(table.name(), type), type);
    } else if (table instanceof HasTableOperations) {
      return createMetadataTableInstance(
          ((HasTableOperations) table).operations(),
          table.name(),
          metadataTableName(table.name(), type),
          type);
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Cannot create metadata table for table %s: "
                  + "table is not a base table or does not have table operations",
              table));
    }
  }

  public static Table createMetadataTableInstance(
      TableOperations ops, String baseTableName, String metadataTableName, MetadataTableType type) {
    Table baseTable = new BaseTable(ops, baseTableName);
    return createMetadataTableInstance(baseTable, metadataTableName, type);
  }

  private static Table createMetadataTableInstance(
      Table baseTable, String metadataTableName, MetadataTableType type) {
    return switch (type) {
      case ENTRIES -> new ManifestEntriesTable(baseTable, metadataTableName);
      case FILES -> new FilesTable(baseTable, metadataTableName);
      case DATA_FILES -> new DataFilesTable(baseTable, metadataTableName);
      case DELETE_FILES -> new DeleteFilesTable(baseTable, metadataTableName);
      case HISTORY -> new HistoryTable(baseTable, metadataTableName);
      case SNAPSHOTS -> new SnapshotsTable(baseTable, metadataTableName);
      case METADATA_LOG_ENTRIES -> new MetadataLogEntriesTable(baseTable, metadataTableName);
      case REFS -> new RefsTable(baseTable, metadataTableName);
      case MANIFESTS -> new ManifestsTable(baseTable, metadataTableName);
      case PARTITIONS -> new PartitionsTable(baseTable, metadataTableName);
      case ALL_DATA_FILES -> new AllDataFilesTable(baseTable, metadataTableName);
      case ALL_DELETE_FILES -> new AllDeleteFilesTable(baseTable, metadataTableName);
      case ALL_FILES -> new AllFilesTable(baseTable, metadataTableName);
      case ALL_MANIFESTS -> new AllManifestsTable(baseTable, metadataTableName);
      case ALL_ENTRIES -> new AllEntriesTable(baseTable, metadataTableName);
      case POSITION_DELETES -> new PositionDeletesTable(baseTable, metadataTableName);
      default ->
          throw new NoSuchTableException(
              "Unknown metadata table type: %s for %s", type, metadataTableName);
    };
  }

  public static Table createMetadataTableInstance(
      TableOperations ops,
      String catalogName,
      TableIdentifier baseTableIdentifier,
      TableIdentifier metadataTableIdentifier,
      MetadataTableType type) {
    String baseTableName = BaseMetastoreCatalog.fullTableName(catalogName, baseTableIdentifier);
    String metadataTableName =
        BaseMetastoreCatalog.fullTableName(catalogName, metadataTableIdentifier);
    return createMetadataTableInstance(ops, baseTableName, metadataTableName, type);
  }

  private static String metadataTableName(String tableName, MetadataTableType type) {
    return tableName + (tableName.contains("/") ? "#" : ".") + type.name().toLowerCase(Locale.ROOT);
  }
}
