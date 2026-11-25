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
import java.util.Set;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

public class MetadataTableUtils {
  private static final int MIN_FORMAT_VERSION_WITH_ROOT_MANIFESTS = 4;
  private static final Set<MetadataTableType> MANIFEST_INDEPENDENT_TABLES =
      Sets.immutableEnumSet(
          MetadataTableType.HISTORY,
          MetadataTableType.SNAPSHOTS,
          MetadataTableType.METADATA_LOG_ENTRIES,
          MetadataTableType.REFS);

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
    if (!MANIFEST_INDEPENDENT_TABLES.contains(type)) {
      TableMetadata metadata = ((BaseTable) baseTable).operations().current();
      if (metadata.formatVersion() >= MIN_FORMAT_VERSION_WITH_ROOT_MANIFESTS
          && metadata.snapshots().stream()
              .anyMatch(snapshot -> snapshot.rootManifestLocation() != null)) {
        throw new UnsupportedOperationException(
            String.format(
                "Metadata tables are not supported for tables with v4 root manifests: %s",
                metadataTableName));
      }
    }

    switch (type) {
      case ENTRIES:
        return new ManifestEntriesTable(baseTable, metadataTableName);
      case FILES:
        return new FilesTable(baseTable, metadataTableName);
      case DATA_FILES:
        return new DataFilesTable(baseTable, metadataTableName);
      case DELETE_FILES:
        return new DeleteFilesTable(baseTable, metadataTableName);
      case HISTORY:
        return new HistoryTable(baseTable, metadataTableName);
      case SNAPSHOTS:
        return new SnapshotsTable(baseTable, metadataTableName);
      case METADATA_LOG_ENTRIES:
        return new MetadataLogEntriesTable(baseTable, metadataTableName);
      case REFS:
        return new RefsTable(baseTable, metadataTableName);
      case MANIFESTS:
        return new ManifestsTable(baseTable, metadataTableName);
      case PARTITIONS:
        return new PartitionsTable(baseTable, metadataTableName);
      case ALL_DATA_FILES:
        return new AllDataFilesTable(baseTable, metadataTableName);
      case ALL_DELETE_FILES:
        return new AllDeleteFilesTable(baseTable, metadataTableName);
      case ALL_FILES:
        return new AllFilesTable(baseTable, metadataTableName);
      case ALL_MANIFESTS:
        return new AllManifestsTable(baseTable, metadataTableName);
      case ALL_ENTRIES:
        return new AllEntriesTable(baseTable, metadataTableName);
      case POSITION_DELETES:
        return new PositionDeletesTable(baseTable, metadataTableName);
      default:
        throw new NoSuchTableException(
            "Unknown metadata table type: %s for %s", type, metadataTableName);
    }
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
