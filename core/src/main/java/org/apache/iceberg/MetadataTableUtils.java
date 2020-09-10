/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;

public class MetadataTableUtils {
  private MetadataTableUtils() {
  }

  public static boolean hasMetadataTableName(TableIdentifier identifier) {
    return MetadataTableType.from(identifier.name()) != null;
  }

  public static Table createMetadataTableInstance(TableOperations originTableOps,
                                                  String fullTableName,
                                                  MetadataTableType type) {
    Table baseTableForMetadata = new BaseTable(originTableOps, fullTableName);
    switch (type) {
      case ENTRIES:
        return new ManifestEntriesTable(originTableOps, baseTableForMetadata);
      case FILES:
        return new DataFilesTable(originTableOps, baseTableForMetadata);
      case HISTORY:
        return new HistoryTable(originTableOps, baseTableForMetadata);
      case SNAPSHOTS:
        return new SnapshotsTable(originTableOps, baseTableForMetadata);
      case MANIFESTS:
        return new ManifestsTable(originTableOps, baseTableForMetadata);
      case PARTITIONS:
        return new PartitionsTable(originTableOps, baseTableForMetadata);
      case ALL_DATA_FILES:
        return new AllDataFilesTable(originTableOps, baseTableForMetadata);
      case ALL_MANIFESTS:
        return new AllManifestsTable(originTableOps, baseTableForMetadata);
      case ALL_ENTRIES:
        return new AllEntriesTable(originTableOps, baseTableForMetadata);
      default:
        throw new NoSuchTableException("Unknown metadata table type: %s for %s", type, fullTableName);
    }
  }

  public static Table createMetadataTableInstance(TableOperations originTableOps,
                                                  TableIdentifier originTableIdentifier,
                                                  MetadataTableType type) {
    return createMetadataTableInstance(originTableOps,
        BaseMetastoreCatalog.fullTableName(type.name(), originTableIdentifier),
        type);
  }
}
