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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;

public class MetadataTableUtils {
  static final String DATA_SEQUENCE_NUMBER = "data_sequence_number";

  public static final Set<String> DERIVED_FIELDS =
      Sets.newHashSet(MetricsUtil.READABLE_METRICS, DATA_SEQUENCE_NUMBER);

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

  static class StructWithComputedColumns implements StructLike {
    private final StructLike struct;
    private final int projectionColumnCount;
    private final int dataSequenceNumberPosition;
    private final Long dataSequenceNumber;
    private final int metricsPosition;
    private final StructLike readableMetricsStruct;

    private final int[] positionMap;

    StructWithComputedColumns(
        Schema base,
        Schema projection,
        StructLike struct,
        Long dataSequenceNumber,
        StructLike readableMetrics) {
      this.projectionColumnCount = projection.columns().size();
      this.positionMap = new int[this.projectionColumnCount];
      // build projection map
      for (Types.NestedField field : projection.asStruct().fields()) {
        int projectPosition = projection.columns().indexOf(field);
        int basePosition = base.columns().indexOf(base.findField(field.fieldId()));
        Preconditions.checkArgument(
            projectPosition >= 0, "Cannot find %s in projection", field.name());
        Preconditions.checkArgument(basePosition >= 0, "Cannot find %s in base", field.name());
        positionMap[projectPosition] = basePosition;
      }
      this.struct = struct;
      this.dataSequenceNumberPosition =
          projection.columns().indexOf(projection.findField(DATA_SEQUENCE_NUMBER));
      this.dataSequenceNumber = dataSequenceNumber;
      this.metricsPosition =
          projection.columns().indexOf(projection.findField(MetricsUtil.READABLE_METRICS));
      this.readableMetricsStruct = readableMetrics;
    }

    @Override
    public int size() {
      return projectionColumnCount;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      if (pos == dataSequenceNumberPosition) {
        return javaClass.cast(dataSequenceNumber);
      } else if (pos == metricsPosition) {
        return javaClass.cast(readableMetricsStruct);
      } else {
        int structPosition = positionMap[pos];
        return struct.get(structPosition, javaClass);
      }
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("StructWithComputedColumns is read only");
    }
  }
}
