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
package org.apache.iceberg.flink.sink.dynamic;

import java.util.Map;
import org.apache.avro.Schema.Parser;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.VariantType;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.util.Collector;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkCreateTableOptions;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.data.VariantRowDataWrapper;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;

public class VariantAvroDynamicTableRecordGenerator extends DynamicTableRecordGenerator {

  private transient Map<String, Integer> fieldNameToPosition;
  private static final Splitter COMMA = Splitter.on(',');
  private transient int maxCacheSize;
  private transient Map<TableIdentifier, SchemaAndPartitionSpecCacheItem> tableCache;
  @VisibleForTesting static final String DATA_COLUMN = "data";
  @VisibleForTesting static final String AVRO_SCHEMA_COLUMN = "avro_schema";
  @VisibleForTesting static final String AVRO_SCHEMA_ID_COLUMN = "avro_schema_id";
  @VisibleForTesting static final String PARTITION_COLUMNS = "partition_columns";

  public VariantAvroDynamicTableRecordGenerator(
      RowType rowType, Map<String, String> writeProperties) {
    super(rowType, writeProperties);

    String catalogDatabaseColumn = FlinkCreateTableOptions.CATALOG_DATABASE.key();
    Preconditions.checkArgument(
        rowType.getFieldIndex(catalogDatabaseColumn) != -1
            || writeProperties().containsKey(catalogDatabaseColumn),
        "Invalid %s:null.Either %s column should be passed in Row or set in table options",
        catalogDatabaseColumn,
        catalogDatabaseColumn);

    String catalogTableColumn = FlinkCreateTableOptions.CATALOG_TABLE.key();
    Preconditions.checkArgument(
        rowType.getFieldIndex(catalogTableColumn) != -1
            || writeProperties().containsKey(catalogTableColumn),
        "Invalid %s:null.Either %s column should be passed in Row or set in table options",
        catalogTableColumn,
        catalogTableColumn);

    validateRequiredFieldAndType(DATA_COLUMN, new VariantType(false));
    validateRequiredFieldAndType(AVRO_SCHEMA_COLUMN, new VarCharType(false, Integer.MAX_VALUE));
    validateRequiredFieldAndType(AVRO_SCHEMA_ID_COLUMN, new VarCharType(false, Integer.MAX_VALUE));
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);

    this.fieldNameToPosition = fieldNameToPositionMapping();

    String value = writeProperties().get(FlinkWriteOptions.CACHE_MAX_SIZE.key());
    if (value != null) {
      maxCacheSize = Integer.parseInt(value);
    } else {
      maxCacheSize = FlinkWriteOptions.CACHE_MAX_SIZE.defaultValue();
    }

    this.tableCache = new LRUCache<>(maxCacheSize);
  }

  @Override
  public void generate(RowData inputRecord, Collector<DynamicRecord> out) throws Exception {
    String catalogDatabaseColumn = FlinkCreateTableOptions.CATALOG_DATABASE.key();
    String catalogDb =
        stringTypedColumnValue(
            inputRecord, catalogDatabaseColumn, writeProperties().get(catalogDatabaseColumn));

    String catalogTableColumn = FlinkCreateTableOptions.CATALOG_TABLE.key();
    String catalogTable =
        stringTypedColumnValue(
            inputRecord, catalogTableColumn, writeProperties().get(catalogTableColumn));

    // All write options overrides should be inferred in DynamicIcebergSink
    String branch = stringTypedColumnValue(inputRecord, FlinkWriteOptions.BRANCH.key());

    DistributionMode distributionMode = null;
    String distributionModeStr =
        stringTypedColumnValue(inputRecord, FlinkWriteOptions.DISTRIBUTION_MODE.key());
    if (distributionModeStr != null) {
      distributionMode = DistributionMode.fromName(distributionModeStr);
    }

    int writeParallelism = -1;
    Integer pos = fieldNameToPosition.get(FlinkWriteOptions.WRITE_PARALLELISM.key());
    if (pos != null) {
      writeParallelism = inputRecord.getInt(pos);
    }

    Variant variantData = inputRecord.getVariant(fieldNameToPosition.get(DATA_COLUMN));
    String avroSchema = stringTypedColumnValue(inputRecord, AVRO_SCHEMA_COLUMN);
    String avroSchemaId = stringTypedColumnValue(inputRecord, AVRO_SCHEMA_ID_COLUMN);

    TableIdentifier tableIdentifier = TableIdentifier.of(catalogDb, catalogTable);
    SchemaAndPartitionSpecCacheItem cacheItem =
        tableCache.computeIfAbsent(
            tableIdentifier, identifier -> new SchemaAndPartitionSpecCacheItem(maxCacheSize));

    SchemaCacheItem schemaCacheItem = cacheItem.schema(avroSchemaId, avroSchema);

    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
    String partitionCols = stringTypedColumnValue(inputRecord, PARTITION_COLUMNS, null);
    if (partitionCols != null) {
      partitionSpec = cacheItem.partitionSpec(partitionCols, schemaCacheItem.tableSchema());
    }

    out.collect(
        new DynamicRecord(
            tableIdentifier,
            branch,
            schemaCacheItem.tableSchema(),
            schemaCacheItem.variantRowDataWrapper().wrap(variantData),
            partitionSpec,
            distributionMode,
            writeParallelism));
  }

  @VisibleForTesting
  Map<TableIdentifier, SchemaAndPartitionSpecCacheItem> tableCache() {
    return tableCache;
  }

  private String stringTypedColumnValue(RowData rowData, String column, String defaultValue) {
    Integer pos = fieldNameToPosition.get(column);
    if (pos != null) {
      StringData value = rowData.getString(pos);
      return value == null ? defaultValue : value.toString();
    }

    return defaultValue;
  }

  private String stringTypedColumnValue(RowData rowData, String column) {
    return stringTypedColumnValue(rowData, column, null);
  }

  @VisibleForTesting
  static class SchemaAndPartitionSpecCacheItem {
    private final Map<String, SchemaCacheItem> schemaCache;
    private final Map<String, PartitionSpec> partitionSpecCache;

    SchemaAndPartitionSpecCacheItem(int maximumSize) {
      this.schemaCache = new LRUCache<>(maximumSize);
      this.partitionSpecCache = new LRUCache<>(maximumSize);
    }

    SchemaCacheItem schema(String avroSchemaId, String avroSchema) {
      SchemaCacheItem schemaCacheItem = schemaCache.get(avroSchemaId);
      if (schemaCacheItem == null) {
        Schema icebergTableSchema = AvroSchemaUtil.toIceberg(new Parser().parse(avroSchema));
        RowType rowType = FlinkSchemaUtil.convert(icebergTableSchema);
        VariantRowDataWrapper variantRowDataWrapper = new VariantRowDataWrapper(rowType);
        schemaCacheItem = new SchemaCacheItem(icebergTableSchema, variantRowDataWrapper);
        schemaCache.put(avroSchemaId, schemaCacheItem);
      }

      return schemaCacheItem;
    }

    PartitionSpec partitionSpec(String partitionCols, Schema schema) {
      PartitionSpec partitionSpec = partitionSpecCache.get(partitionCols);

      if (partitionSpec == null) {
        PartitionSpec.Builder partitionSpecBuilder = PartitionSpec.builderFor(schema);
        for (String col : COMMA.split(partitionCols)) {
          partitionSpecBuilder.identity(col);
        }

        partitionSpec = partitionSpecBuilder.build();
        partitionSpecCache.put(partitionCols, partitionSpec);
      }

      return partitionSpec;
    }

    @VisibleForTesting
    SchemaCacheItem schemaCacheItem(String avroSchemaId) {
      return schemaCache.get(avroSchemaId);
    }

    @VisibleForTesting
    PartitionSpec partitionSpec(String partitionCols) {
      return partitionSpecCache.get(partitionCols);
    }
  }

  @VisibleForTesting
  record SchemaCacheItem(Schema tableSchema, VariantRowDataWrapper variantRowDataWrapper) {}
}
