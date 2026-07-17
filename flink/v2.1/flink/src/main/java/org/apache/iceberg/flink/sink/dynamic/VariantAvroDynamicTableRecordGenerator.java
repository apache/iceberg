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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
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

  @VisibleForTesting static final String DATA_COLUMN = "data";
  @VisibleForTesting static final String AVRO_SCHEMA_COLUMN = "avro_schema";
  @VisibleForTesting static final String AVRO_SCHEMA_ID_COLUMN = "avro_schema_id";
  @VisibleForTesting static final String PARTITION_COLUMNS = "partition_columns";

  private static final Splitter COMMA = Splitter.on(',').trimResults().omitEmptyStrings();

  private final Map<String, String> writeProperties;
  private transient int maxCacheSize;
  private transient Map<TableIdentifier, SchemaAndPartitionSpecCacheItem> tableCache;

  public VariantAvroDynamicTableRecordGenerator(
      RowType rowType, Map<String, String> writeProperties, Configuration flinkConfiguration) {
    super(rowType, writeProperties, flinkConfiguration);
    this.writeProperties = writeProperties;

    validateColumnWithConfigFallback(rowType, FlinkCreateTableOptions.CATALOG_DATABASE);
    validateColumnWithConfigFallback(rowType, FlinkCreateTableOptions.CATALOG_TABLE);

    validateRequiredColumnAndType(DATA_COLUMN, new VariantType(false));
    validateRequiredColumnAndType(AVRO_SCHEMA_COLUMN, VarCharType.STRING_TYPE);
    validateRequiredColumnAndType(AVRO_SCHEMA_ID_COLUMN, VarCharType.STRING_TYPE);
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);

    this.maxCacheSize = flinkDynamicSinkConf().cacheMaxSize();
    this.tableCache = new LRUCache<>(maxCacheSize);
  }

  @Override
  public void generate(RowData inputRecord, Collector<DynamicRecord> out) throws Exception {
    Variant variantData = inputRecord.getVariant(fieldNameToPosition().get(DATA_COLUMN));
    String avroSchema = columnValueAsString(inputRecord, AVRO_SCHEMA_COLUMN);
    String avroSchemaId = columnValueAsString(inputRecord, AVRO_SCHEMA_ID_COLUMN);

    if (variantData == null || avroSchema == null || avroSchemaId == null) {
      return;
    }

    TableIdentifier tableIdentifier = extractTableIdentifier(inputRecord);
    String branch = columnValueAsString(inputRecord, FlinkWriteOptions.BRANCH.key());
    String distributionModeName =
        columnValueWithConfigFallback(inputRecord, FlinkWriteOptions.DISTRIBUTION_MODE);
    DistributionMode distributionMode =
        distributionModeName != null ? DistributionMode.fromName(distributionModeName) : null;

    int writeParallelism = 0;
    Integer position = fieldNameToPosition().get(FlinkWriteOptions.WRITE_PARALLELISM.key());
    if (position != null && !inputRecord.isNullAt(position)) {
      writeParallelism = inputRecord.getInt(position);
    }

    SchemaAndPartitionSpecCacheItem cacheItem =
        tableCache.computeIfAbsent(
            tableIdentifier, identifier -> new SchemaAndPartitionSpecCacheItem(maxCacheSize));

    SchemaCacheItem schemaCacheItem = cacheItem.schemaItem(avroSchemaId, avroSchema);

    String partitionColumns = columnValueAsString(inputRecord, PARTITION_COLUMNS);
    PartitionSpec partitionSpec =
        partitionColumns != null
            ? cacheItem.partitionSpec(partitionColumns, schemaCacheItem.tableSchema())
            : PartitionSpec.unpartitioned();

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

  private String columnValueAsString(RowData rowData, String columnName) {
    return columnValueAsString(rowData, columnName, null);
  }

  private String columnValueAsString(RowData rowData, String columnName, String defaultValue) {
    Integer position = fieldNameToPosition().get(columnName);
    if (position != null) {
      StringData value = rowData.getString(position);
      return value == null ? defaultValue : value.toString();
    }

    return defaultValue;
  }

  private String columnValueWithConfigFallback(RowData rowData, ConfigOption<String> config) {
    return columnValueAsString(rowData, config.key(), writeProperties.get(config.key()));
  }

  private void validateColumnWithConfigFallback(RowType rowType, ConfigOption<String> column) {
    String columnName = column.key();
    Preconditions.checkArgument(
        rowType.getFieldIndex(columnName) != -1 || writeProperties.containsKey(columnName),
        "Invalid %s: null. Either pass the column in Row or set it in table options.",
        columnName);
  }

  private TableIdentifier extractTableIdentifier(RowData inputRecord) {
    String catalogDb =
        columnValueWithConfigFallback(inputRecord, FlinkCreateTableOptions.CATALOG_DATABASE);
    Preconditions.checkNotNull(
        catalogDb,
        "Invalid %s: null. Either pass the value in Row or set it in table options.",
        FlinkCreateTableOptions.CATALOG_DATABASE.key());

    String catalogTable =
        columnValueWithConfigFallback(inputRecord, FlinkCreateTableOptions.CATALOG_TABLE);
    Preconditions.checkNotNull(
        catalogTable,
        "Invalid %s: null. Either pass the value in Row or set it in table options.",
        FlinkCreateTableOptions.CATALOG_TABLE.key());

    return TableIdentifier.of(catalogDb, catalogTable);
  }

  @VisibleForTesting
  static class SchemaAndPartitionSpecCacheItem {
    private final Map<String, SchemaCacheItem> schemaCache;
    private final Map<String, PartitionSpec> partitionSpecCache;

    SchemaAndPartitionSpecCacheItem(int maximumSize) {
      this.schemaCache = new LRUCache<>(maximumSize);
      this.partitionSpecCache = new LRUCache<>(maximumSize);
    }

    private SchemaCacheItem schemaItem(String avroSchemaId, String avroSchema) {
      return schemaCache.computeIfAbsent(
          avroSchemaId,
          key -> {
            Schema icebergTableSchema = AvroSchemaUtil.toIceberg(new Parser().parse(avroSchema));
            RowType rowType = FlinkSchemaUtil.convert(icebergTableSchema);
            return new SchemaCacheItem(icebergTableSchema, new VariantRowDataWrapper(rowType));
          });
    }

    private PartitionSpec partitionSpec(String partitionColumns, Schema schema) {
      return partitionSpecCache.computeIfAbsent(
          partitionColumns,
          key -> {
            PartitionSpec.Builder partitionSpecBuilder = PartitionSpec.builderFor(schema);
            for (String column : COMMA.split(key)) {
              partitionSpecBuilder.identity(column);
            }
            return partitionSpecBuilder.build();
          });
    }

    @VisibleForTesting
    SchemaCacheItem schemaCacheItem(String avroSchemaId) {
      return schemaCache.get(avroSchemaId);
    }

    @VisibleForTesting
    PartitionSpec partitionSpec(String partitionColumns) {
      return partitionSpecCache.get(partitionColumns);
    }
  }

  @VisibleForTesting
  record SchemaCacheItem(Schema tableSchema, VariantRowDataWrapper variantRowDataWrapper) {}
}
