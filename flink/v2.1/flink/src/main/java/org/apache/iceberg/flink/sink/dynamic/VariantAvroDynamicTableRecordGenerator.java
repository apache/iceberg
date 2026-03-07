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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;

public class VariantAvroDynamicTableRecordGenerator extends DynamicTableRecordGenerator {
  private transient Map<String, Integer> fieldNameToPosition;
  private transient Map<TableIdentifier, SchemaAndPartitionSpecCacheItem> tableCache;
  private static final Parser PARSER = new Parser();
  private static final String DEFAULT_CACHE_MAX_SIZE = "100";
  private static final Splitter COMMA = Splitter.on(',');
  private transient int maxCacheSize;

  public VariantAvroDynamicTableRecordGenerator(RowType rowType, Map<String, String> writeProps) {
    super(rowType, writeProps);

    String catalogDatabaseColumn = FlinkCreateTableOptions.CATALOG_DATABASE.key();
    Preconditions.checkArgument(
        rowType.getFieldIndex(catalogDatabaseColumn) != -1
            || writeProps().containsKey(catalogDatabaseColumn),
        "Invalid %s:null." + "Either %s column should be passed in Row or set in table options",
        catalogDatabaseColumn,
        catalogDatabaseColumn);

    String catalogTableColumn = FlinkCreateTableOptions.CATALOG_TABLE.key();
    Preconditions.checkArgument(
        rowType.getFieldIndex(catalogTableColumn) != -1
            || writeProps().containsKey(catalogTableColumn),
        "Invalid %s:null." + "Either %s column should be passed in Row or set in table options",
        catalogTableColumn,
        catalogTableColumn);

    validateRequiredFieldAndType("data", new VariantType(false));
    validateRequiredFieldAndType("avro_schema", new VarCharType(false, Integer.MAX_VALUE));
    validateRequiredFieldAndType("avro_schema_id", new VarCharType(false, Integer.MAX_VALUE));
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);

    this.fieldNameToPosition = getFieldPositionIndex();

    this.maxCacheSize =
        Integer.parseInt(
            writeProps().getOrDefault("schema-cache-max-size", DEFAULT_CACHE_MAX_SIZE));
    this.tableCache = new LRUCache<>(maxCacheSize);
  }

  @Override
  public void generate(RowData inputRecord, Collector<DynamicRecord> out) throws Exception {
    String catalogDatabaseColumn = FlinkCreateTableOptions.CATALOG_DATABASE.key();
    String catalogDb =
        getStringColumnValue(
            inputRecord, catalogDatabaseColumn, writeProps().get(catalogDatabaseColumn));

    String catalogTableColumn = FlinkCreateTableOptions.CATALOG_TABLE.key();
    String catalogTable =
        getStringColumnValue(inputRecord, catalogTableColumn, writeProps().get(catalogTableColumn));

    // All write options overrides should be inferred in DynamicIcebergSink
    String branch = getStringColumnValue(inputRecord, FlinkWriteOptions.BRANCH.key());

    DistributionMode distributionMode = null;
    String distributionModeStr =
        getStringColumnValue(inputRecord, FlinkWriteOptions.DISTRIBUTION_MODE.key());
    if (distributionModeStr != null) {
      distributionMode = DistributionMode.fromName(distributionModeStr);
    }

    int writeParallelism = -1;
    Integer pos = fieldNameToPosition.get(FlinkWriteOptions.WRITE_PARALLELISM.key());
    if (pos != null) {
      writeParallelism = inputRecord.getInt(pos);
    }

    Variant variantData = inputRecord.getVariant(fieldNameToPosition.get("data"));
    String avroSchema = getStringColumnValue(inputRecord, "avro_schema");
    String avroSchemaId = getStringColumnValue(inputRecord, "avro_schema_id");

    TableIdentifier tableIdentifier = TableIdentifier.of(catalogDb, catalogTable);
    SchemaAndPartitionSpecCacheItem cacheItem =
        tableCache.computeIfAbsent(
            tableIdentifier, identifier -> new SchemaAndPartitionSpecCacheItem(maxCacheSize));

    SchemaCacheItem schemaCacheItem = cacheItem.getOrCreateSchema(avroSchemaId, avroSchema);

    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
    String partitionCols = getStringColumnValue(inputRecord, "partition_cols", null);
    if (partitionCols != null) {
      partitionSpec =
          cacheItem.getOrCreatePartitionSpec(partitionCols, schemaCacheItem.tableSchema());
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

  private String getStringColumnValue(RowData rowData, String col, String defaultValue) {
    Integer pos = fieldNameToPosition.get(col);
    if (pos != null) {
      StringData value = rowData.getString(pos);
      return value == null ? defaultValue : value.toString();
    }

    return defaultValue;
  }

  private String getStringColumnValue(RowData rowData, String col) {
    return getStringColumnValue(rowData, col, null);
  }

  private static class SchemaAndPartitionSpecCacheItem {
    private final Map<String, SchemaCacheItem> schemaCache;
    private final Map<String, PartitionSpec> partitionSpecCache;

    SchemaAndPartitionSpecCacheItem(int maximumSize) {
      this.schemaCache = new LRUCache<>(maximumSize);
      this.partitionSpecCache = new LRUCache<>(maximumSize);
    }

    SchemaCacheItem getOrCreateSchema(String avroSchemaId, String avroSchema) {
      SchemaCacheItem schemaCacheItem = schemaCache.get(avroSchemaId);
      if (schemaCacheItem == null) {
        Schema icebergTableSchema = AvroSchemaUtil.toIceberg(PARSER.parse(avroSchema));
        RowType rowType = FlinkSchemaUtil.convert(icebergTableSchema);
        VariantRowDataWrapper variantRowDataWrapper = new VariantRowDataWrapper(rowType);
        schemaCacheItem = new SchemaCacheItem(icebergTableSchema, variantRowDataWrapper);
        schemaCache.put(avroSchemaId, schemaCacheItem);
      }

      return schemaCacheItem;
    }

    PartitionSpec getOrCreatePartitionSpec(String partitionCols, Schema schema) {
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
  }

  private record SchemaCacheItem(Schema tableSchema, VariantRowDataWrapper variantRowDataWrapper) {}
}
