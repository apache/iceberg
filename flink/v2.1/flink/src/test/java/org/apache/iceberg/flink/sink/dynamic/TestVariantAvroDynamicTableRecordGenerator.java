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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaParseException;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.VariantType;
import org.apache.flink.types.variant.Variant;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.DataGenerator;
import org.apache.iceberg.flink.DataGenerators;
import org.apache.iceberg.flink.FlinkCreateTableOptions;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.data.VariantRowDataWrapper;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

class TestVariantAvroDynamicTableRecordGenerator {

  private static final String TEST_DB = "test_db";
  private static final String TEST_TABLE = "test_table";
  private static final String BRANCH = "main";
  private static final Configuration FLINK_CONFIG = new Configuration();

  @Test
  void testMissingRequiredFields() {
    List<LogicalType> types = Lists.newArrayList();
    List<String> names = Lists.newArrayList();

    types.add(new VarCharType(false, 255));
    names.add(FlinkCreateTableOptions.CATALOG_DATABASE.key());
    types.add(new VarCharType(false, 255));
    names.add(FlinkCreateTableOptions.CATALOG_TABLE.key());

    RowType rowType = RowType.of(types.toArray(new LogicalType[0]), names.toArray(new String[0]));

    assertThatThrownBy(
            () ->
                new VariantAvroDynamicTableRecordGenerator(
                    rowType, Collections.emptyMap(), FLINK_CONFIG))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Missing column data. Expected column data of type VARIANT NOT NULL.");
  }

  @Test
  void testNullRequiredColumnsSkipRecord() throws Exception {
    DataGenerator dataGenerator = new DataGenerators.Primitives();
    Variant variantData = dataGenerator.generateFlinkVariantData();
    Schema schema = dataGenerator.avroSchema();
    String schemaId = schema.getName() + ":v1";
    RowType rowType = createRowTypeWithDbAndTable();

    RowData nullData =
        GenericRowData.of(
            null,
            StringData.fromString(schema.toString()),
            StringData.fromString(schemaId),
            StringData.fromString(TEST_DB),
            StringData.fromString(TEST_TABLE));
    assertThat(runGenerate(rowType, Collections.emptyMap(), nullData)).isEmpty();

    RowData nullSchema =
        GenericRowData.of(
            variantData,
            null,
            StringData.fromString(schemaId),
            StringData.fromString(TEST_DB),
            StringData.fromString(TEST_TABLE));
    assertThat(runGenerate(rowType, Collections.emptyMap(), nullSchema)).isEmpty();

    RowData nullSchemaId =
        GenericRowData.of(
            variantData,
            StringData.fromString(schema.toString()),
            null,
            StringData.fromString(TEST_DB),
            StringData.fromString(TEST_TABLE));
    assertThat(runGenerate(rowType, Collections.emptyMap(), nullSchemaId)).isEmpty();
  }

  @Test
  void testMalformedAvroSchemaThrows() {
    DataGenerator dataGenerator = new DataGenerators.Primitives();
    Variant variantData = dataGenerator.generateFlinkVariantData();
    String schemaId = "TestSchema:v1";
    RowType rowType = createRowTypeWithDbAndTable();

    RowData malformed =
        GenericRowData.of(
            variantData,
            StringData.fromString("not-a-valid-avro-schema"),
            StringData.fromString(schemaId),
            StringData.fromString(TEST_DB),
            StringData.fromString(TEST_TABLE));

    assertThatThrownBy(() -> runGenerate(rowType, Collections.emptyMap(), malformed))
        .isInstanceOf(SchemaParseException.class);
  }

  @Test
  void testWithWritePropertiesOverride() throws Exception {
    DataGenerator dataGenerator = new DataGenerators.Primitives();
    Variant variantData = dataGenerator.generateFlinkVariantData();
    Schema schema = dataGenerator.avroSchema();
    String schemaVersion = "v1";

    Map<String, String> writeProperties = Maps.newHashMap();
    writeProperties.put(FlinkCreateTableOptions.CATALOG_DATABASE.key(), "test_db_override");
    writeProperties.put(FlinkCreateTableOptions.CATALOG_TABLE.key(), "test_table_override");

    RowType rowType = createRowTypeWithRequiredFields();
    RowData inputRecord =
        GenericRowData.of(
            variantData,
            StringData.fromString(schema.toString()),
            StringData.fromString(schema.getName() + schemaVersion));

    List<DynamicRecord> records = runGenerate(rowType, writeProperties, inputRecord);

    assertThat(records).hasSize(1);
    DynamicRecord record = records.get(0);
    assertThat(record.tableIdentifier())
        .isEqualTo(TableIdentifier.of("test_db_override", "test_table_override"));
    assertThat(record.branch()).isNull();
    assertThat(record.spec()).isEqualTo(PartitionSpec.unpartitioned());
    assertThat(record.writeParallelism()).isEqualTo(0);
  }

  @Test
  void testNullCatalogColumnsFallBackToWriteProperties() throws Exception {
    DataGenerator dataGenerator = new DataGenerators.Primitives();
    Variant variantData = dataGenerator.generateFlinkVariantData();
    Schema schema = dataGenerator.avroSchema();
    String schemaVersion = "v1";

    Map<String, String> writeProperties = Maps.newHashMap();
    writeProperties.put(FlinkCreateTableOptions.CATALOG_DATABASE.key(), "fallback_db");
    writeProperties.put(FlinkCreateTableOptions.CATALOG_TABLE.key(), "fallback_table");

    RowType rowType = createRowTypeWithDbAndTable();
    RowData inputRecord =
        GenericRowData.of(
            variantData,
            StringData.fromString(schema.toString()),
            StringData.fromString(schema.getName() + schemaVersion),
            null,
            null);

    List<DynamicRecord> records = runGenerate(rowType, writeProperties, inputRecord);

    assertThat(records).hasSize(1);
    DynamicRecord record = records.get(0);
    assertThat(record.tableIdentifier())
        .isEqualTo(TableIdentifier.of("fallback_db", "fallback_table"));
  }

  @Test
  void testBasicFields() throws Exception {
    DataGenerator dataGenerator = new DataGenerators.Primitives();
    Variant variantData = dataGenerator.generateFlinkVariantData();
    Schema schema = dataGenerator.avroSchema();
    String schemaVersion = "v1";

    RowType rowType = createRowTypeWithAllColumns();
    RowData inputRecord =
        GenericRowData.of(
            variantData,
            StringData.fromString(schema.toString()),
            StringData.fromString(schema.getName() + schemaVersion),
            StringData.fromString(TEST_DB),
            StringData.fromString(TEST_TABLE),
            StringData.fromString(BRANCH),
            StringData.fromString("row_id,string_field"),
            1,
            StringData.fromString(DistributionMode.HASH.modeName()));

    List<DynamicRecord> records = runGenerate(rowType, Collections.emptyMap(), inputRecord);

    assertThat(records).hasSize(1);
    DynamicRecord record = records.get(0);
    assertThat(record.rowData()).isInstanceOf(VariantRowDataWrapper.class);
    assertThat(record.tableIdentifier()).isEqualTo(TableIdentifier.of(TEST_DB, TEST_TABLE));
    assertThat(record.branch()).isEqualTo(BRANCH);
    assertThat(record.writeParallelism()).isEqualTo(1);
    assertThat(record.distributionMode()).isEqualTo(DistributionMode.HASH);

    PartitionSpec expectedPartitionSpec =
        PartitionSpec.builderFor(AvroSchemaUtil.toIceberg(schema))
            .identity("row_id")
            .identity("string_field")
            .build();
    assertThat(record.spec()).isEqualTo(expectedPartitionSpec);
  }

  @Test
  void testSchemaCaching() throws Exception {
    DataGenerator dataGenerator1 = new DataGenerators.Primitives();
    DataGenerator dataGenerator2 = new DataGenerators.StructOfPrimitive();

    Schema schema1 = AvroSchemaUtil.convert(dataGenerator1.icebergSchema(), "TestSchema1");
    Schema schema2 = AvroSchemaUtil.convert(dataGenerator2.icebergSchema(), "TestSchema2");
    String schemaId1 = schema1.getName() + ":1";
    String schemaId2 = schema2.getName() + ":1";

    RowType rowType = createRowTypeWithDbAndTable();
    VariantAvroDynamicTableRecordGenerator generator =
        new VariantAvroDynamicTableRecordGenerator(rowType, Collections.emptyMap(), FLINK_CONFIG);
    generator.open(new DefaultOpenContext());

    List<DynamicRecord> records = Lists.newArrayList();
    ListCollector<DynamicRecord> collector = new ListCollector<>(records);

    // Generate first record with schema1
    RowData inputRecord1 = inputRow(dataGenerator1.generateFlinkVariantData(), schema1, schemaId1);
    generator.generate(inputRecord1, collector);

    // Verify cache has been populated with first table and schema
    VariantAvroDynamicTableRecordGenerator.SchemaAndPartitionSpecCacheItem cacheItem =
        generator.tableCache().get(TableIdentifier.of(TEST_DB, TEST_TABLE));
    assertThat(cacheItem).isNotNull();
    VariantAvroDynamicTableRecordGenerator.SchemaCacheItem schemaCacheItem =
        cacheItem.schemaCacheItem(schemaId1);
    assertThat(schemaCacheItem).isNotNull();
    assertThat(schemaCacheItem.tableSchema().sameSchema(dataGenerator1.icebergSchema())).isTrue();

    // Generate second record with same schema1 (should use cached schema)
    RowData inputRecord1Repeat =
        inputRow(dataGenerator1.generateFlinkVariantData(), schema1, schemaId1);
    generator.generate(inputRecord1Repeat, collector);

    // Generate third record with different schema2 and different table
    RowData inputRecord2 =
        inputRow(
            dataGenerator2.generateFlinkVariantData(), schema2, schemaId2, TEST_DB, "test_table2");
    generator.generate(inputRecord2, collector);

    // Verify we now have two table cache items
    assertThat(generator.tableCache()).hasSize(2);
    assertThat(generator.tableCache()).containsKey(TableIdentifier.of(TEST_DB, TEST_TABLE));
    assertThat(generator.tableCache()).containsKey(TableIdentifier.of(TEST_DB, "test_table2"));

    // Verify second table cache item has different schema
    VariantAvroDynamicTableRecordGenerator.SchemaAndPartitionSpecCacheItem cacheItem2 =
        generator.tableCache().get(TableIdentifier.of(TEST_DB, "test_table2"));
    assertThat(cacheItem2).isNotNull();
    assertThat(
            cacheItem2
                .schemaCacheItem(schemaId2)
                .tableSchema()
                .sameSchema(dataGenerator2.icebergSchema()))
        .isTrue();

    assertThat(records).hasSize(3);
    // Should emit DynamicRecord with same tableSchema instance.
    assertThat(records.get(1).schema()).isSameAs(schemaCacheItem.tableSchema());
  }

  @Test
  void testSchemaEvolution() throws Exception {
    // Create simple schema with one field
    Schema initialSchema =
        SchemaBuilder.builder()
            .record("UserRecord")
            .namespace("test.schema")
            .fields()
            .requiredLong("id")
            .endRecord();
    String initialSchemaId = "UserRecord:v1";

    // Create evolved schema with additional field
    Schema evolvedSchema =
        SchemaBuilder.builder()
            .record("UserRecord")
            .namespace("test.schema")
            .fields()
            .requiredLong("id")
            .optionalString("name")
            .endRecord();
    String evolvedSchemaId = "UserRecord:v2";

    RowType rowType = createRowTypeWithDbAndTable();
    VariantAvroDynamicTableRecordGenerator generator =
        new VariantAvroDynamicTableRecordGenerator(rowType, Collections.emptyMap(), FLINK_CONFIG);
    generator.open(new DefaultOpenContext());

    List<DynamicRecord> records = Lists.newArrayList();
    ListCollector<DynamicRecord> collector = new ListCollector<>(records);

    // Create variant data for initial schema (only id field)
    Variant initialVariantData =
        Variant.newBuilder().object().add("id", Variant.newBuilder().of(123L)).build();

    // Generate record with initial schema
    RowData inputRecord1 = inputRow(initialVariantData, initialSchema, initialSchemaId);
    generator.generate(inputRecord1, collector);

    // Create variant data for evolved schema (id and name fields)
    Variant evolvedVariantData =
        Variant.newBuilder()
            .object()
            .add("id", Variant.newBuilder().of(456L))
            .add("name", Variant.newBuilder().of("name"))
            .build();

    // Generate record with evolved schema
    RowData inputRecord2 = inputRow(evolvedVariantData, evolvedSchema, evolvedSchemaId);
    generator.generate(inputRecord2, collector);

    assertThat(records).hasSize(2);

    // Verify all records were generated successfully
    DynamicRecord record1 = records.get(0);
    DynamicRecord record2 = records.get(1);

    assertThat(record1.rowData().getLong(0)).isEqualTo(123L);
    assertThat(record2.rowData().getLong(0)).isEqualTo(456L);
    assertThat(record2.rowData().getString(1).toString()).isEqualTo("name");

    VariantAvroDynamicTableRecordGenerator.SchemaAndPartitionSpecCacheItem cacheItem =
        generator.tableCache().get(TableIdentifier.of(TEST_DB, TEST_TABLE));
    assertThat(cacheItem).isNotNull();
    VariantAvroDynamicTableRecordGenerator.SchemaCacheItem schemaCacheItem =
        cacheItem.schemaCacheItem(initialSchemaId);
    assertThat(schemaCacheItem.tableSchema().sameSchema(AvroSchemaUtil.toIceberg(initialSchema)))
        .isTrue();

    schemaCacheItem = cacheItem.schemaCacheItem(evolvedSchemaId);
    assertThat(schemaCacheItem.tableSchema().sameSchema(AvroSchemaUtil.toIceberg(evolvedSchema)))
        .isTrue();
  }

  @Test
  void testPartitionSpecCaching() throws Exception {
    DataGenerator dataGenerator = new DataGenerators.Primitives();
    Variant variantData = dataGenerator.generateFlinkVariantData();
    Schema avroSchema = dataGenerator.avroSchema();
    String schemaId = "TestSchema:1";

    RowType rowType = createRowTypeWithAllColumns();
    VariantAvroDynamicTableRecordGenerator generator =
        new VariantAvroDynamicTableRecordGenerator(rowType, Collections.emptyMap(), FLINK_CONFIG);
    generator.open(new DefaultOpenContext());

    List<DynamicRecord> records = Lists.newArrayList();
    ListCollector<DynamicRecord> collector = new ListCollector<>(records);

    // Generate first record with partition on "row_id"
    RowData inputRecord1 = inputRow(variantData, avroSchema, schemaId, BRANCH, "row_id", 1);
    generator.generate(inputRecord1, collector);

    // Generate second record with same partition spec (should use cached partition spec)
    RowData inputRecord2 = inputRow(variantData, avroSchema, schemaId, BRANCH, "row_id", 1);
    generator.generate(inputRecord2, collector);

    // Generate third record with different partition spec
    RowData inputRecord3 = inputRow(variantData, avroSchema, schemaId, BRANCH, "string_field", 1);
    generator.generate(inputRecord3, collector);

    assertThat(records).hasSize(3);

    DynamicRecord record1 = records.get(0);
    DynamicRecord record2 = records.get(1);
    DynamicRecord record3 = records.get(2);

    // Verify first two records have same partition spec (partitioned on row_id)
    assertThat(record1.spec().fields()).hasSize(1);
    assertThat(record2.spec().fields()).hasSize(1);
    assertThat(record1.spec().fields().get(0).name()).isEqualTo("row_id");
    assertThat(record2.spec().fields().get(0).name()).isEqualTo("row_id");
    assertThat(record1.spec()).isSameAs(record2.spec());

    // Verify third record has different partition spec (partitioned on string_field)
    assertThat(record3.spec().fields()).hasSize(1);
    assertThat(record3.spec().fields().get(0).name()).isEqualTo("string_field");

    VariantAvroDynamicTableRecordGenerator.SchemaAndPartitionSpecCacheItem cacheItem =
        generator.tableCache().get(TableIdentifier.of(TEST_DB, TEST_TABLE));
    assertThat(cacheItem.partitionSpec("row_id")).isNotNull();
    assertThat(cacheItem.partitionSpec("string_field")).isNotNull();
  }

  private static List<DynamicRecord> runGenerate(
      RowType rowType, Map<String, String> writeProperties, RowData inputRow) throws Exception {
    VariantAvroDynamicTableRecordGenerator generator =
        new VariantAvroDynamicTableRecordGenerator(rowType, writeProperties, FLINK_CONFIG);
    generator.open(new DefaultOpenContext());
    List<DynamicRecord> records = Lists.newArrayList();
    generator.generate(inputRow, new ListCollector<>(records));
    return records;
  }

  private static RowData inputRow(Variant data, Schema schema, String schemaId) {
    return inputRow(data, schema, schemaId, TEST_DB, TEST_TABLE);
  }

  private static RowData inputRow(
      Variant data, Schema schema, String schemaId, String database, String table) {
    return GenericRowData.of(
        data,
        StringData.fromString(schema.toString()),
        StringData.fromString(schemaId),
        StringData.fromString(database),
        StringData.fromString(table));
  }

  private static RowData inputRow(
      Variant data,
      Schema schema,
      String schemaId,
      String branch,
      String partitionColumns,
      int parallelism) {
    return GenericRowData.of(
        data,
        StringData.fromString(schema.toString()),
        StringData.fromString(schemaId),
        StringData.fromString(TEST_DB),
        StringData.fromString(TEST_TABLE),
        StringData.fromString(branch),
        StringData.fromString(partitionColumns),
        parallelism,
        StringData.fromString(DistributionMode.NONE.modeName()));
  }

  private static void addRequiredFields(List<LogicalType> types, List<String> names) {
    types.add(new VariantType(false));
    names.add(VariantAvroDynamicTableRecordGenerator.DATA_COLUMN);
    types.add(new VarCharType(false, Integer.MAX_VALUE));
    names.add(VariantAvroDynamicTableRecordGenerator.AVRO_SCHEMA_COLUMN);
    types.add(new VarCharType(false, 255));
    names.add(VariantAvroDynamicTableRecordGenerator.AVRO_SCHEMA_ID_COLUMN);
  }

  private static RowType createRowTypeWithRequiredFields() {
    List<LogicalType> types = Lists.newArrayList();
    List<String> names = Lists.newArrayList();

    addRequiredFields(types, names);

    return RowType.of(types.toArray(new LogicalType[0]), names.toArray(new String[0]));
  }

  private static RowType createRowTypeWithDbAndTable() {
    List<LogicalType> types = Lists.newArrayList();
    List<String> names = Lists.newArrayList();

    addRequiredFields(types, names);

    types.add(new VarCharType(false, 255));
    names.add(FlinkCreateTableOptions.CATALOG_DATABASE.key());

    types.add(new VarCharType(false, 255));
    names.add(FlinkCreateTableOptions.CATALOG_TABLE.key());

    return RowType.of(types.toArray(new LogicalType[0]), names.toArray(new String[0]));
  }

  private static RowType createRowTypeWithAllColumns() {
    List<LogicalType> types = Lists.newArrayList();
    List<String> names = Lists.newArrayList();

    addRequiredFields(types, names);

    types.add(new VarCharType(false, 255));
    names.add(FlinkCreateTableOptions.CATALOG_DATABASE.key());

    types.add(new VarCharType(false, 255));
    names.add(FlinkCreateTableOptions.CATALOG_TABLE.key());

    types.add(new VarCharType(false, 255));
    names.add(FlinkWriteOptions.BRANCH.key());

    types.add(new VarCharType(false, 255));
    names.add(VariantAvroDynamicTableRecordGenerator.PARTITION_COLUMNS);

    types.add(new IntType(false));
    names.add(FlinkWriteOptions.WRITE_PARALLELISM.key());

    types.add(new VarCharType(false, 255));
    names.add(FlinkWriteOptions.DISTRIBUTION_MODE.key());

    return RowType.of(types.toArray(new LogicalType[0]), names.toArray(new String[0]));
  }
}
