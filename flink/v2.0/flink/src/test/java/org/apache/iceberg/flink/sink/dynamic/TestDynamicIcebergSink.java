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

import static org.apache.iceberg.flink.TestFixtures.DATABASE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.legacy.api.TableSchema;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExceptionUtils;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.flink.sink.TestFlinkIcebergSinkBase;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestDynamicIcebergSink extends TestFlinkIcebergSinkBase {

  private static long seed;

  @BeforeEach
  void before() {
    env =
        StreamExecutionEnvironment.getExecutionEnvironment(
                MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG)
            .enableCheckpointing(100)
            .setParallelism(2);
    seed = 0;
  }

  private static class DynamicIcebergDataImpl implements Serializable {
    Row rowProvided;
    Row rowExpected;
    Schema schemaProvided;
    Schema schemaExpected;
    String tableName;
    String branch;
    PartitionSpec partitionSpec;
    boolean upsertMode;
    Set<String> equalityFields;

    private DynamicIcebergDataImpl(
        Schema schemaProvided, String tableName, String branch, PartitionSpec partitionSpec) {
      this(
          schemaProvided,
          schemaProvided,
          tableName,
          branch,
          partitionSpec,
          false,
          Collections.emptySet(),
          false);
    }

    private DynamicIcebergDataImpl(
        Schema schemaProvided,
        Schema schemaExpected,
        String tableName,
        String branch,
        PartitionSpec partitionSpec) {
      this(
          schemaProvided,
          schemaExpected,
          tableName,
          branch,
          partitionSpec,
          false,
          Collections.emptySet(),
          false);
    }

    private DynamicIcebergDataImpl(
        Schema schemaProvided,
        String tableName,
        String branch,
        PartitionSpec partitionSpec,
        boolean upsertMode,
        Set<String> equalityFields,
        boolean isDuplicate) {
      this(
          schemaProvided,
          schemaProvided,
          tableName,
          branch,
          partitionSpec,
          upsertMode,
          equalityFields,
          isDuplicate);
    }

    private DynamicIcebergDataImpl(
        Schema schemaProvided,
        Schema schemaExpected,
        String tableName,
        String branch,
        PartitionSpec partitionSpec,
        boolean upsertMode,
        Set<String> equalityFields,
        boolean isDuplicate) {
      this.rowProvided = randomRow(schemaProvided, isDuplicate ? seed : ++seed);
      this.rowExpected = isDuplicate ? null : rowProvided;
      this.schemaProvided = schemaProvided;
      this.schemaExpected = schemaExpected;
      this.tableName = tableName;
      this.branch = branch;
      this.partitionSpec = partitionSpec;
      this.upsertMode = upsertMode;
      this.equalityFields = equalityFields;
    }
  }

  private static class Generator implements DynamicRecordGenerator<DynamicIcebergDataImpl> {

    @Override
    public void convert(DynamicIcebergDataImpl row, Collector<DynamicRecord> out) {
      TableIdentifier tableIdentifier = TableIdentifier.of(DATABASE, row.tableName);
      String branch = row.branch;
      Schema schema = row.schemaProvided;
      PartitionSpec spec = row.partitionSpec;
      DynamicRecord dynamicRecord =
          new DynamicRecord(
              tableIdentifier,
              branch,
              schema,
              converter(schema).toInternal(row.rowProvided),
              spec,
              spec.isPartitioned() ? DistributionMode.HASH : DistributionMode.NONE,
              10);
      dynamicRecord.setUpsertMode(row.upsertMode);
      dynamicRecord.setEqualityFields(row.equalityFields);
      out.collect(dynamicRecord);
    }
  }

  private static DataFormatConverters.RowConverter converter(Schema schema) {
    RowType rowType = FlinkSchemaUtil.convert(schema);
    TableSchema tableSchema = FlinkSchemaUtil.toSchema(rowType);
    return new DataFormatConverters.RowConverter(tableSchema.getFieldDataTypes());
  }

  @Test
  void testWrite() throws Exception {
    List<DynamicIcebergDataImpl> rows =
        Lists.newArrayList(
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA, "t1", "main", PartitionSpec.unpartitioned()),
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA, "t1", "main", PartitionSpec.unpartitioned()));

    runTest(rows);
  }

  @Test
  void testWritePartitioned() throws Exception {
    PartitionSpec spec = PartitionSpec.builderFor(SimpleDataUtil.SCHEMA).bucket("id", 10).build();

    List<DynamicIcebergDataImpl> rows =
        Lists.newArrayList(
            new DynamicIcebergDataImpl(SimpleDataUtil.SCHEMA, "t1", "main", spec),
            new DynamicIcebergDataImpl(SimpleDataUtil.SCHEMA, "t1", "main", spec),
            new DynamicIcebergDataImpl(SimpleDataUtil.SCHEMA, "t1", "main", spec),
            new DynamicIcebergDataImpl(SimpleDataUtil.SCHEMA, "t1", "main", spec),
            new DynamicIcebergDataImpl(SimpleDataUtil.SCHEMA, "t1", "main", spec));

    runTest(rows);
  }

  @Test
  void testWritePartitionedAdjustSchemaIdsInSpec() throws Exception {
    Schema schema =
        new Schema(
            // Use zero-based schema field ids
            Types.NestedField.required(0, "id", Types.IntegerType.get()),
            Types.NestedField.required(1, "data", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("id", 10).build();
    Schema schema2 =
        new Schema(
            // Use zero-based schema field ids
            Types.NestedField.required(0, "id", Types.IntegerType.get()),
            Types.NestedField.required(1, "data", Types.StringType.get()),
            Types.NestedField.optional(2, "extra", Types.StringType.get()));
    PartitionSpec spec2 = PartitionSpec.builderFor(schema2).bucket("extra", 23).build();

    List<DynamicIcebergDataImpl> rows =
        Lists.newArrayList(
            new DynamicIcebergDataImpl(schema, "t1", "main", spec),
            new DynamicIcebergDataImpl(schema, "t1", "main", spec),
            new DynamicIcebergDataImpl(schema, "t1", "main", spec),
            new DynamicIcebergDataImpl(schema2, "t1", "main", spec2),
            new DynamicIcebergDataImpl(schema2, "t1", "main", spec2));

    runTest(rows);
  }

  @Test
  void testSchemaEvolutionFieldOrderChanges() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.IntegerType.get()),
            Types.NestedField.required(1, "data", Types.StringType.get()));
    Schema expectedSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()));

    Schema schema2 =
        new Schema(
            Types.NestedField.required(0, "id", Types.IntegerType.get()),
            Types.NestedField.required(1, "extra", Types.StringType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()));
    Schema expectedSchema2 =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(3, "extra", Types.StringType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()));

    List<DynamicIcebergDataImpl> rows =
        Lists.newArrayList(
            new DynamicIcebergDataImpl(
                schema, expectedSchema, "t1", "main", PartitionSpec.unpartitioned()),
            new DynamicIcebergDataImpl(
                schema, expectedSchema, "t1", "main", PartitionSpec.unpartitioned()),
            new DynamicIcebergDataImpl(
                schema, expectedSchema, "t1", "main", PartitionSpec.unpartitioned()),
            new DynamicIcebergDataImpl(
                schema2, expectedSchema2, "t1", "main", PartitionSpec.unpartitioned()),
            new DynamicIcebergDataImpl(
                schema2, expectedSchema2, "t1", "main", PartitionSpec.unpartitioned()));

    for (DynamicIcebergDataImpl row : rows) {
      if (row.schemaExpected == expectedSchema) {
        // We manually adjust the expected Row to match the second expected schema
        row.rowExpected = Row.of(row.rowProvided.getField(0), null, row.rowProvided.getField(1));
      }
    }

    runTest(rows);
  }

  @Test
  void testMultipleTables() throws Exception {
    List<DynamicIcebergDataImpl> rows =
        Lists.newArrayList(
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA, "t1", "main", PartitionSpec.unpartitioned()),
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA, "t2", "main", PartitionSpec.unpartitioned()));

    runTest(rows);
  }

  @Test
  void testMultipleTablesPartitioned() throws Exception {
    PartitionSpec spec = PartitionSpec.builderFor(SimpleDataUtil.SCHEMA).bucket("id", 10).build();

    List<DynamicIcebergDataImpl> rows =
        Lists.newArrayList(
            new DynamicIcebergDataImpl(SimpleDataUtil.SCHEMA, "t1", "main", spec),
            new DynamicIcebergDataImpl(SimpleDataUtil.SCHEMA, "t2", "main", spec));

    runTest(rows);
  }

  @Test
  void testSchemaEvolutionAddField() throws Exception {
    List<DynamicIcebergDataImpl> rows =
        Lists.newArrayList(
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA, "t1", "main", PartitionSpec.unpartitioned()),
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA2, "t1", "main", PartitionSpec.unpartitioned()));

    runTest(rows, this.env, 1);
  }

  @Test
  void testRowEvolutionNullMissingOptionalField() throws Exception {
    List<DynamicIcebergDataImpl> rows =
        Lists.newArrayList(
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA2, "t1", "main", PartitionSpec.unpartitioned()),
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA, "t1", "main", PartitionSpec.unpartitioned()));

    runTest(rows, this.env, 1);
  }

  @Test
  void testSchemaEvolutionNonBackwardsCompatible() throws Exception {
    Schema backwardsIncompatibleSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()));
    // Required column is missing in this schema
    Schema erroringSchema =
        new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    List<DynamicIcebergDataImpl> rows =
        Lists.newArrayList(
            new DynamicIcebergDataImpl(
                backwardsIncompatibleSchema, "t1", "main", PartitionSpec.unpartitioned()),
            new DynamicIcebergDataImpl(
                erroringSchema, "t1", "main", PartitionSpec.unpartitioned()));

    try {
      runTest(rows, StreamExecutionEnvironment.getExecutionEnvironment(), 1);
      fail();
    } catch (JobExecutionException e) {
      assertThat(
              ExceptionUtils.findThrowable(
                  e,
                  t ->
                      t.getMessage()
                          .contains(
                              "Field 2 in target schema ROW<`id` INT NOT NULL, `data` STRING NOT NULL> is non-nullable but does not exist in source schema.")))
          .isNotEmpty();
    }
  }

  @Test
  void testPartitionSpecEvolution() throws Exception {
    PartitionSpec spec1 = PartitionSpec.builderFor(SimpleDataUtil.SCHEMA).bucket("id", 10).build();
    PartitionSpec spec2 =
        PartitionSpec.builderFor(SimpleDataUtil.SCHEMA).bucket("id", 5).identity("data").build();

    List<DynamicIcebergDataImpl> rows =
        Lists.newArrayList(
            new DynamicIcebergDataImpl(SimpleDataUtil.SCHEMA, "t1", "main", spec1),
            new DynamicIcebergDataImpl(SimpleDataUtil.SCHEMA, "t1", "main", spec2),
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA, "t1", "main", PartitionSpec.unpartitioned()),
            new DynamicIcebergDataImpl(SimpleDataUtil.SCHEMA, "t1", "main", spec1),
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA, "t1", "main", PartitionSpec.unpartitioned()),
            new DynamicIcebergDataImpl(SimpleDataUtil.SCHEMA, "t1", "main", spec2),
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA, "t1", "main", PartitionSpec.unpartitioned()),
            new DynamicIcebergDataImpl(SimpleDataUtil.SCHEMA, "t1", "main", spec1),
            new DynamicIcebergDataImpl(SimpleDataUtil.SCHEMA, "t1", "main", spec2));

    runTest(rows);
  }

  @Test
  void testMultipleBranches() throws Exception {
    List<DynamicIcebergDataImpl> rows =
        Lists.newArrayList(
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA, "t1", "branch1", PartitionSpec.unpartitioned()),
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA, "t1", "main", PartitionSpec.unpartitioned()));

    runTest(rows);
  }

  @Test
  void testWriteMultipleTablesWithSchemaChanges() throws Exception {
    List<DynamicIcebergDataImpl> rows =
        Lists.newArrayList(
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA, "t1", "main", PartitionSpec.unpartitioned()),
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA, "t2", "main", PartitionSpec.unpartitioned()),
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA, "t1", "main", PartitionSpec.unpartitioned()),
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA2, "t2", "main", PartitionSpec.unpartitioned()),
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA, "t1", "main", PartitionSpec.unpartitioned()),
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA, "t2", "main", PartitionSpec.unpartitioned()),
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA, "t1", "main", PartitionSpec.unpartitioned()),
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA2, "t2", "main", PartitionSpec.unpartitioned()),
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA, "t1", "main", PartitionSpec.unpartitioned()));

    runTest(rows);
  }

  @Test
  void testUpsert() throws Exception {
    List<DynamicIcebergDataImpl> rows =
        Lists.newArrayList(
            // Insert one rows
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA,
                "t1",
                "main",
                PartitionSpec.unpartitioned(),
                true,
                Sets.newHashSet("id"),
                false),
            // Remaining rows are duplicates
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA,
                "t1",
                "main",
                PartitionSpec.unpartitioned(),
                true,
                Sets.newHashSet("id"),
                true),
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA,
                "t1",
                "main",
                PartitionSpec.unpartitioned(),
                true,
                Sets.newHashSet("id"),
                true),
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA,
                "t1",
                "main",
                PartitionSpec.unpartitioned(),
                true,
                Sets.newHashSet("id"),
                true));

    executeDynamicSink(rows, env, true, 1);

    try (CloseableIterable<Record> iterable =
        IcebergGenerics.read(
                CATALOG_EXTENSION.catalog().loadTable(TableIdentifier.of("default", "t1")))
            .build()) {
      List<Record> records = Lists.newArrayList();
      for (Record record : iterable) {
        records.add(record);
      }

      assertThat(records.size()).isEqualTo(1);
      Record actual = records.get(0);
      DynamicIcebergDataImpl input = rows.get(0);
      assertThat(actual.get(0)).isEqualTo(input.rowProvided.getField(0));
      assertThat(actual.get(1)).isEqualTo(input.rowProvided.getField(1));
      // There is an additional _pos field which gets added
    }
  }

  private void runTest(List<DynamicIcebergDataImpl> dynamicData) throws Exception {
    runTest(dynamicData, this.env, 2);
  }

  private void runTest(
      List<DynamicIcebergDataImpl> dynamicData, StreamExecutionEnvironment env, int parallelism)
      throws Exception {
    runTest(dynamicData, env, true, parallelism);
    runTest(dynamicData, env, false, parallelism);
  }

  private void runTest(
      List<DynamicIcebergDataImpl> dynamicData,
      StreamExecutionEnvironment env,
      boolean immediateUpdate,
      int parallelism)
      throws Exception {
    executeDynamicSink(dynamicData, env, immediateUpdate, parallelism);
    verifyResults(dynamicData);
  }

  private void executeDynamicSink(
      List<DynamicIcebergDataImpl> dynamicData,
      StreamExecutionEnvironment env,
      boolean immediateUpdate,
      int parallelism)
      throws Exception {
    DataStream<DynamicIcebergDataImpl> dataStream =
        env.addSource(createBoundedSource(dynamicData), TypeInformation.of(new TypeHint<>() {}));
    env.setParallelism(parallelism);

    DynamicIcebergSink.forInput(dataStream)
        .withGenerator(new Generator())
        .catalogLoader(CATALOG_EXTENSION.catalogLoader())
        .writeParallelism(parallelism)
        .immediateTableUpdate(immediateUpdate)
        .append();

    // Write the data
    env.execute("Test Iceberg DataStream");
  }

  private void verifyResults(List<DynamicIcebergDataImpl> dynamicData) throws IOException {
    // Calculate the expected result
    Map<Tuple2<String, String>, List<RowData>> expectedData = Maps.newHashMap();
    Map<String, Schema> expectedSchema = Maps.newHashMap();
    dynamicData.forEach(
        r -> {
          Schema oldSchema = expectedSchema.get(r.tableName);
          if (oldSchema == null || oldSchema.columns().size() < r.schemaProvided.columns().size()) {
            expectedSchema.put(r.tableName, r.schemaExpected);
          }
        });

    dynamicData.forEach(
        r -> {
          List<RowData> data =
              expectedData.computeIfAbsent(
                  Tuple2.of(r.tableName, r.branch), unused -> Lists.newArrayList());
          data.addAll(
              convertToRowData(expectedSchema.get(r.tableName), ImmutableList.of(r.rowExpected)));
        });

    // Check the expected result
    int count = dynamicData.size();
    for (Map.Entry<Tuple2<String, String>, List<RowData>> e : expectedData.entrySet()) {
      SimpleDataUtil.assertTableRows(
          CATALOG_EXTENSION
              .catalogLoader()
              .loadCatalog()
              .loadTable(TableIdentifier.of(DATABASE, e.getKey().f0)),
          e.getValue(),
          e.getKey().f1);
      count -= e.getValue().size();
    }

    // Found every record
    assertThat(count).isZero();
  }

  private List<RowData> convertToRowData(Schema schema, List<Row> rows) {
    DataFormatConverters.RowConverter converter = converter(schema);
    return rows.stream()
        .map(
            r -> {
              Row updateRow = r;
              // We need conversion to generate the missing columns
              if (r.getArity() != schema.columns().size()) {
                updateRow = new Row(schema.columns().size());
                for (int i = 0; i < r.getArity(); ++i) {
                  updateRow.setField(i, r.getField(i));
                }
              }
              return converter.toInternal(updateRow);
            })
        .collect(Collectors.toList());
  }

  private static Row randomRow(Schema schema, long seedOverride) {
    return TestHelpers.convertRecordToRow(
            RandomGenericData.generate(schema, 1, seedOverride), schema)
        .get(0);
  }
}
