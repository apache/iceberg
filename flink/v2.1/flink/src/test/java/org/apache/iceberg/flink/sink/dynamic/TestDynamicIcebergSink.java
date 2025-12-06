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
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.SerializableSupplier;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.flink.sink.TestFlinkIcebergSinkBase;
import org.apache.iceberg.flink.sink.dynamic.TestDynamicCommitter.CommitHook;
import org.apache.iceberg.flink.sink.dynamic.TestDynamicCommitter.FailBeforeAndAfterCommit;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
    public void generate(DynamicIcebergDataImpl row, Collector<DynamicRecord> out) {
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
    ResolvedSchema resolvedSchema = FlinkSchemaUtil.toResolvedSchema(rowType);
    return new DataFormatConverters.RowConverter(
        resolvedSchema.getColumnDataTypes().toArray(DataType[]::new));
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
  void testRowEvolutionMakeMissingRequiredFieldOptional() throws Exception {
    Schema existingSchemaWithRequiredField =
        new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()));

    CATALOG_EXTENSION
        .catalog()
        .createTable(TableIdentifier.of(DATABASE, "t1"), existingSchemaWithRequiredField);

    Schema writeSchemaWithoutRequiredField =
        new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()));

    List<DynamicIcebergDataImpl> rows =
        Lists.newArrayList(
            new DynamicIcebergDataImpl(
                writeSchemaWithoutRequiredField,
                existingSchemaWithRequiredField,
                "t1",
                "main",
                PartitionSpec.unpartitioned()));

    runTest(rows, this.env, 1);
  }

  @Test
  void testSchemaEvolutionNonBackwardsCompatible() throws Exception {
    Schema initialSchema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    // Type change is not allowed
    Schema erroringSchema = new Schema(Types.NestedField.required(1, "id", Types.StringType.get()));

    List<DynamicIcebergDataImpl> rows =
        Lists.newArrayList(
            new DynamicIcebergDataImpl(initialSchema, "t1", "main", PartitionSpec.unpartitioned()),
            new DynamicIcebergDataImpl(
                erroringSchema, "t1", "main", PartitionSpec.unpartitioned()));

    try {
      runTest(rows, StreamExecutionEnvironment.getExecutionEnvironment(), 1);
      fail();
    } catch (JobExecutionException e) {
      assertThat(
              ExceptionUtils.findThrowable(
                  e, t -> t.getMessage().contains("Cannot change column type: id: int -> string")))
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

    executeDynamicSink(rows, env, true, 1, null);

    try (CloseableIterable<Record> iterable =
        IcebergGenerics.read(
                CATALOG_EXTENSION.catalog().loadTable(TableIdentifier.of("default", "t1")))
            .build()) {
      List<Record> records = Lists.newArrayList();
      for (Record record : iterable) {
        records.add(record);
      }

      assertThat(records).hasSize(1);
      Record actual = records.get(0);
      DynamicIcebergDataImpl input = rows.get(0);
      assertThat(actual.get(0)).isEqualTo(input.rowProvided.getField(0));
      assertThat(actual.get(1)).isEqualTo(input.rowProvided.getField(1));
      // There is an additional _pos field which gets added
    }
  }

  @Test
  void testUpsertV3() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.of(TableProperties.FORMAT_VERSION, "3");
    CATALOG_EXTENSION
        .catalog()
        .createTable(
            TableIdentifier.of(DATABASE, "t1"),
            SimpleDataUtil.SCHEMA,
            PartitionSpec.unpartitioned(),
            null,
            properties);

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

    executeDynamicSink(rows, env, true, 1, null);

    try (CloseableIterable<Record> iterable =
        IcebergGenerics.read(
                CATALOG_EXTENSION.catalog().loadTable(TableIdentifier.of("default", "t1")))
            .build()) {
      List<Record> records = Lists.newArrayList();
      for (Record record : iterable) {
        records.add(record);
      }

      assertThat(records).hasSize(1);
      Record actual = records.get(0);
      DynamicIcebergDataImpl input = rows.get(0);
      assertThat(actual.get(0)).isEqualTo(input.rowProvided.getField(0));
      assertThat(actual.get(1)).isEqualTo(input.rowProvided.getField(1));
    }
  }

  @Test
  void testMultiFormatVersion() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.of(TableProperties.FORMAT_VERSION, "3");
    CATALOG_EXTENSION
        .catalog()
        .createTable(
            TableIdentifier.of(DATABASE, "t1"),
            SimpleDataUtil.SCHEMA,
            PartitionSpec.unpartitioned(),
            null,
            properties);

    ImmutableMap<String, String> properties1 = ImmutableMap.of(TableProperties.FORMAT_VERSION, "2");
    CATALOG_EXTENSION
        .catalog()
        .createTable(
            TableIdentifier.of(DATABASE, "t2"),
            SimpleDataUtil.SCHEMA,
            PartitionSpec.unpartitioned(),
            null,
            properties1);

    List<DynamicIcebergDataImpl> rowsForTable1 =
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

    List<DynamicIcebergDataImpl> rowsForTable2 =
        Lists.newArrayList(
            // Insert one rows
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA,
                "t2",
                "main",
                PartitionSpec.unpartitioned(),
                true,
                Sets.newHashSet("id"),
                false),
            // Remaining rows are duplicates
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA,
                "t2",
                "main",
                PartitionSpec.unpartitioned(),
                true,
                Sets.newHashSet("id"),
                true),
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA,
                "t2",
                "main",
                PartitionSpec.unpartitioned(),
                true,
                Sets.newHashSet("id"),
                true),
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA,
                "t2",
                "main",
                PartitionSpec.unpartitioned(),
                true,
                Sets.newHashSet("id"),
                true));

    List<DynamicIcebergDataImpl> rows = Lists.newArrayList();
    rows.addAll(rowsForTable1);
    rows.addAll(rowsForTable2);

    executeDynamicSink(rows, env, true, 1, null);

    try (CloseableIterable<Record> iterable =
        IcebergGenerics.read(
                CATALOG_EXTENSION.catalog().loadTable(TableIdentifier.of("default", "t1")))
            .build()) {
      List<Record> records = Lists.newArrayList();
      for (Record record : iterable) {
        records.add(record);
      }

      assertThat(records).hasSize(1);
      Record actual = records.get(0);
      DynamicIcebergDataImpl input = rowsForTable1.get(0);
      assertThat(actual.get(0)).isEqualTo(input.rowProvided.getField(0));
      assertThat(actual.get(1)).isEqualTo(input.rowProvided.getField(1));
    }

    try (CloseableIterable<Record> iterable =
        IcebergGenerics.read(
                CATALOG_EXTENSION.catalog().loadTable(TableIdentifier.of("default", "t2")))
            .build()) {
      List<Record> records = Lists.newArrayList();
      for (Record record : iterable) {
        records.add(record);
      }

      assertThat(records).hasSize(1);
      Record actual = records.get(0);
      DynamicIcebergDataImpl input = rowsForTable2.get(0);
      assertThat(actual.get(0)).isEqualTo(input.rowProvided.getField(0));
      assertThat(actual.get(1)).isEqualTo(input.rowProvided.getField(1));
    }
  }

  @Test
  void testCommitFailedBeforeOrAfterCommit() throws Exception {
    // Configure a Restart strategy to allow recovery
    Configuration configuration = new Configuration();
    configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
    // Allow max 4 retries to make up for the four failures we are simulating here
    configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 4);
    configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
    env.configure(configuration);

    List<DynamicIcebergDataImpl> rows =
        Lists.newArrayList(
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA, "t1", "main", PartitionSpec.unpartitioned()),
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA, "t2", "main", PartitionSpec.unpartitioned()));

    final CommitHook commitHook = new FailBeforeAndAfterCommit();
    assertThat(FailBeforeAndAfterCommit.failedBeforeCommit).isFalse();
    assertThat(FailBeforeAndAfterCommit.failedBeforeCommitOperation).isFalse();
    assertThat(FailBeforeAndAfterCommit.failedAfterCommitOperation).isFalse();
    assertThat(FailBeforeAndAfterCommit.failedAfterCommit).isFalse();

    executeDynamicSink(rows, env, true, 1, commitHook);

    assertThat(FailBeforeAndAfterCommit.failedBeforeCommit).isTrue();
    assertThat(FailBeforeAndAfterCommit.failedBeforeCommitOperation).isTrue();
    assertThat(FailBeforeAndAfterCommit.failedAfterCommitOperation).isTrue();
    assertThat(FailBeforeAndAfterCommit.failedAfterCommit).isTrue();
  }

  @Test
  void testCommitConcurrency() throws Exception {

    List<DynamicIcebergDataImpl> rows =
        Lists.newArrayList(
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA, "t1", "main", PartitionSpec.unpartitioned()),
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA, "t2", "main", PartitionSpec.unpartitioned()));

    TableIdentifier tableIdentifier = TableIdentifier.of("default", "t1");
    Catalog catalog = CATALOG_EXTENSION.catalog();
    catalog.createTable(tableIdentifier, new Schema());

    final CommitHook commitHook = new AppendRightBeforeCommit(tableIdentifier.toString());

    executeDynamicSink(rows, env, true, 1, commitHook);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testCommitsOnceWhenConcurrentDuplicateCommit(boolean overwriteMode) throws Exception {
    TableIdentifier tableId = TableIdentifier.of(DATABASE, "t1");
    List<DynamicIcebergDataImpl> records =
        Lists.newArrayList(
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA, tableId.name(), "main", PartitionSpec.unpartitioned()),
            new DynamicIcebergDataImpl(
                SimpleDataUtil.SCHEMA, tableId.name(), "main", PartitionSpec.unpartitioned()));

    CommitHook duplicateCommit =
        new DuplicateCommitHook(
            () ->
                new DynamicCommitter(
                    CATALOG_EXTENSION.catalogLoader().loadCatalog(),
                    Collections.emptyMap(),
                    overwriteMode,
                    10,
                    "sinkId",
                    new DynamicCommitterMetrics(new UnregisteredMetricsGroup())));

    executeDynamicSink(records, env, true, 2, duplicateCommit, overwriteMode);

    Table table = CATALOG_EXTENSION.catalog().loadTable(tableId);

    if (!overwriteMode) {
      verifyResults(records);
      assertThat(table.currentSnapshot().summary())
          .containsAllEntriesOf(Map.of("total-records", String.valueOf(records.size())));
    }

    long totalAddedRecords =
        Lists.newArrayList(table.snapshots()).stream()
            .map(snapshot -> snapshot.summary().getOrDefault("added-records", "0"))
            .mapToLong(Long::valueOf)
            .sum();
    assertThat(totalAddedRecords).isEqualTo(records.size());
  }

  @Test
  void testOptInDropUnusedColumns() throws Exception {
    Schema schema1 =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "extra", Types.StringType.get()));

    Schema schema2 =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()));

    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.of(DATABASE, "t1");
    catalog.createTable(tableIdentifier, schema1);

    List<DynamicIcebergDataImpl> rows =
        Lists.newArrayList(
            // Drop columns
            new DynamicIcebergDataImpl(schema2, "t1", "main", PartitionSpec.unpartitioned()),
            // Re-add columns
            new DynamicIcebergDataImpl(schema1, "t1", "main", PartitionSpec.unpartitioned()));

    DataStream<DynamicIcebergDataImpl> dataStream =
        env.fromData(rows, TypeInformation.of(new TypeHint<>() {}));
    env.setParallelism(1);

    DynamicIcebergSink.forInput(dataStream)
        .generator(new Generator())
        .catalogLoader(CATALOG_EXTENSION.catalogLoader())
        .immediateTableUpdate(true)
        .dropUnusedColumns(true)
        .append();

    env.execute("Test Drop Unused Columns");

    Table table = catalog.loadTable(tableIdentifier);
    table.refresh();

    assertThat(table.schema().columns()).hasSize(2);
    assertThat(table.schema().findField("id")).isNotNull();
    assertThat(table.schema().findField("data")).isNotNull();
    assertThat(table.schema().findField("extra")).isNull();

    List<Record> records = Lists.newArrayList(IcebergGenerics.read(table).build());
    assertThat(records).hasSize(2);
  }

  /**
   * Represents a concurrent duplicate commit during an ongoing commit operation, which can happen
   * in production scenarios when using REST catalog.
   */
  static class DuplicateCommitHook implements CommitHook {
    // Static to maintain state after Flink restarts
    private static boolean hasTriggered = false;

    private final SerializableSupplier<DynamicCommitter> duplicateCommitterSupplier;
    private final List<Committer.CommitRequest<DynamicCommittable>> commitRequests;

    DuplicateCommitHook(SerializableSupplier<DynamicCommitter> duplicateCommitterSupplier) {
      this.duplicateCommitterSupplier = duplicateCommitterSupplier;
      this.commitRequests = Lists.newArrayList();

      resetState();
    }

    private static void resetState() {
      hasTriggered = false;
    }

    @Override
    public void beforeCommit(Collection<Committer.CommitRequest<DynamicCommittable>> requests) {
      if (!hasTriggered) {
        this.commitRequests.addAll(requests);
      }
    }

    @Override
    public void beforeCommitOperation() {
      if (!hasTriggered) {
        try {
          duplicateCommitterSupplier.get().commit(commitRequests);
        } catch (final IOException | InterruptedException e) {
          throw new RuntimeException("Duplicate committer failed", e);
        }

        commitRequests.clear();
        hasTriggered = true;
      }
    }
  }

  private static class AppendRightBeforeCommit implements CommitHook {

    final String tableIdentifier;

    private AppendRightBeforeCommit(String tableIdentifier) {
      this.tableIdentifier = tableIdentifier;
    }

    @Override
    public void beforeCommitOperation() {
      // Create a conflict
      Table table = CATALOG_EXTENSION.catalog().loadTable(TableIdentifier.parse(tableIdentifier));
      DataFile dataFile =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withInputFile(new InMemoryInputFile(new byte[] {1, 2, 3}))
              .withFormat(FileFormat.AVRO)
              .withRecordCount(3)
              .build();
      table.newAppend().appendFile(dataFile).commit();
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
    executeDynamicSink(dynamicData, env, immediateUpdate, parallelism, null);
    verifyResults(dynamicData);
  }

  private void executeDynamicSink(
      List<DynamicIcebergDataImpl> dynamicData,
      StreamExecutionEnvironment env,
      boolean immediateUpdate,
      int parallelism,
      @Nullable CommitHook commitHook)
      throws Exception {
    executeDynamicSink(dynamicData, env, immediateUpdate, parallelism, commitHook, false);
  }

  private void executeDynamicSink(
      List<DynamicIcebergDataImpl> dynamicData,
      StreamExecutionEnvironment env,
      boolean immediateUpdate,
      int parallelism,
      @Nullable CommitHook commitHook,
      boolean overwrite)
      throws Exception {
    DataStream<DynamicIcebergDataImpl> dataStream =
        env.fromData(dynamicData, TypeInformation.of(new TypeHint<>() {}));
    env.setParallelism(parallelism);

    if (commitHook != null) {
      new CommitHookEnabledDynamicIcebergSink(commitHook)
          .forInput(dataStream)
          .generator(new Generator())
          .catalogLoader(CATALOG_EXTENSION.catalogLoader())
          .writeParallelism(parallelism)
          .immediateTableUpdate(immediateUpdate)
          .setSnapshotProperty("commit.retry.num-retries", "0")
          .overwrite(overwrite)
          .append();
    } else {
      DynamicIcebergSink.forInput(dataStream)
          .generator(new Generator())
          .catalogLoader(CATALOG_EXTENSION.catalogLoader())
          .writeParallelism(parallelism)
          .immediateTableUpdate(immediateUpdate)
          .overwrite(overwrite)
          .append();
    }

    // Write the data
    env.execute("Test Iceberg DataStream");
  }

  static class CommitHookEnabledDynamicIcebergSink<T> extends DynamicIcebergSink.Builder<T> {
    private final CommitHook commitHook;

    CommitHookEnabledDynamicIcebergSink(CommitHook commitHook) {
      this.commitHook = commitHook;
    }

    @Override
    DynamicIcebergSink instantiateSink(
        Map<String, String> writeProperties, FlinkWriteConf flinkWriteConf) {
      return new CommitHookDynamicIcebergSink(
          commitHook,
          CATALOG_EXTENSION.catalogLoader(),
          Collections.emptyMap(),
          "uidPrefix",
          writeProperties,
          flinkWriteConf,
          100);
    }
  }

  static class CommitHookDynamicIcebergSink extends DynamicIcebergSink {

    private final CommitHook commitHook;
    private final boolean overwriteMode;

    CommitHookDynamicIcebergSink(
        CommitHook commitHook,
        CatalogLoader catalogLoader,
        Map<String, String> snapshotProperties,
        String uidPrefix,
        Map<String, String> writeProperties,
        FlinkWriteConf flinkWriteConf,
        int cacheMaximumSize) {
      super(
          catalogLoader,
          snapshotProperties,
          uidPrefix,
          writeProperties,
          flinkWriteConf,
          cacheMaximumSize);
      this.commitHook = commitHook;
      this.overwriteMode = flinkWriteConf.overwriteMode();
    }

    @Override
    public Committer<DynamicCommittable> createCommitter(CommitterInitContext context) {
      return new TestDynamicCommitter.CommitHookEnabledDynamicCommitter(
          commitHook,
          CATALOG_EXTENSION.catalogLoader().loadCatalog(),
          Collections.emptyMap(),
          overwriteMode,
          10,
          "sinkId",
          new DynamicCommitterMetrics(context.metricGroup()));
    }
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
