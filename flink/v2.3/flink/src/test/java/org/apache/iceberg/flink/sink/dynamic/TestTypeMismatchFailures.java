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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A record whose column type differs from the table's must either be written or fail with a {@link
 * DynamicRecordException}, so that the error stream can divert it. Covers every ordered pair of
 * types, as a column and nested in each kind of container.
 */
class TestTypeMismatchFailures {

  private static final TableIdentifier TABLE = TableIdentifier.of("default", "table");

  private static final int FIRST_NESTED_ID = 10;

  private static final List<TypeCase> TYPES =
      List.of(
          new TypeCase("boolean", id -> Types.BooleanType.get()),
          new TypeCase("int", id -> Types.IntegerType.get()),
          new TypeCase("long", id -> Types.LongType.get()),
          new TypeCase("float", id -> Types.FloatType.get()),
          new TypeCase("double", id -> Types.DoubleType.get()),
          new TypeCase("decimal(9,2)", id -> Types.DecimalType.of(9, 2)),
          new TypeCase("decimal(12,2)", id -> Types.DecimalType.of(12, 2)),
          new TypeCase("decimal(9,4)", id -> Types.DecimalType.of(9, 4)),
          new TypeCase("string", id -> Types.StringType.get()),
          new TypeCase("uuid", id -> Types.UUIDType.get()),
          new TypeCase("date", id -> Types.DateType.get()),
          new TypeCase("time", id -> Types.TimeType.get()),
          new TypeCase("timestamp", id -> Types.TimestampType.withoutZone()),
          new TypeCase("timestamptz", id -> Types.TimestampType.withZone()),
          new TypeCase("timestamp_ns", id -> Types.TimestampNanoType.withoutZone()),
          new TypeCase("fixed(4)", id -> Types.FixedType.ofLength(4)),
          new TypeCase("fixed(8)", id -> Types.FixedType.ofLength(8)),
          new TypeCase("binary", id -> Types.BinaryType.get()),
          new TypeCase("variant", id -> Types.VariantType.get()),
          new TypeCase(
              "struct<int>", id -> Types.StructType.of(optional(id, "a", Types.IntegerType.get()))),
          new TypeCase(
              "struct<string>",
              id -> Types.StructType.of(optional(id, "a", Types.StringType.get()))),
          new TypeCase("list<int>", id -> Types.ListType.ofOptional(id, Types.IntegerType.get())),
          new TypeCase("list<string>", id -> Types.ListType.ofOptional(id, Types.StringType.get())),
          new TypeCase(
              "map<string,int>",
              id ->
                  Types.MapType.ofOptional(
                      id, id + 1, Types.StringType.get(), Types.IntegerType.get())),
          new TypeCase(
              "map<string,string>",
              id ->
                  Types.MapType.ofOptional(
                      id, id + 1, Types.StringType.get(), Types.StringType.get())));

  private static final List<String> NESTED_TYPES =
      List.of("int", "long", "string", "struct<int>", "list<int>", "map<string,int>");

  private static final Map<String, UnaryOperator<Type>> POSITIONS =
      Map.of(
          "struct field", type -> Types.StructType.of(optional(2, "x", type)),
          "list element", type -> Types.ListType.ofOptional(2, type),
          "map value", type -> Types.MapType.ofOptional(2, 3, Types.StringType.get(), type),
          "map key", type -> Types.MapType.ofOptional(2, 3, type, Types.StringType.get()));

  @ParameterizedTest(name = "{0} column in record, {1} in table")
  @MethodSource("columnPairs")
  void columnTypeMismatchFailsOnlyWithRecordException(TypeCase record, TypeCase table) {
    assertWrittenOrRecordException(
        new Schema(optional(1, "a", record.type().apply(2))),
        new Schema(optional(1, "a", table.type().apply(2))));
  }

  @ParameterizedTest(name = "{0} in record, {1} in table, nested as {2}")
  @MethodSource("nestedPairs")
  void nestedTypeMismatchFailsOnlyWithRecordException(
      TypeCase record, TypeCase table, String position) {
    UnaryOperator<Type> nest = POSITIONS.get(position);
    assertWrittenOrRecordException(
        new Schema(optional(1, "a", nest.apply(record.type().apply(FIRST_NESTED_ID)))),
        new Schema(optional(1, "a", nest.apply(table.type().apply(FIRST_NESTED_ID)))));
  }

  private static void assertWrittenOrRecordException(Schema recordSchema, Schema tableSchema) {
    InMemoryCatalog catalog = new InMemoryCatalog();
    catalog.initialize("catalog", Map.of());
    catalog.createNamespace(TABLE.namespace());
    catalog.createTable(
        TABLE,
        tableSchema,
        PartitionSpec.unpartitioned(),
        Map.of(TableProperties.FORMAT_VERSION, "3"));
    TableMetadataCache cache = new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10, true, false);
    TableUpdater updater = new TableUpdater(cache, catalog, true, false);

    Throwable failure =
        catchThrowable(
            () ->
                updater.update(
                    TABLE,
                    SnapshotRef.MAIN_BRANCH,
                    recordSchema,
                    PartitionSpec.unpartitioned(),
                    TableCreator.DEFAULT));

    assertThat(failure)
        .satisfiesAnyOf(
            error -> assertThat(error).isNull(),
            error -> assertThat(error).isInstanceOf(DynamicRecordException.class));
  }

  private static Stream<Arguments> columnPairs() {
    return TYPES.stream()
        .flatMap(record -> TYPES.stream().map(table -> Arguments.of(record, table)));
  }

  private static Stream<Arguments> nestedPairs() {
    List<TypeCase> nested = TYPES.stream().filter(t -> NESTED_TYPES.contains(t.name())).toList();
    return POSITIONS.keySet().stream()
        .sorted()
        .flatMap(
            position ->
                nested.stream()
                    .flatMap(
                        record ->
                            nested.stream().map(table -> Arguments.of(record, table, position))));
  }

  private record TypeCase(String name, IntFunction<Type> type) {
    @Override
    public String toString() {
      return name;
    }
  }
}
