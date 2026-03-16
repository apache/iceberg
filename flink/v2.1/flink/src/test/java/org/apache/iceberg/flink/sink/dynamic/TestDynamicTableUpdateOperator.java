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
import static org.apache.iceberg.flink.TestFixtures.TABLE;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import org.apache.flink.table.data.GenericRowData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestDynamicTableUpdateOperator {

  private static final boolean CASE_SENSITIVE = true;
  private static final boolean CASE_INSENSITIVE = false;

  private static final boolean DROP_COLUMNS = true;
  private static final boolean PRESERVE_COLUMNS = false;

  @RegisterExtension
  private static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(DATABASE, TABLE);

  private static final Schema SCHEMA1 =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  private static final Schema SCHEMA2 =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @Test
  void testDynamicTableUpdateOperatorNewTable() throws Exception {
    int cacheMaximumSize = 10;
    int cacheRefreshMs = 1000;
    int inputSchemaCacheMaximumSize = 10;
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier table = TableIdentifier.of(TABLE);

    assertThat(catalog.tableExists(table)).isFalse();
    DynamicTableUpdateOperator operator =
        new DynamicTableUpdateOperator(
            CATALOG_EXTENSION.catalogLoader(),
            cacheMaximumSize,
            cacheRefreshMs,
            inputSchemaCacheMaximumSize,
            TableCreator.DEFAULT,
            CASE_SENSITIVE,
            PRESERVE_COLUMNS);
    operator.open(null);

    DynamicRecordInternal input =
        new DynamicRecordInternal(
            TABLE,
            "branch",
            SCHEMA1,
            GenericRowData.of(1, "test"),
            PartitionSpec.unpartitioned(),
            42,
            false,
            Collections.emptySet());
    DynamicRecordInternal output = operator.map(input);

    assertThat(catalog.tableExists(table)).isTrue();
    assertThat(input).isEqualTo(output);
  }

  @Test
  void testDynamicTableUpdateOperatorSchemaChange() throws Exception {
    int cacheMaximumSize = 10;
    int cacheRefreshMs = 1000;
    int inputSchemaCacheMaximumSize = 10;
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier table = TableIdentifier.of(TABLE);

    DynamicTableUpdateOperator operator =
        new DynamicTableUpdateOperator(
            CATALOG_EXTENSION.catalogLoader(),
            cacheMaximumSize,
            cacheRefreshMs,
            inputSchemaCacheMaximumSize,
            TableCreator.DEFAULT,
            CASE_SENSITIVE,
            PRESERVE_COLUMNS);
    operator.open(null);

    catalog.createTable(table, SCHEMA1);
    DynamicRecordInternal input =
        new DynamicRecordInternal(
            TABLE,
            "branch",
            SCHEMA2,
            GenericRowData.of(1, "test"),
            PartitionSpec.unpartitioned(),
            42,
            false,
            Collections.emptySet());
    DynamicRecordInternal output = operator.map(input);

    assertThat(catalog.loadTable(table).schema().sameSchema(SCHEMA2)).isTrue();
    assertThat(input).isEqualTo(output);

    // Process the same input again
    DynamicRecordInternal output2 = operator.map(input);
    assertThat(output2).isEqualTo(output);
    assertThat(catalog.loadTable(table).schema().schemaId()).isEqualTo(output.schema().schemaId());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCaseInSensitivity(boolean caseSensitive) throws Exception {
    int cacheMaximumSize = 10;
    int cacheRefreshMs = 1000;
    int inputSchemaCacheMaximumSize = 10;
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier table = TableIdentifier.of(TABLE);

    Schema initialSchema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    Schema caseSensitiveSchema =
        new Schema(Types.NestedField.required(1, "Id", Types.IntegerType.get()));

    DynamicTableUpdateOperator operator =
        new DynamicTableUpdateOperator(
            CATALOG_EXTENSION.catalogLoader(),
            cacheMaximumSize,
            cacheRefreshMs,
            inputSchemaCacheMaximumSize,
            TableCreator.DEFAULT,
            caseSensitive,
            PRESERVE_COLUMNS);
    operator.open(null);

    catalog.createTable(table, initialSchema);
    DynamicRecordInternal input =
        new DynamicRecordInternal(
            TABLE,
            "branch",
            caseSensitiveSchema,
            GenericRowData.of(1, "test"),
            PartitionSpec.unpartitioned(),
            42,
            false,
            Collections.emptySet());
    DynamicRecordInternal output = operator.map(input);

    if (caseSensitive) {
      // Schema changes due to case sensitivity
      Schema expectedSchema =
          new Schema(
              Types.NestedField.optional(2, "Id", Types.IntegerType.get()),
              Types.NestedField.optional(1, "id", Types.IntegerType.get()));
      Schema tableSchema = catalog.loadTable(table).schema();
      assertThat(tableSchema.sameSchema(expectedSchema)).isTrue();
      assertThat(output.schema().sameSchema(expectedSchema)).isTrue();
    } else {
      // No schema change due to case insensitivity
      assertThat(catalog.loadTable(table).schema().sameSchema(initialSchema)).isTrue();
      assertThat(output.schema().sameSchema(initialSchema)).isTrue();
    }
  }

  @Test
  void testDynamicTableUpdateOperatorPreserveUnusedColumns() throws Exception {
    int cacheMaximumSize = 10;
    int cacheRefreshMs = 1000;
    int inputSchemaCacheMaximumSize = 10;
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier table = TableIdentifier.of(TABLE);

    DynamicTableUpdateOperator operator =
        new DynamicTableUpdateOperator(
            CATALOG_EXTENSION.catalogLoader(),
            cacheMaximumSize,
            cacheRefreshMs,
            inputSchemaCacheMaximumSize,
            TableCreator.DEFAULT,
            CASE_SENSITIVE,
            PRESERVE_COLUMNS);
    operator.open(null);

    catalog.createTable(table, SCHEMA2);

    DynamicRecordInternal input =
        new DynamicRecordInternal(
            TABLE,
            "branch",
            SCHEMA1,
            GenericRowData.of(1),
            PartitionSpec.unpartitioned(),
            42,
            false,
            Collections.emptySet());
    DynamicRecordInternal output = operator.map(input);

    Schema tableSchema = catalog.loadTable(table).schema();
    assertThat(tableSchema.columns()).hasSize(2);
    assertThat(tableSchema.findField("id")).isNotNull();
    assertThat(tableSchema.findField("data")).isNotNull();
    assertThat(tableSchema.findField("data").isOptional()).isTrue();
    assertThat(input).isEqualTo(output);
  }

  @Test
  void testDynamicTableUpdateOperatorDropUnusedColumns() throws Exception {
    int cacheMaximumSize = 10;
    int cacheRefreshMs = 1000;
    int inputSchemaCacheMaximumSize = 10;
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier table = TableIdentifier.of(TABLE);

    DynamicTableUpdateOperator operator =
        new DynamicTableUpdateOperator(
            CATALOG_EXTENSION.catalogLoader(),
            cacheMaximumSize,
            cacheRefreshMs,
            inputSchemaCacheMaximumSize,
            TableCreator.DEFAULT,
            CASE_INSENSITIVE,
            DROP_COLUMNS);
    operator.open(null);

    catalog.createTable(table, SCHEMA2);

    DynamicRecordInternal input =
        new DynamicRecordInternal(
            TABLE,
            "branch",
            SCHEMA1,
            GenericRowData.of(1),
            PartitionSpec.unpartitioned(),
            42,
            false,
            Collections.emptySet());
    DynamicRecordInternal output = operator.map(input);

    Schema tableSchema = catalog.loadTable(table).schema();
    assertThat(tableSchema.columns()).hasSize(1);
    assertThat(tableSchema.findField("id")).isNotNull();
    assertThat(tableSchema.findField("data")).isNull();
    assertThat(input).isEqualTo(output);
  }
}
