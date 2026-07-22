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
package org.apache.iceberg.flink;

import static org.apache.parquet.schema.Types.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.reader.ReaderUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.variants.Variant;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

class TestFlinkVariantShreddingType extends CatalogTestBase {

  private static final String TABLE_NAME = "test_table";
  private Table icebergTable;

  @Parameters(name = "catalogName={0}, baseNamespace={1}")
  protected static List<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    parameters.add(new Object[] {"testhadoop", Namespace.empty()});
    parameters.add(new Object[] {"testhadoop_basenamespace", Namespace.of("l0", "l1")});
    return parameters;
  }

  @Override
  @BeforeEach
  public void before() {
    super.before();
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
    sql(
        """
            CREATE TABLE %s (
              id int NOT NULL,
              address variant NOT NULL
            ) WITH (
              'write.format.default'='%s',
              'format-version'='3',
              'shred-variants'='true',
              'variant-inference-buffer-size'='10'
            )
            """,
        TABLE_NAME, FileFormat.PARQUET.name());
    icebergTable = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));
  }

  @Override
  @AfterEach
  public void clean() {
    super.clean();
    getTableEnv()
        .getConfig()
        .getConfiguration()
        .setString("table.exec.resource.default-parallelism", "4");
  }

  @TestTemplate
  public void testExcludingNullValue() throws IOException {
    String values =
        """
            (1, parse_json('{"name": "Alice", "age": 30, "dummy": null}')),
            (2, parse_json('{"name": "Bob", "age": 25}')),
            (3, parse_json('{"name": "Charlie", "age": 35}'))
            """;
    sql("INSERT INTO %s VALUES %s", TABLE_NAME, values);

    GroupType name =
        field(
            "name",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()));
    GroupType age =
        field(
            "age",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(8, true)));
    GroupType address = variant("address", 2, Type.Repetition.REQUIRED, objectFields(age, name));
    MessageType expectedSchema = parquetSchema(address);
    verifyParquetSchema(icebergTable, expectedSchema);
  }

  @TestTemplate
  public void testConsistentType() throws IOException {
    String values =
        """
            (1, parse_json('{"age": "25"}')),
            (2, parse_json('{"age": 30}')),
            (3, parse_json('{"age": "35"}'))
            """;
    sql("INSERT INTO %s VALUES %s", TABLE_NAME, values);

    GroupType age =
        field(
            "age",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()));
    GroupType address = variant("address", 2, Type.Repetition.REQUIRED, objectFields(age));
    MessageType expectedSchema = parquetSchema(address);
    verifyParquetSchema(icebergTable, expectedSchema);
  }

  @TestTemplate
  public void testPrimitiveType() throws IOException {
    String values =
        """
            (1, parse_json('123')),
            (2, parse_json('"abc"')),
            (3, parse_json('12'))
            """;
    sql("INSERT INTO %s VALUES %s", TABLE_NAME, values);

    GroupType address =
        variant(
            "address",
            2,
            Type.Repetition.REQUIRED,
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(8, true)));
    MessageType expectedSchema = parquetSchema(address);

    assertThat(SimpleDataUtil.tableRecords(icebergTable)).hasSize(3);
    verifyParquetSchema(icebergTable, expectedSchema);
  }

  @TestTemplate
  public void testPrimitiveDecimalType() throws IOException {
    String values =
        """
            (1, parse_json('123.56')),
            (2, parse_json('"abc"')),
            (3, parse_json('12.56'))
            """;
    sql("INSERT INTO %s VALUES %s", TABLE_NAME, values);

    GroupType address =
        variant(
            "address",
            2,
            Type.Repetition.REQUIRED,
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.decimalType(2, 5)));
    MessageType expectedSchema = parquetSchema(address);
    assertThat(SimpleDataUtil.tableRecords(icebergTable)).hasSize(3);
    verifyParquetSchema(icebergTable, expectedSchema);
  }

  @TestTemplate
  public void testBooleanType() throws IOException {
    String values =
        """
            (1, parse_json('{"active": true}')),
            (2, parse_json('{"active": false}')),
            (3, parse_json('{"active": true}'))
            """;
    sql("INSERT INTO %s VALUES %s", TABLE_NAME, values);

    GroupType active = field("active", shreddedPrimitive(PrimitiveType.PrimitiveTypeName.BOOLEAN));
    GroupType address = variant("address", 2, Type.Repetition.REQUIRED, objectFields(active));
    MessageType expectedSchema = parquetSchema(address);
    verifyParquetSchema(icebergTable, expectedSchema);
  }

  @TestTemplate
  public void testDecimalTypeWithInconsistentScales() throws IOException {
    String values =
        """
            (1, parse_json('{"price": 123.456789}')),
            (2, parse_json('{"price": 678.90}')),
            (3, parse_json('{"price": 999.99}'))
            """;
    sql("INSERT INTO %s VALUES %s", TABLE_NAME, values);

    GroupType price =
        field(
            "price",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.decimalType(6, 9)));
    GroupType address = variant("address", 2, Type.Repetition.REQUIRED, objectFields(price));
    MessageType expectedSchema = parquetSchema(address);
    verifyParquetSchema(icebergTable, expectedSchema);
  }

  @TestTemplate
  public void testDecimalTypeWithConsistentScales() throws IOException {
    String values =
        """
            (1, parse_json('{"price": 123.45}')),
            (2, parse_json('{"price": 678.90}')),
            (3, parse_json('{"price": 999.99}'))
            """;
    sql("INSERT INTO %s VALUES %s", TABLE_NAME, values);

    GroupType price =
        field(
            "price",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.decimalType(2, 5)));
    GroupType address = variant("address", 2, Type.Repetition.REQUIRED, objectFields(price));
    MessageType expectedSchema = parquetSchema(address);
    verifyParquetSchema(icebergTable, expectedSchema);
  }

  @TestTemplate
  public void testArrayType() throws IOException {
    String values =
        """
            (1, parse_json('["java", "scala", "python"]')),
            (2, parse_json('["rust", "go"]')),
            (3, parse_json('["javascript"]'))
            """;
    sql("INSERT INTO %s VALUES %s", TABLE_NAME, values);

    GroupType arr =
        list(
            element(
                shreddedPrimitive(
                    PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType())));
    GroupType address = variant("address", 2, Type.Repetition.REQUIRED, arr);
    MessageType expectedSchema = parquetSchema(address);

    verifyParquetSchema(icebergTable, expectedSchema);
  }

  @TestTemplate
  public void testNestedArrayType() throws IOException {

    String values =
        """
            (1, parse_json('{"tags": ["java", "scala", "python"]}')),
            (2, parse_json('{"tags": ["rust", "go"]}')),
            (3, parse_json('{"tags": ["javascript"]}'))
            """;
    sql("INSERT INTO %s VALUES %s", TABLE_NAME, values);

    GroupType tags =
        field(
            "tags",
            list(
                element(
                    shreddedPrimitive(
                        PrimitiveType.PrimitiveTypeName.BINARY,
                        LogicalTypeAnnotation.stringType()))));
    GroupType address = variant("address", 2, Type.Repetition.REQUIRED, objectFields(tags));
    MessageType expectedSchema = parquetSchema(address);

    verifyParquetSchema(icebergTable, expectedSchema);
  }

  @TestTemplate
  public void testNestedObjectType() throws IOException {

    String values =
        """
            (1, parse_json('{"location": {"city": "Seattle", "zip": 98101}, "tags": ["java", "scala", "python"]}')),
            (2, parse_json('{"location": {"city": "Portland", "zip": 97201}}')),
            (3, parse_json('{"location": {"city": "NYC", "zip": 10001}}'))
            """;
    sql("INSERT INTO %s VALUES %s", TABLE_NAME, values);

    GroupType city =
        field(
            "city",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()));
    GroupType zip =
        field(
            "zip",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(32, true)));
    GroupType location = field("location", objectFields(city, zip));
    GroupType tags =
        field(
            "tags",
            list(
                element(
                    shreddedPrimitive(
                        PrimitiveType.PrimitiveTypeName.BINARY,
                        LogicalTypeAnnotation.stringType()))));

    GroupType address =
        variant("address", 2, Type.Repetition.REQUIRED, objectFields(location, tags));
    MessageType expectedSchema = parquetSchema(address);

    verifyParquetSchema(icebergTable, expectedSchema);
  }

  @TestTemplate
  public void testLazyInitializationWithBufferedRows() throws IOException {

    String values =
        """
            (1, parse_json('{"name": "Alice", "age": 30}')),
            (2, parse_json('{"name": "Bob", "age": 25}')),
            (3, parse_json('{"name": "Charlie", "age": 35}')),
            (4, parse_json('{"name": "David", "age": 28}')),
            (5, parse_json('{"name": "Eve", "age": 32}')),
            (6, parse_json('{"name": "Frank", "age": 40}')),
            (7, parse_json('{"name": "Grace", "age": 27}'))
            """;
    sql("INSERT INTO %s VALUES %s", TABLE_NAME, values);

    GroupType name =
        field(
            "name",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()));
    GroupType age =
        field(
            "age",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(8, true)));
    GroupType address = variant("address", 2, Type.Repetition.REQUIRED, objectFields(age, name));
    MessageType expectedSchema = parquetSchema(address);

    verifyParquetSchema(icebergTable, expectedSchema);
    assertThat(genericRowData()).hasSize(7);
  }

  @TestTemplate
  public void testColumnIndexTruncateLength() throws IOException {
    sql("ALTER TABLE %s SET('variant-inference-buffer-size'='3')", TABLE_NAME);

    int customTruncateLength = 10;
    sql(
        "ALTER TABLE %s SET ('%s'='%d')",
        TABLE_NAME, "parquet.columnindex.truncate.length", customTruncateLength);

    StringBuilder valuesBuilder = new StringBuilder();
    for (int i = 1; i <= 10; i++) {
      if (i > 1) {
        valuesBuilder.append(", ");
      }

      String longValue = "A".repeat(20);
      valuesBuilder.append(
          String.format(
              """
                      (%d, parse_json('{"description": "%s", "id": %d}'))
                      """
                  .trim(),
              i,
              longValue,
              i));
    }
    sql("INSERT INTO %s VALUES %s", TABLE_NAME, valuesBuilder.toString());

    GroupType description =
        field(
            "description",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()));
    GroupType id =
        field(
            "id",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(8, true)));
    GroupType address =
        variant("address", 2, Type.Repetition.REQUIRED, objectFields(description, id));
    MessageType expectedSchema = parquetSchema(address);

    verifyParquetSchema(icebergTable, expectedSchema);
    assertThat(genericRowData()).hasSize(10);
  }

  @TestTemplate
  public void testIntegerFamilyPromotion() throws IOException {

    // Mix of INT8, INT16, INT32, INT64 - should promote to INT64
    String values =
        """
            (1, parse_json('{"value": 10}')),
            (2, parse_json('{"value": 1000}')),
            (3, parse_json('{"value": 100000}')),
            (4, parse_json('{"value": 10000000000}'))
            """;
    sql("INSERT INTO %s VALUES %s", TABLE_NAME, values);

    GroupType value =
        field(
            "value",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.INT64, LogicalTypeAnnotation.intType(64, true)));
    GroupType address = variant("address", 2, Type.Repetition.REQUIRED, objectFields(value));
    MessageType expectedSchema = parquetSchema(address);

    verifyParquetSchema(icebergTable, expectedSchema);
  }

  @TestTemplate
  public void testDecimalFamilyPromotion() throws IOException {

    // Test that they get promoted to the most capable decimal type observed
    String values =
        """
            (1, parse_json('{"value": 1.5}')),
            (2, parse_json('{"value": 123.456789}')),
            (3, parse_json('{"value": 123456789123456.789}'))
            """;
    sql("INSERT INTO %s VALUES %s", TABLE_NAME, values);

    GroupType value =
        field(
            "value",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
                9, // decimalRequiredBytes(21)
                LogicalTypeAnnotation.decimalType(6, 21)));
    GroupType address = variant("address", 2, Type.Repetition.REQUIRED, objectFields(value));
    MessageType expectedSchema = parquetSchema(address);

    verifyParquetSchema(icebergTable, expectedSchema);
  }

  @TestTemplate
  public void testDataRoundTripWithShredding() throws IOException {
    String values =
        """
            (1, parse_json('{"name": "Alice", "age": 30}')),
            (2, parse_json('{"name": "Bob", "age": 25}')),
            (3, parse_json('{"name": "Charlie", "age": 35}'))
            """;
    sql("INSERT INTO %s VALUES %s", TABLE_NAME, values);

    GroupType name =
        field(
            "name",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()));
    GroupType age =
        field(
            "age",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(8, true)));
    GroupType address = variant("address", 2, Type.Repetition.REQUIRED, objectFields(age, name));
    MessageType expectedSchema = parquetSchema(address);

    verifyParquetSchema(icebergTable, expectedSchema);

    // Verify that we can read the data back correctly
    List<Row> rows =
        sql(
            """
                    SELECT id,
                           JSON_VALUE(address, '$.name'),
                           JSON_VALUE(address, '$.age' RETURNING int)
                    FROM %s
                    ORDER BY id
                    """,
            TABLE_NAME);
    assertThat(rows).hasSize(3);
    assertThat(rows.get(0).getField(0)).isEqualTo(1);
    assertThat(rows.get(0).getField(1)).isEqualTo("Alice");
    assertThat(rows.get(0).getField(2)).isEqualTo(30);
    assertThat(rows.get(1).getField(0)).isEqualTo(2);
    assertThat(rows.get(1).getField(1)).isEqualTo("Bob");
    assertThat(rows.get(1).getField(2)).isEqualTo(25);
    assertThat(rows.get(2).getField(0)).isEqualTo(3);
    assertThat(rows.get(2).getField(1)).isEqualTo("Charlie");
    assertThat(rows.get(2).getField(2)).isEqualTo(35);
  }

  @TestTemplate
  public void testVariantWithNullValues() throws IOException {

    String values =
        """
            (1, parse_json('null')),
            (2, parse_json('null')),
            (3, parse_json('null'))
            """;
    sql("INSERT INTO %s VALUES %s", TABLE_NAME, values);

    GroupType address = variant("address", 2, Type.Repetition.REQUIRED);
    MessageType expectedSchema = parquetSchema(address);

    verifyParquetSchema(icebergTable, expectedSchema);
  }

  @TestTemplate
  public void testArrayOfNullElementsWithShredding() throws IOException {

    sql(
        """
            INSERT INTO %s VALUES
              (1, parse_json('[null, null, null]')),
              (2, parse_json('[null]'))
            """,
        TABLE_NAME);

    // Array elements are all null, element type is null, falls back to unshredded
    GroupType address = variant("address", 2, Type.Repetition.REQUIRED);
    MessageType expectedSchema = parquetSchema(address);

    verifyParquetSchema(icebergTable, expectedSchema);
  }

  @TestTemplate
  public void testInfrequentFieldPruning() throws IOException {
    // This test relies on the current VariantShreddingAnalyzer MIN_FIELD_FREQUENCY threshold of
    // 0.10: rare_field appears in 1/11 rows (~0.09), so it should be pruned.
    sql("ALTER TABLE %s SET('variant-inference-buffer-size'='11')", TABLE_NAME);
    StringBuilder valuesBuilder = new StringBuilder();
    for (int i = 1; i <= 11; i++) {
      if (i > 1) {
        valuesBuilder.append(", ");
      }

      if (i == 1) {
        // Only the first row has rare_field
        valuesBuilder.append(
            String.format(
                """
                        (%d, parse_json('{"name": "User%d", "rare_field": "rare"}'))
                        """
                    .trim(),
                i,
                i));
      } else {
        valuesBuilder.append(
            String.format(
                """
                        (%d, parse_json('{"name": "User%d"}'))
                        """
                    .trim(),
                i,
                i));
      }
    }

    sql("INSERT INTO %s VALUES %s", TABLE_NAME, valuesBuilder.toString());

    // rare_field appears in 1/11 rows, should be pruned
    // name appears in 11/11 rows and should be kept
    GroupType name =
        field(
            "name",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()));
    GroupType address = variant("address", 2, Type.Repetition.REQUIRED, objectFields(name));
    MessageType expectedSchema = parquetSchema(address);

    verifyParquetSchema(icebergTable, expectedSchema);
  }

  @TestTemplate
  public void testMixedTypeTieBreaking() throws IOException {
    StringBuilder valuesBuilder = new StringBuilder();
    for (int i = 1; i <= 10; i++) {
      if (i > 1) {
        valuesBuilder.append(", ");
      }

      if (i <= 5) {
        valuesBuilder.append(
            String.format(
                """
                        (%d, parse_json('{"val": %d}'))
                        """
                    .trim(),
                i,
                i));
      } else {
        valuesBuilder.append(
            String.format(
                """
                        (%d, parse_json('{"val": "text%d"}'))
                        """
                    .trim(),
                i,
                i));
      }
    }

    sql("INSERT INTO %s VALUES %s", TABLE_NAME, valuesBuilder.toString());

    // 5 ints + 5 strings is a tie so STRING wins (higher TIE_BREAK_PRIORITY)
    GroupType val =
        field(
            "val",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()));
    GroupType address = variant("address", 2, Type.Repetition.REQUIRED, objectFields(val));
    MessageType expectedSchema = parquetSchema(address);

    verifyParquetSchema(icebergTable, expectedSchema);
  }

  @TestTemplate
  public void testFieldOnlyAfterBuffer() throws IOException {
    getTableEnv()
        .getConfig()
        .getConfiguration()
        .setString("table.exec.resource.default-parallelism", "1");

    sql("ALTER TABLE %s SET('variant-inference-buffer-size'='3')", TABLE_NAME);

    sql(
        """
            CREATE TEMPORARY VIEW tmp_source AS
            SELECT * FROM (VALUES
              (1, parse_json('{"name": "Alice"}')),
              (2, parse_json('{"name": "Bob"}')),
              (3, parse_json('{"name": "Charlie"}')),
              (4, parse_json('{"name": "David", "score": 95}')),
              (5, parse_json('{"name": "Eve", "score": 88}')),
              (6, parse_json('{"name": "Frank", "score": 72}')),
              (7, parse_json('{"name": "Grace", "score": 91}'))
            ) AS t(id, address)
            """);

    sql("INSERT INTO %s SELECT id, address FROM tmp_source ORDER BY id", TABLE_NAME);

    // Schema is determined from buffer (rows 1-3) which only has "name".
    // "score" is not shredded
    GroupType name =
        field(
            "name",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()));
    GroupType address = variant("address", 2, Type.Repetition.REQUIRED, objectFields(name));
    MessageType expectedSchema = parquetSchema(address);

    verifyParquetSchema(icebergTable, expectedSchema);

    // Verify all data round-trips despite "score" not being shredded
    List<Row> rows =
        sql(
            """
                    SELECT id,
                           JSON_VALUE(address, '$.name'),
                           JSON_VALUE(address, '$.score' RETURNING int)
                    FROM %s
                    ORDER BY id
                    """,
            TABLE_NAME);
    assertThat(rows).hasSize(7);
    assertThat(rows.get(0).getField(1)).isEqualTo("Alice");
    assertThat(rows.get(0).getField(2)).isNull();
    assertThat(rows.get(3).getField(1)).isEqualTo("David");
    assertThat(rows.get(3).getField(2)).isEqualTo(95);
    assertThat(rows.get(6).getField(1)).isEqualTo("Grace");
    assertThat(rows.get(6).getField(2)).isEqualTo(91);

    sql("DROP TEMPORARY VIEW IF EXISTS tmp_source");
  }

  @TestTemplate
  public void testCrossFileDifferentShreddedType() throws IOException {
    sql("ALTER TABLE %s SET('variant-inference-buffer-size'='3')", TABLE_NAME);

    // File 1: "score" is always integer → shredded as INT8
    String batch1 =
        """
            (1, parse_json('{"score": 95}')),
            (2, parse_json('{"score": 88}')),
            (3, parse_json('{"score": 72}'))
            """;
    sql("INSERT INTO %s VALUES %s", TABLE_NAME, batch1);

    // Verify file 1 schema: score shredded as INT8
    GroupType scoreInt =
        field(
            "score",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(8, true)));
    MessageType expectedSchema1 =
        parquetSchema(variant("address", 2, Type.Repetition.REQUIRED, objectFields(scoreInt)));
    verifyParquetSchema(icebergTable, expectedSchema1);

    // File 2: "score" is always string → shredded as STRING
    String batch2 =
        """
            (4, parse_json('{"score": "high"}')),
            (5, parse_json('{"score": "medium"}')),
            (6, parse_json('{"score": "low"}'))
            """;
    sql("INSERT INTO %s VALUES %s", TABLE_NAME, batch2);

    // Query across both files, reader must handle different shredded types
    List<Row> rows =
        sql(
            """
                    SELECT id,
                           json_value(address, '$.score')
                    FROM %s
                    ORDER BY id
                    """,
            TABLE_NAME);
    assertThat(rows).hasSize(6);
    assertThat(rows.get(0).getField(1)).isEqualTo("95");
    assertThat(rows.get(1).getField(1)).isEqualTo("88");
    assertThat(rows.get(3).getField(1)).isEqualTo("high");
    assertThat(rows.get(5).getField(1)).isEqualTo("low");
  }

  @TestTemplate
  public void testAllNullVariantColumn() throws IOException {

    String variantNullAbleTableName = "test_all_null_variant_column";
    sql(
        """
            CREATE TABLE %s (
              id int NOT NULL,
              address variant
            ) WITH (
              'write.format.default'='%s',
              'format-version'='3',
              'shred-variants'='true',
              'variant-inference-buffer-size'='10'
            )
            """,
        variantNullAbleTableName, FileFormat.PARQUET.name());

    sql(
        """
            INSERT INTO %s VALUES
              (1, CAST(null AS VARIANT)),
              (2, CAST(null AS VARIANT)),
              (3, CAST(null AS VARIANT))
            """,
        variantNullAbleTableName);

    // All variant values are SQL NULL, so no shredding should occur
    MessageType expectedSchema = parquetSchema(variant("address", 2, Type.Repetition.OPTIONAL));
    Table variantNullAbleTable =
        validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, variantNullAbleTableName));
    verifyParquetSchema(variantNullAbleTable, expectedSchema);

    List<Row> rows = sql("SELECT id, address FROM %s ORDER BY id", variantNullAbleTableName);
    assertThat(rows).hasSize(3);
    assertThat(rows.get(0).getField(1)).isNull();
    assertThat(rows.get(1).getField(1)).isNull();
    assertThat(rows.get(2).getField(1)).isNull();
  }

  @TestTemplate
  public void testBufferSizeOne() throws IOException {
    sql("ALTER TABLE %s SET('variant-inference-buffer-size'='1')", TABLE_NAME);

    sql(
        """
            INSERT INTO %s VALUES
              (1, parse_json('{"name": "Alice", "age": 30}')),
              (2, parse_json('{"name": "Bob", "age": 25}')),
              (3, parse_json('{"name": "Charlie", "age": 35}'))
            """,
        TABLE_NAME);

    // Schema inferred from first row only, should still shred name and age
    GroupType age =
        field(
            "age",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(8, true)));
    GroupType name =
        field(
            "name",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()));
    GroupType address = variant("address", 2, Type.Repetition.REQUIRED, objectFields(age, name));
    MessageType expectedSchema = parquetSchema(address);

    verifyParquetSchema(icebergTable, expectedSchema);

    List<Row> rows =
        sql(
            """
                    SELECT id,
                           json_value(address, '$.name')
                    FROM %s
                    ORDER BY id
                    """,
            TABLE_NAME);
    assertThat(rows).hasSize(3);
    assertThat(rows.get(0).getField(1)).isEqualTo("Alice");
    assertThat(rows.get(2).getField(1)).isEqualTo("Charlie");
  }

  @TestTemplate
  public void testDecimalFallbackAfterBuffer() throws IOException {
    getTableEnv()
        .getConfig()
        .getConfiguration()
        .setString("table.exec.resource.default-parallelism", "1");

    sql("ALTER TABLE %s SET('variant-inference-buffer-size'='3')", TABLE_NAME);

    // Buffer: scale=2, 3 integer digits -> DECIMAL(5,2)
    // Row 4: precision overflow -> fallback to value field
    // Row 5: scale overflow -> fallback to value field
    // Row 6: fits typed column, scale widened from 1 to 2 via setScale
    sql(
        """
            CREATE TEMPORARY VIEW tmp_source AS
            SELECT * FROM (VALUES
              (1, parse_json('{"val": 123.45}')),
              (2, parse_json('{"val": 678.90}')),
              (3, parse_json('{"val": 999.99}')),
              (4, parse_json('{"val": 123456.78}')),
              (5, parse_json('{"val": 1.2345}')),
              (6, parse_json('{"val": 12.3}'))
            ) AS t(id, address)
            """);

    sql("INSERT INTO %s SELECT id, address FROM tmp_source ORDER BY id", TABLE_NAME);

    GroupType val =
        field(
            "val",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.decimalType(2, 5)));
    GroupType address = variant("address", 2, Type.Repetition.REQUIRED, objectFields(val));
    MessageType expectedSchema = parquetSchema(address);

    verifyParquetSchema(icebergTable, expectedSchema);

    List<Row> rows =
        sql(
            """
                    SELECT id,
                           CAST(json_value(address, '$.val') AS DECIMAL(10, 4))
                    FROM %s
                    ORDER BY id
                    """,
            TABLE_NAME);
    assertThat(rows).hasSize(6);
    assertThat(rows.get(0).getField(1)).isEqualTo(new BigDecimal("123.4500"));
    assertThat(rows.get(3).getField(1)).isEqualTo(new BigDecimal("123456.7800"));
    assertThat(rows.get(4).getField(1)).isEqualTo(new BigDecimal("1.2345"));
    assertThat(rows.get(5).getField(1)).isEqualTo(new BigDecimal("12.3000"));

    sql("DROP TEMPORARY VIEW IF EXISTS tmp_source");
  }

  private void verifyParquetSchema(Table table, MessageType expectedSchema) throws IOException {
    table.refresh();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      assertThat(tasks).isNotEmpty();

      FileScanTask task = tasks.iterator().next();
      String path = task.file().location();

      HadoopInputFile inputFile =
          HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(path), new Configuration());

      try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
        MessageType actualSchema = reader.getFileMetaData().getSchema();
        assertThat(actualSchema).isEqualTo(expectedSchema);
      }
    }
  }

  private static MessageType parquetSchema(Type variantTypes) {
    return org.apache.parquet.schema.Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.INT32)
        .id(1)
        .named("id")
        .addFields(variantTypes)
        .named("table");
  }

  private static GroupType variant(String name, int fieldId, Type.Repetition repetition) {
    return org.apache.parquet.schema.Types.buildGroup(repetition)
        .id(fieldId)
        .as(LogicalTypeAnnotation.variantType(Variant.VARIANT_SPEC_VERSION))
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("metadata")
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("value")
        .named(name);
  }

  private static GroupType variant(
      String name, int fieldId, Type.Repetition repetition, Type shreddedType) {
    checkShreddedType(shreddedType);
    return org.apache.parquet.schema.Types.buildGroup(repetition)
        .id(fieldId)
        .as(LogicalTypeAnnotation.variantType(Variant.VARIANT_SPEC_VERSION))
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("metadata")
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("value")
        .addField(shreddedType)
        .named(name);
  }

  private static Type shreddedPrimitive(PrimitiveType.PrimitiveTypeName primitive) {
    return optional(primitive).named("typed_value");
  }

  private static Type shreddedPrimitive(
      PrimitiveType.PrimitiveTypeName primitive, LogicalTypeAnnotation annotation) {
    return optional(primitive).as(annotation).named("typed_value");
  }

  private static Type shreddedPrimitive(
      PrimitiveType.PrimitiveTypeName primitive, int length, LogicalTypeAnnotation annotation) {
    return optional(primitive).length(length).as(annotation).named("typed_value");
  }

  private static GroupType objectFields(GroupType... fields) {
    for (GroupType fieldType : fields) {
      checkField(fieldType);
    }

    return org.apache.parquet.schema.Types.buildGroup(Type.Repetition.OPTIONAL)
        .addFields(fields)
        .named("typed_value");
  }

  private static GroupType field(String name, Type shreddedType) {
    checkShreddedType(shreddedType);
    return org.apache.parquet.schema.Types.buildGroup(Type.Repetition.REQUIRED)
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("value")
        .addField(shreddedType)
        .named(name);
  }

  private static GroupType element(Type shreddedType) {
    return field("element", shreddedType);
  }

  private static GroupType list(GroupType elementType) {
    return org.apache.parquet.schema.Types.optionalList().element(elementType).named("typed_value");
  }

  private static void checkShreddedType(Type shreddedType) {
    Preconditions.checkArgument(
        shreddedType.getName().equals("typed_value"),
        "Invalid shredded type name: %s should be typed_value",
        shreddedType.getName());
    Preconditions.checkArgument(
        shreddedType.isRepetition(Type.Repetition.OPTIONAL),
        "Invalid shredded type repetition: %s should be OPTIONAL",
        shreddedType.getRepetition());
  }

  private static void checkField(GroupType fieldType) {
    Preconditions.checkArgument(
        fieldType.isRepetition(Type.Repetition.REQUIRED),
        "Invalid field type repetition: %s should be REQUIRED",
        fieldType.getRepetition());
  }

  private List<GenericRowData> genericRowData() throws IOException {
    List<GenericRowData> genericRowData = Lists.newArrayList();
    try (CloseableIterable<CombinedScanTask> combinedScanTasks =
        icebergTable.newScan().planTasks()) {
      for (CombinedScanTask combinedScanTask : combinedScanTasks) {
        try (DataIterator<RowData> dataIterator =
            ReaderUtil.createDataIterator(
                combinedScanTask, icebergTable.schema(), icebergTable.schema())) {
          while (dataIterator.hasNext()) {
            GenericRowData rowData = (GenericRowData) dataIterator.next();
            genericRowData.add(rowData);
          }
        }
      }
    }

    return genericRowData;
  }
}
