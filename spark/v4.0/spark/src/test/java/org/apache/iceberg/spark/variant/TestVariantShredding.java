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
package org.apache.iceberg.spark.variant;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestVariantShredding extends CatalogTestBase {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "address", Types.VariantType.get()));

  private static final Schema SCHEMA2 =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "address", Types.VariantType.get()),
          Types.NestedField.optional(3, "metadata", Types.VariantType.get()));

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties()
      },
    };
  }

  @BeforeAll
  public static void startMetastoreAndSpark() {
    // First call parent to initialize metastore and spark with local[2]
    CatalogTestBase.startMetastoreAndSpark();

    // Now stop and recreate spark with local[1] to write all rows to a single file
    if (spark != null) {
      spark.stop();
    }

    spark =
        SparkSession.builder()
            .master("local[1]") // Use one thread to write the rows to a single parquet file
            .config("spark.driver.host", InetAddress.getLoopbackAddress().getHostAddress())
            .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
            .config("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
            .config("spark.sql.legacy.respectNullabilityInTextDatasetConversion", "true")
            .enableHiveSupport()
            .getOrCreate();

    sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
  }

  @BeforeEach
  public void before() {
    super.before();
    validationCatalog.createTable(
        tableIdent, SCHEMA, null, Map.of(TableProperties.FORMAT_VERSION, "3"));
  }

  @AfterEach
  public void after() {
    validationCatalog.dropTable(tableIdent, true);
  }

  @TestTemplate
  public void testVariantShreddingWrite() throws IOException {
    spark.conf().set(SparkSQLProperties.SHRED_VARIANTS, "true");
    String values =
        "(1, parse_json('{\"name\": \"Joe\", \"streets\": [\"Apt #3\", \"1234 Ave\"], \"zip\": 10001}')), (2, null)";
    sql("INSERT INTO %s VALUES %s", tableName, values);

    GroupType name =
        field(
            "name",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()));
    GroupType streets =
        field(
            "streets",
            list(
                element(
                    shreddedPrimitive(
                        PrimitiveType.PrimitiveTypeName.BINARY,
                        LogicalTypeAnnotation.stringType()))));
    GroupType zip =
        field(
            "zip",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(16)));
    GroupType address = variant("address", 2, Type.Repetition.OPTIONAL, objectFields(name, streets, zip));
    MessageType expectedSchema = parquetSchema(address);

    Table table = validationCatalog.loadTable(tableIdent);
    verifyParquetSchema(table, expectedSchema);
  }

  @TestTemplate
  public void testVariantShreddingWithNullFirstRow() throws IOException {
    spark.conf().set(SparkSQLProperties.SHRED_VARIANTS, "true");

    String values = "(1, null), (2, parse_json('{\"city\": \"Seattle\", \"state\": \"WA\"}'))";
    sql("INSERT INTO %s VALUES %s", tableName, values);

    GroupType city =
        field(
            "city",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()));
    GroupType state =
        field(
            "state",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()));
    GroupType address = variant("address", 2, Type.Repetition.OPTIONAL, objectFields(city, state));
    MessageType expectedSchema = parquetSchema(address);

    Table table = validationCatalog.loadTable(tableIdent);
    verifyParquetSchema(table, expectedSchema);
  }

  @TestTemplate
  public void testVariantShreddingWithTwoVariantColumns() throws IOException {
    validationCatalog.dropTable(tableIdent, true);
    validationCatalog.createTable(
        tableIdent, SCHEMA2, null, Map.of(TableProperties.FORMAT_VERSION, "3"));

    spark.conf().set(SparkSQLProperties.SHRED_VARIANTS, "true");

    String values =
        "(1, parse_json('{\"city\": \"NYC\", \"zip\": 10001}'), parse_json('{\"type\": \"home\", \"verified\": true}')), "
            + "(2, null, null)";
    sql("INSERT INTO %s VALUES %s", tableName, values);

    GroupType city =
        field(
            "city",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()));
    GroupType zip =
        field(
            "zip",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(16, true)));
    GroupType address = variant("address", 2, Type.Repetition.OPTIONAL, objectFields(city, zip));

    GroupType type =
        field(
            "type",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()));
    GroupType verified =
        field("verified", shreddedPrimitive(PrimitiveType.PrimitiveTypeName.BOOLEAN));
    GroupType metadata = variant("metadata", 3, Type.Repetition.OPTIONAL, objectFields(type, verified));

    MessageType expectedSchema = parquetSchema(address, metadata);

    Table table = validationCatalog.loadTable(tableIdent);
    verifyParquetSchema(table, expectedSchema);
  }

  @TestTemplate
  public void testVariantShreddingWithTwoVariantColumnsOneNull() throws IOException {
    validationCatalog.dropTable(tableIdent, true);
    validationCatalog.createTable(
        tableIdent, SCHEMA2, null, Map.of(TableProperties.FORMAT_VERSION, "3"));

    spark.conf().set(SparkSQLProperties.SHRED_VARIANTS, "true");

    // First row: address is null, metadata has value
    // Second row: address has value, metadata is null
    String values =
        "(1, null, parse_json('{\"label\": \"primary\"}')),"
            + " (2, parse_json('{\"street\": \"Main St\"}'), null)";
    sql("INSERT INTO %s VALUES %s", tableName, values);

    GroupType street =
        field(
            "street",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()));
    GroupType address = variant("address", 2, Type.Repetition.OPTIONAL, objectFields(street));

    GroupType label =
        field(
            "label",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()));
    GroupType metadata = variant("metadata", 3, Type.Repetition.OPTIONAL, objectFields(label));

    MessageType expectedSchema = parquetSchema(address, metadata);

    Table table = validationCatalog.loadTable(tableIdent);
    verifyParquetSchema(table, expectedSchema);
  }

  @TestTemplate
  public void testVariantShreddingDisabled() throws IOException {
    // Test with shredding explicitly disabled
    spark.conf().set(SparkSQLProperties.SHRED_VARIANTS, "false");

    String values = "(1, parse_json('{\"city\": \"NYC\", \"zip\": 10001}')), (2, null)";
    sql("INSERT INTO %s VALUES %s", tableName, values);

    GroupType address = variant("address", 2, Type.Repetition.OPTIONAL);
    MessageType expectedSchema = parquetSchema(address);

    Table table = validationCatalog.loadTable(tableIdent);
    verifyParquetSchema(table, expectedSchema);
  }

  @TestTemplate
  public void testConsistentTypeCreatesTypedValue() throws IOException {
    spark.conf().set(SparkSQLProperties.SHRED_VARIANTS, "true");

    String values =
        "(1, parse_json('{\"name\": \"Alice\", \"age\": 30}')),"
            + " (2, parse_json('{\"name\": \"Bob\", \"age\": 25}')),"
            + " (3, parse_json('{\"name\": \"Charlie\", \"age\": 35}'))";
    sql("INSERT INTO %s VALUES %s", tableName, values);

    GroupType name =
        field(
            "name",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()));
    GroupType age =
        field("age", shreddedPrimitive(PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(8, true)));
    GroupType address = variant("address", 2, Type.Repetition.REQUIRED, objectFields(age, name));
    MessageType expectedSchema = parquetSchema(address);

    Table table = validationCatalog.loadTable(tableIdent);
    verifyParquetSchema(table, expectedSchema);
  }

  /**
   * Test Heuristic 2: Inconsistent Type → Value Only
   *
   * <p>When a field appears with different types across rows, only the "value" field should be
   * created (no "typed_value").
   */
  @TestTemplate
  public void testInconsistentTypeCreatesValueOnly() throws IOException {
    spark.conf().set(SparkSQLProperties.SHRED_VARIANTS, "true");

    // "age" appears as both string and int - inconsistent type
    String values =
        "(1, parse_json('{\"age\": \"25\"}')),"
            + " (2, parse_json('{\"age\": 30}')),"
            + " (3, parse_json('{\"age\": \"35\"}'))";
    sql("INSERT INTO %s VALUES %s", tableName, values);

    // "age" should have only "value" field, no "typed_value"
    GroupType age = valueOnlyField("age");
    GroupType address = variant("address", 2, Type.Repetition.REQUIRED, objectFields(age));
    MessageType expectedSchema = parquetSchema(address);

    Table table = validationCatalog.loadTable(tableIdent);
    verifyParquetSchema(table, expectedSchema);
  }

  /**
   * Test Heuristic 3: Rare Fields Are Dropped
   *
   * <p>Fields that appear in less than the configured threshold percentage of rows should be
   * dropped from the shredded schema.
   */
  @TestTemplate
  public void testRareFieldIsDropped() throws IOException {
    spark.conf().set(SparkSQLProperties.SHRED_VARIANTS, "true");
    // Set threshold to 20% (0.2)
    spark.conf().set(SparkSQLProperties.VARIANT_MIN_OCCURRENCE_THRESHOLD, "0.2");

    // "common" appears in all 10 rows (100%), "rare" appears in 1 row (10%)
    String values =
        "(1, parse_json('{\"common\": 1, \"rare\": 100}')),"
            + " (2, parse_json('{\"common\": 2}')),"
            + " (3, parse_json('{\"common\": 3}')),"
            + " (4, parse_json('{\"common\": 4}')),"
            + " (5, parse_json('{\"common\": 5}')),"
            + " (6, parse_json('{\"common\": 6}')),"
            + " (7, parse_json('{\"common\": 7}')),"
            + " (8, parse_json('{\"common\": 8}')),"
            + " (9, parse_json('{\"common\": 9}')),"
            + " (10, parse_json('{\"common\": 10}'))";
    sql("INSERT INTO %s VALUES %s", tableName, values);

    // Only "common" should be present (appears in 100% of rows)
    // "rare" should be dropped (appears in only 10% of rows, below 20% threshold)
    GroupType common =
        field("common", shreddedPrimitive(PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(8, true)));
    GroupType address = variant("address", 2, Type.Repetition.REQUIRED, objectFields(common));
    MessageType expectedSchema = parquetSchema(address);

    Table table = validationCatalog.loadTable(tableIdent);
    verifyParquetSchema(table, expectedSchema);

    // Reset threshold to default to avoid interference with other tests
    spark.conf().unset(SparkSQLProperties.VARIANT_MIN_OCCURRENCE_THRESHOLD);
  }

  /**
   * Test Heuristic 4: Boolean Type Handling
   *
   * <p>Both "true" and "false" values should be treated as the same consistent boolean type, and a
   * typed_value field should be created.
   */
  @TestTemplate
  public void testBooleanTypeHandling() throws IOException {
    spark.conf().set(SparkSQLProperties.SHRED_VARIANTS, "true");

    // "active" field has both true and false values - should be treated as consistent boolean
    String values =
        "(1, parse_json('{\"active\": true}')),"
            + " (2, parse_json('{\"active\": false}')),"
            + " (3, parse_json('{\"active\": true}'))";
    sql("INSERT INTO %s VALUES %s", tableName, values);

    // "active" should have typed_value with boolean type
    GroupType active = field("active", shreddedPrimitive(PrimitiveType.PrimitiveTypeName.BOOLEAN));
    GroupType address = variant("address", 2, Type.Repetition.REQUIRED, objectFields(active));
    MessageType expectedSchema = parquetSchema(address);

    Table table = validationCatalog.loadTable(tableIdent);
    verifyParquetSchema(table, expectedSchema);

    // Reset field limit to default to avoid interference from previous tests
    spark.conf().unset(SparkSQLProperties.VARIANT_MAX_SHREDDED_FIELDS);
  }

  /**
   * Test Heuristic 5: Mixed Fields (Consistent and Inconsistent)
   *
   * <p>Tests a realistic scenario with multiple fields where some have consistent types and others
   * don't.
   */
  @TestTemplate
  public void testMixedFieldsConsistentAndInconsistent() throws IOException {
    spark.conf().set(SparkSQLProperties.SHRED_VARIANTS, "true");

    // "name": always string (consistent)
    // "age": mixed int/string (inconsistent)
    // "active": boolean (consistent)
    String values =
        "(1, parse_json('{\"name\": \"Alice\", \"age\": 30, \"active\": true}')),"
            + " (2, parse_json('{\"name\": \"Bob\", \"age\": \"25\", \"active\": false}')),"
            + " (3, parse_json('{\"name\": \"Charlie\", \"age\": \"35\", \"active\": true}'))";
    sql("INSERT INTO %s VALUES %s", tableName, values);

    // "name" should have typed_value (consistent string)
    GroupType name =
        field(
            "name",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()));

    // "age" should NOT have typed_value (inconsistent types)
    GroupType age = valueOnlyField("age");

    // "active" should have typed_value (consistent boolean)
    GroupType active = field("active", shreddedPrimitive(PrimitiveType.PrimitiveTypeName.BOOLEAN));

    GroupType address = variant("address", 2, Type.Repetition.REQUIRED, objectFields(active, age, name));
    MessageType expectedSchema = parquetSchema(address);

    Table table = validationCatalog.loadTable(tableIdent);
    verifyParquetSchema(table, expectedSchema);
  }

  /**
   * Test Heuristic 6: Field Limit Enforcement
   *
   * <p>Verify that the analyzer respects the maximum field limit and stops adding fields once the
   * limit is reached.
   */
  @TestTemplate
  public void testMaxFieldLimitEnforcement() throws IOException {
    spark.conf().set(SparkSQLProperties.SHRED_VARIANTS, "true");
    // Set very low field limit
    spark.conf().set(SparkSQLProperties.VARIANT_MAX_SHREDDED_FIELDS, "4");

    // Create rows with many fields (a, b, c, d, e, f)
    String values =
        "(1, parse_json('{\"a\": 1, \"b\": 2, \"c\": 3, \"d\": 4, \"e\": 5, \"f\": 6}')),"
            + " (2, parse_json('{\"a\": 1, \"b\": 2, \"c\": 3, \"d\": 4, \"e\": 5, \"f\": 6}'))";
    sql("INSERT INTO %s VALUES %s", tableName, values);

    // With limit 4: field "a" (2 fields: value + typed_value) + field "b" (2 fields) = 4 total
    // Fields are added alphabetically, so only "a" and "b" should be present
    GroupType a =
        field("a", shreddedPrimitive(PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(8, true)));
    GroupType b =
        field("b", shreddedPrimitive(PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(8, true)));

    GroupType address = variant("address", 2, Type.Repetition.REQUIRED, objectFields(a, b));
    MessageType expectedSchema = parquetSchema(address);

    Table table = validationCatalog.loadTable(tableIdent);
    verifyParquetSchema(table, expectedSchema);

    // Reset field limit to default to avoid interference from previous tests
    spark.conf().unset(SparkSQLProperties.VARIANT_MAX_SHREDDED_FIELDS);
  }

  /**
   * Test Heuristic 7: Decimal Type Handling - Inconsistent Scales
   *
   * <p>Verify that decimal fields with different scales are treated as inconsistent types
   * and only get a value field (no typed_value).
   */
  @TestTemplate
  public void testDecimalTypeHandlingInconsistentScales() throws IOException {
    spark.conf().set(SparkSQLProperties.SHRED_VARIANTS, "true");

    // Decimal values with different scales: scale 6, 2, 2
    // 123.456789 → precision 9, scale 6
    // 678.90 → precision 5, scale 2
    // 999.99 → precision 5, scale 2
    // These are treated as inconsistent types due to different scales
    String values =
        "(1, parse_json('{\"price\": 123.456789}')),"
            + " (2, parse_json('{\"price\": 678.90}')),"
            + " (3, parse_json('{\"price\": 999.99}'))";
    sql("INSERT INTO %s VALUES %s", tableName, values);

    // "price" has inconsistent scales, so only "value" field (no typed_value)
    GroupType price = valueOnlyField("price");
    GroupType address = variant("address", 2, Type.Repetition.REQUIRED, objectFields(price));
    MessageType expectedSchema = parquetSchema(address);

    Table table = validationCatalog.loadTable(tableIdent);
    verifyParquetSchema(table, expectedSchema);
  }

  /**
   * Test Heuristic 7b: Decimal Type Handling - Consistent Scales
   *
   * <p>Verify that decimal fields with the same scale get proper typed_value with inferred
   * precision/scale.
   */
  @TestTemplate
  public void testDecimalTypeHandlingConsistentScales() throws IOException {
    spark.conf().set(SparkSQLProperties.SHRED_VARIANTS, "true");

    // Decimal values with consistent scale (all 2 decimal places)
    String values =
        "(1, parse_json('{\"price\": 123.45}')),"
            + " (2, parse_json('{\"price\": 678.90}')),"
            + " (3, parse_json('{\"price\": 999.99}'))";
    sql("INSERT INTO %s VALUES %s", tableName, values);

    // "price" should have typed_value with inferred DECIMAL(5,2) type
    GroupType price =
        field(
            "price",
            org.apache.parquet.schema.Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                .as(LogicalTypeAnnotation.decimalType(2, 5))
                .named("typed_value"));
    GroupType address = variant("address", 2, Type.Repetition.REQUIRED, objectFields(price));
    MessageType expectedSchema = parquetSchema(address);

    Table table = validationCatalog.loadTable(tableIdent);
    verifyParquetSchema(table, expectedSchema);
  }

  /**
   * Test Heuristic 7c: Decimal Type Handling - Inconsistent After Buffering
   *
   * <p>Verify that when buffered rows have consistent decimal scales but subsequent unbuffered rows
   * have inconsistent scales, the inconsistent values are written to the value field only.
   * The schema is inferred from buffered rows and should include typed_value for the consistent type.
   */
  @TestTemplate
  public void testDecimalTypeHandlingInconsistentAfterBuffering() throws IOException {
    spark.conf().set(SparkSQLProperties.SHRED_VARIANTS, "true");
    // Set a small buffer size to test the scenario
    spark.conf().set(SparkSQLProperties.VARIANT_INFERENCE_BUFFER_SIZE, "3");

    // First 3 rows (buffered): consistent scale (2 decimal places)
    // 4th row onwards (unbuffered): different scale (6 decimal places)
    // Schema should be inferred from buffered rows with DECIMAL(5,2)
    // The unbuffered row with different scale should still write successfully to value field
    String values =
        "(1, parse_json('{\"price\": 123.45}')),"
            + " (2, parse_json('{\"price\": 678.90}')),"
            + " (3, parse_json('{\"price\": 999.99}')),"
            + " (4, parse_json('{\"price\": 111.111111}'))"; // Different scale - should write to value only
    sql("INSERT INTO %s VALUES %s", tableName, values);

    // Schema should have typed_value with DECIMAL(5,2) based on buffered rows
    GroupType price =
        field(
            "price",
            org.apache.parquet.schema.Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                .as(LogicalTypeAnnotation.decimalType(2, 5))
                .named("typed_value"));
    GroupType address = variant("address", 2, Type.Repetition.REQUIRED, objectFields(price));
    MessageType expectedSchema = parquetSchema(address);

    Table table = validationCatalog.loadTable(tableIdent);
    verifyParquetSchema(table, expectedSchema);

    // Verify all rows were written successfully
    List<Object[]> result = sql("SELECT id, address FROM %s ORDER BY id", tableName);
    assertThat(result).hasSize(4);

    // Reset buffer size to default
    spark.conf().unset(SparkSQLProperties.VARIANT_INFERENCE_BUFFER_SIZE);
  }

  /**
   * Test Heuristic 8: Array Type Handling
   *
   * <p>Verify that array fields with consistent element types get proper typed_value.
   */
  @TestTemplate
  public void testArrayTypeHandling() throws IOException {
    spark.conf().set(SparkSQLProperties.SHRED_VARIANTS, "true");

    // Arrays with consistent element types (all strings)
    String values =
        "(1, parse_json('{\"tags\": [\"java\", \"scala\", \"python\"]}')),"
            + " (2, parse_json('{\"tags\": [\"rust\", \"go\"]}')),"
            + " (3, parse_json('{\"tags\": [\"javascript\"]}'))";
    sql("INSERT INTO %s VALUES %s", tableName, values);

    // "tags" should have typed_value with list of strings
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

    Table table = validationCatalog.loadTable(tableIdent);
    verifyParquetSchema(table, expectedSchema);
  }

  /**
   * Test Heuristic 9: Nested Object Handling
   *
   * <p>Verify that simple nested objects are recursively shredded.
   */
  @TestTemplate
  public void testNestedObjectHandling() throws IOException {
    spark.conf().set(SparkSQLProperties.SHRED_VARIANTS, "true");

    // Nested objects with consistent structure
    String values =
        "(1, parse_json('{\"location\": {\"city\": \"Seattle\", \"zip\": 98101}}')),"
            + " (2, parse_json('{\"location\": {\"city\": \"Portland\", \"zip\": 97201}}')),"
            + " (3, parse_json('{\"location\": {\"city\": \"NYC\", \"zip\": 10001}}'))";
    sql("INSERT INTO %s VALUES %s", tableName, values);

    // Nested "location" object should be shredded with its fields
    GroupType city =
        field(
            "city",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()));
    GroupType zip =
        field("zip", shreddedPrimitive(PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(32, true)));
    GroupType location = field("location", objectFields(zip, city));

    GroupType address = variant("address", 2, Type.Repetition.REQUIRED, objectFields(location));
    MessageType expectedSchema = parquetSchema(address);

    Table table = validationCatalog.loadTable(tableIdent);
    verifyParquetSchema(table, expectedSchema);
  }

  /** Helper method to create a value-only field (no typed_value) for inconsistent types. */
  private static GroupType valueOnlyField(String name) {
    return org.apache.parquet.schema.Types.buildGroup(Type.Repetition.REQUIRED)
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("value")
        .named(name);
  }

  private void verifyParquetSchema(Table table, MessageType expectedSchema) throws IOException {
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

  private static MessageType parquetSchema(Type... variantTypes) {
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
    return org.apache.parquet.schema.Types.optional(primitive).named("typed_value");
  }

  private static Type shreddedPrimitive(
      PrimitiveType.PrimitiveTypeName primitive, LogicalTypeAnnotation annotation) {
    return org.apache.parquet.schema.Types.optional(primitive).as(annotation).named("typed_value");
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
}
