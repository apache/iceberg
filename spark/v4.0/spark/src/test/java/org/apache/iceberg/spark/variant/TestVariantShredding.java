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
    GroupType address = variant("address", 2, objectFields(name, streets, zip));
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
    GroupType address = variant("address", 2, objectFields(city, state));
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
    GroupType address = variant("address", 2, objectFields(city, zip));

    GroupType type =
        field(
            "type",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()));
    GroupType verified =
        field("verified", shreddedPrimitive(PrimitiveType.PrimitiveTypeName.BOOLEAN));
    GroupType metadata = variant("metadata", 3, objectFields(type, verified));

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
    GroupType address = variant("address", 2, objectFields(street));

    GroupType label =
        field(
            "label",
            shreddedPrimitive(
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()));
    GroupType metadata = variant("metadata", 3, objectFields(label));

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

    GroupType address = variant("address", 2);
    MessageType expectedSchema = parquetSchema(address);

    Table table = validationCatalog.loadTable(tableIdent);
    verifyParquetSchema(table, expectedSchema);
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

  private static GroupType variant(String name, int fieldId) {
    return org.apache.parquet.schema.Types.buildGroup(Type.Repetition.OPTIONAL)
        .id(fieldId)
        .as(LogicalTypeAnnotation.variantType(Variant.VARIANT_SPEC_VERSION))
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("metadata")
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("value")
        .named(name);
  }

  private static GroupType variant(String name, int fieldId, Type shreddedType) {
    checkShreddedType(shreddedType);
    return org.apache.parquet.schema.Types.buildGroup(Type.Repetition.OPTIONAL)
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
