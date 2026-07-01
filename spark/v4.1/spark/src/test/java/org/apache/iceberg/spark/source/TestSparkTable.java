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
package org.apache.iceberg.spark.source;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.CatalogManager;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.constraints.Constraint;
import org.apache.spark.sql.connector.catalog.constraints.PrimaryKey;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSparkTable extends CatalogTestBase {
  private static final String LEGACY_SIGNED_UUID_FILE_NAME = "legacy-signed-uuid.parquet";
  private static final String UNSIGNED_UUID_FILE_NAME = "unsigned-uuid.parquet";
  private static final String ID_BOUNDS_FILE_NAME = "id-bounds.parquet";

  private static final int ID_FIELD_ID = 1;
  private static final int UUID_FIELD_ID = 2;

  private static final UUID UUID_00 = UUID.fromString("00000000-0000-0000-0000-000000000001");
  private static final UUID UUID_40 = UUID.fromString("40000000-0000-0000-0000-000000000001");
  private static final UUID UUID_7F = UUID.fromString("7fffffff-ffff-ffff-ffff-ffffffffffff");
  private static final UUID UUID_80 = UUID.fromString("80000000-0000-0000-0000-000000000001");
  private static final UUID UUID_FF = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff");

  private static final Schema UUID_SCHEMA =
      new Schema(
          required(ID_FIELD_ID, "id", Types.IntegerType.get()),
          required(UUID_FIELD_ID, "uuid_col", Types.UUIDType.get()));

  private static final PartitionSpec UUID_SPEC = PartitionSpec.unpartitioned();
  private static final PartitionSpec UUID_PARTITION_SPEC =
      PartitionSpec.builderFor(UUID_SCHEMA).identity("uuid_col").build();

  @BeforeEach
  public void createTable() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, name string NOT NULL, data string) USING iceberg",
        tableName);
  }

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testTableEquality() {
    SparkTable table1 = loadSparkTable();
    SparkTable table2 = loadSparkTable();

    // different instances pointing to the same table must be equivalent
    assertThat(table1).as("References must be different").isNotSameAs(table2);
    assertThat(table1).as("Tables must be equivalent").isEqualTo(table2);
  }

  @TestTemplate
  public void testNoIdentifierFieldsRelyByDefault() {
    SparkTable sparkTable = loadSparkTable();
    assertThat(primaryKeys(sparkTable)).isEmpty();

    // enabling rely without identifier fields still produces no primary key
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s' = 'true')",
        tableName, TableProperties.IDENTIFIER_FIELDS_RELY);
    sparkTable = loadSparkTable();
    assertThat(primaryKeys(sparkTable)).isEmpty();
  }

  @TestTemplate
  public void testIdentifierFieldsRelyViaTableProperty() {
    SparkTable sparkTable = loadSparkTable();
    sparkTable
        .table()
        .updateSchema()
        .allowIncompatibleChanges()
        .setIdentifierFields("id", "name")
        .commit();

    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s' = 'true')",
        tableName, TableProperties.IDENTIFIER_FIELDS_RELY);

    sparkTable = loadSparkTable();
    List<PrimaryKey> pks = primaryKeys(sparkTable);
    assertThat(pks).hasSize(1);

    PrimaryKey pk = pks.get(0);
    assertThat(pk.name()).isEqualTo("iceberg_pk");
    assertThat(pk.enforced()).isFalse();
    assertThat(pk.rely()).isTrue();
    assertThat(pk.validationStatus()).isEqualTo(Constraint.ValidationStatus.UNVALIDATED);

    Set<String> columnNames =
        Arrays.stream(pk.columns()).map(NamedReference::toString).collect(Collectors.toSet());
    assertThat(columnNames).containsExactlyInAnyOrder("id", "name");

    // disabling rely removes the primary key
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s' = 'false')",
        tableName, TableProperties.IDENTIFIER_FIELDS_RELY);
    sparkTable = loadSparkTable();
    assertThat(primaryKeys(sparkTable)).isEmpty();
  }

  @TestTemplate
  public void testIdentifierFieldsRelyViaSessionConf() {
    SparkTable sparkTable = loadSparkTable();
    sparkTable.table().updateSchema().allowIncompatibleChanges().setIdentifierFields("id").commit();

    // session conf enables rely without a table property
    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.IDENTIFIER_FIELDS_RELY, "true"),
        () -> {
          List<PrimaryKey> pks = primaryKeys(loadSparkTable());
          assertThat(pks).hasSize(1);

          Set<String> columnNames =
              Arrays.stream(pks.get(0).columns())
                  .map(NamedReference::toString)
                  .collect(Collectors.toSet());
          assertThat(columnNames).containsExactly("id");
        });

    // session conf rely=false overrides table property rely=true
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s' = 'true')",
        tableName, TableProperties.IDENTIFIER_FIELDS_RELY);
    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.IDENTIFIER_FIELDS_RELY, "false"),
        () -> assertThat(primaryKeys(loadSparkTable())).isEmpty());
  }

  @TestTemplate
  public void uuidPredicatesCannotUseMetadataDeleteButNonUuidStillCan() throws Exception {
    Table table =
        createUuidTableWithFile(
            LEGACY_SIGNED_UUID_FILE_NAME, UUID_FIELD_ID, Types.UUIDType.get(), UUID_80, UUID_40);
    Predicate uuidPredicate = predicate("<=", "uuid_col", uuidLiteral(UUID_40));
    assertCanDeleteWhere(
        table, uuidPredicate, false, "Legacy signed UUID bounds must not be trusted");

    table =
        createUuidTableWithFile(
            UNSIGNED_UUID_FILE_NAME, UUID_FIELD_ID, Types.UUIDType.get(), UUID_00, UUID_FF);
    uuidPredicate = predicate("<=", "uuid_col", uuidLiteral(UUID_7F));
    assertCanDeleteWhere(
        table, uuidPredicate, false, "Current UUID bounds should stay conservative for deletes");

    table = createUuidTable(UUID_PARTITION_SPEC);
    uuidPredicate = predicate("=", "uuid_col", uuidLiteral(UUID_40));
    assertCanDeleteWhere(
        table, uuidPredicate, false, "UUID predicates should not use partition metadata deletes");

    table =
        createUuidTableWithFile(ID_BOUNDS_FILE_NAME, ID_FIELD_ID, Types.IntegerType.get(), 1, 4);
    Predicate idPredicate = predicate("<=", "id", new LiteralValue<>(4, DataTypes.IntegerType));
    assertCanDeleteWhere(
        table, idPredicate, true, "Non-UUID metadata delete should keep using strict metrics");
  }

  private static List<PrimaryKey> primaryKeys(SparkTable table) {
    return Arrays.stream(table.constraints())
        .filter(c -> c instanceof PrimaryKey)
        .map(c -> (PrimaryKey) c)
        .collect(Collectors.toList());
  }

  private Table createUuidTable(PartitionSpec spec) {
    sql("DROP TABLE IF EXISTS %s", tableName);
    return validationCatalog.createTable(tableIdent, UUID_SCHEMA, spec);
  }

  private <T> Table createUuidTableWithFile(
      String fileName, int fieldId, Type.PrimitiveType type, T lower, T upper) {
    Table table = createUuidTable(UUID_SPEC);
    DataFile file = dataFile(table.location() + "/data/" + fileName, fieldId, type, lower, upper);
    table.newFastAppend().appendFile(file).commit();
    table.refresh();
    return table;
  }

  private static SparkTable sparkTable(Table table)
      throws NoSuchMethodException,
          InvocationTargetException,
          InstantiationException,
          IllegalAccessException {
    try {
      Constructor<SparkTable> constructor = SparkTable.class.getConstructor(Table.class);
      return constructor.newInstance(table);
    } catch (NoSuchMethodException e) {
      Constructor<SparkTable> constructor =
          SparkTable.class.getConstructor(Table.class, boolean.class);
      return constructor.newInstance(table, false);
    }
  }

  private static void assertCanDeleteWhere(
      Table table, Predicate predicate, boolean expected, String description) throws Exception {
    assertThat(sparkTable(table).canDeleteWhere(new Predicate[] {predicate}))
        .as(description)
        .isEqualTo(expected);
  }

  private static <T> DataFile dataFile(
      String path, int fieldId, Type.PrimitiveType type, T lower, T upper) {
    return DataFiles.builder(UUID_SPEC)
        .withPath(path)
        .withFileSizeInBytes(10)
        .withMetrics(
            new Metrics(
                4L,
                null,
                ImmutableMap.of(fieldId, 4L),
                ImmutableMap.of(fieldId, 0L),
                null,
                ImmutableMap.of(fieldId, toBuffer(type, lower)),
                ImmutableMap.of(fieldId, toBuffer(type, upper))))
        .build();
  }

  private static <T> ByteBuffer toBuffer(Type.PrimitiveType type, T value) {
    return Conversions.toByteBuffer(type, value);
  }

  private static LiteralValue uuidLiteral(UUID uuid) {
    return new LiteralValue<>(UTF8String.fromString(uuid.toString()), DataTypes.StringType);
  }

  private static Predicate predicate(
      String name, String fieldName, org.apache.spark.sql.connector.expressions.Expression value) {
    org.apache.spark.sql.connector.expressions.Expression[] children =
        new org.apache.spark.sql.connector.expressions.Expression[] {
          FieldReference.apply(fieldName), value
        };
    return new Predicate(name, children);
  }

  private SparkTable loadSparkTable() {
    try {
      CatalogManager catalogManager = spark.sessionState().catalogManager();
      TableCatalog catalog = (TableCatalog) catalogManager.catalog(catalogName);
      Identifier identifier = Identifier.of(tableIdent.namespace().levels(), tableIdent.name());
      return (SparkTable) catalog.loadTable(identifier);
    } catch (NoSuchTableException e) {
      throw new RuntimeException(e);
    }
  }
}
