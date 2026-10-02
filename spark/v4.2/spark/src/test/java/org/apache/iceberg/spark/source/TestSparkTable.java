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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.SparkTableProperties;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.CatalogManager;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.constraints.Constraint;
import org.apache.spark.sql.connector.catalog.constraints.PrimaryKey;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.VariantType$;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSparkTable extends CatalogTestBase {

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
  public void testSupportedSchemaEvolutionChanges() {
    SparkTable table = loadSparkTable();

    assertThat(table.capabilities()).contains(TableCapability.AUTOMATIC_SCHEMA_EVOLUTION);
    assertThat(
            table.supportsColumnChange(
                (TableChange.ColumnChange)
                    TableChange.addColumn(new String[] {"new_col"}, DataTypes.IntegerType, true)))
        .isTrue();
    assertThat(
            table.supportsColumnChange(
                (TableChange.ColumnChange)
                    TableChange.addColumn(new String[] {"new_col"}, DataTypes.IntegerType, false)))
        .isFalse();

    assertThat(
            table.supportsColumnChange(
                (TableChange.ColumnChange)
                    TableChange.updateColumnType(new String[] {"id"}, DataTypes.LongType)))
        .isTrue();
    assertThat(
            table.supportsColumnChange(
                (TableChange.ColumnChange)
                    TableChange.updateColumnType(new String[] {"id"}, DataTypes.IntegerType)))
        .isFalse();

    assertThat(
            table.supportsColumnChange(
                (TableChange.ColumnChange)
                    TableChange.updateColumnNullability(new String[] {"data"}, true)))
        .isTrue();
    assertThat(
            table.supportsColumnChange(
                (TableChange.ColumnChange)
                    TableChange.updateColumnNullability(new String[] {"data"}, false)))
        .isFalse();
    assertThat(
            table.supportsColumnChange(
                (TableChange.ColumnChange)
                    TableChange.updateColumnDefaultValue(new String[] {"data"}, "'default'")))
        .isFalse();
  }

  @TestTemplate
  void addColumnsRespectFormatVersion() {
    List<DataType> types =
        Arrays.asList(
            VariantType$.MODULE$,
            DataTypes.NullType,
            new StructType().add("v", VariantType$.MODULE$),
            DataTypes.createMapType(DataTypes.StringType, VariantType$.MODULE$),
            DataTypes.createArrayType(VariantType$.MODULE$));

    for (int formatVersion : new int[] {2, 3}) {
      sql(
          "ALTER TABLE %s SET TBLPROPERTIES ('%s' = '%s')",
          tableName, TableProperties.FORMAT_VERSION, formatVersion);
      SparkTable table = loadSparkTable();

      for (DataType type : types) {
        assertThat(
                table.supportsColumnChange(
                    (TableChange.ColumnChange)
                        TableChange.addColumn(new String[] {"new_col"}, type, true)))
            .as("Adding %s to a v%s table", type, formatVersion)
            .isEqualTo(formatVersion >= 3);
      }
    }
  }

  @TestTemplate
  void variantTypeUpdatesAreNotSupported() {
    for (int formatVersion : new int[] {2, 3}) {
      sql(
          "ALTER TABLE %s SET TBLPROPERTIES ('%s' = '%s')",
          tableName, TableProperties.FORMAT_VERSION, formatVersion);
      SparkTable table = loadSparkTable();

      assertThat(
              table.supportsColumnChange(
                  (TableChange.ColumnChange)
                      TableChange.updateColumnType(new String[] {"data"}, VariantType$.MODULE$)))
          .as("Updating a column to variant in a v%s table", formatVersion)
          .isFalse();
    }
  }

  @TestTemplate
  void identifierColumnCannotBecomeNullable() {
    SparkTable table = loadSparkTable();
    table.table().updateSchema().allowIncompatibleChanges().setIdentifierFields("id").commit();
    table = loadSparkTable();

    assertThat(
            table.supportsColumnChange(
                (TableChange.ColumnChange)
                    TableChange.updateColumnNullability(new String[] {"id"}, true)))
        .isFalse();
  }

  @TestTemplate
  void identifierStructCannotBecomeNullable() {
    SparkTable table = loadSparkTable();
    table
        .table()
        .updateSchema()
        .allowIncompatibleChanges()
        .addRequiredColumn(
            "parent",
            Types.StructType.of(Types.NestedField.required(1, "id", Types.LongType.get())))
        .setIdentifierFields("parent.id")
        .commit();
    table = loadSparkTable();

    assertThat(
            table.supportsColumnChange(
                (TableChange.ColumnChange)
                    TableChange.updateColumnNullability(new String[] {"parent"}, true)))
        .isFalse();
  }

  @TestTemplate
  void missingColumnCannotBecomeNullable() {
    SparkTable table = loadSparkTable();

    assertThat(
            table.supportsColumnChange(
                (TableChange.ColumnChange)
                    TableChange.updateColumnNullability(new String[] {"missing"}, true)))
        .isFalse();
  }

  @TestTemplate
  public void testMapKeySchemaEvolutionChanges() {
    sql("ALTER TABLE %s ADD COLUMN m map<int, string>", tableName);
    sql("ALTER TABLE %s ADD COLUMN value_map map<int, int>", tableName);
    sql("ALTER TABLE %s ADD COLUMN struct_key_map map<struct<key_field: int>, string>", tableName);
    sql("ALTER TABLE %s ADD COLUMN struct_value_map map<int, struct<value_field: int>>", tableName);
    SparkTable table = loadSparkTable();

    assertThat(
            table.supportsColumnChange(
                (TableChange.ColumnChange)
                    TableChange.updateColumnType(new String[] {"m", "key"}, DataTypes.LongType)))
        .isFalse();
    assertThat(
            table.supportsColumnChange(
                (TableChange.ColumnChange)
                    TableChange.updateColumnType(
                        new String[] {"struct_key_map", "key", "key_field"}, DataTypes.LongType)))
        .isFalse();
    assertThat(
            table.supportsColumnChange(
                (TableChange.ColumnChange)
                    TableChange.addColumn(
                        new String[] {"struct_key_map", "key", "new_field"},
                        DataTypes.IntegerType,
                        true)))
        .isFalse();

    assertThat(
            table.supportsColumnChange(
                (TableChange.ColumnChange)
                    TableChange.updateColumnType(
                        new String[] {"value_map", "value"}, DataTypes.LongType)))
        .isTrue();
    assertThat(
            table.supportsColumnChange(
                (TableChange.ColumnChange)
                    TableChange.addColumn(
                        new String[] {"struct_value_map", "value", "new_field"},
                        DataTypes.IntegerType,
                        true)))
        .isTrue();
  }

  @TestTemplate
  void typeUpdatesMustRoundTripToSparkType() {
    sql("ALTER TABLE %s ADD COLUMN int_col int", tableName);
    SparkTable table = loadSparkTable();

    assertThat(
            table.supportsColumnChange(
                (TableChange.ColumnChange)
                    TableChange.updateColumnType(new String[] {"int_col"}, DataTypes.ShortType)))
        .isFalse();
    assertThat(
            table.supportsColumnChange(
                (TableChange.ColumnChange)
                    TableChange.updateColumnType(new String[] {"int_col"}, DataTypes.ByteType)))
        .isFalse();
    assertThat(
            table.supportsColumnChange(
                (TableChange.ColumnChange)
                    TableChange.updateColumnType(new String[] {"int_col"}, DataTypes.LongType)))
        .isTrue();
  }

  @TestTemplate
  void schemaEvolutionWithAcceptAnySchema() {
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s' = 'true')",
        tableName, SparkTableProperties.WRITE_ACCEPT_ANY_SCHEMA);
    SparkTable table = loadSparkTable();
    TableChange.ColumnChange change =
        (TableChange.ColumnChange)
            TableChange.addColumn(new String[] {"new_col"}, DataTypes.IntegerType, true);

    assertThat(table.supportsColumnChange(change)).isTrue();
    assertThat(table.supportsColumnChange(change)).isTrue();
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

  private static List<PrimaryKey> primaryKeys(SparkTable table) {
    return Arrays.stream(table.constraints())
        .filter(c -> c instanceof PrimaryKey)
        .map(c -> (PrimaryKey) c)
        .collect(Collectors.toList());
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
