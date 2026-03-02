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
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.CatalogManager;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.constraints.Constraint;
import org.apache.spark.sql.connector.catalog.constraints.PrimaryKey;
import org.apache.spark.sql.connector.expressions.NamedReference;
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
        tableName, TableProperties.READ_IDENTIFIER_FIELDS_RELY);
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
        tableName, TableProperties.READ_IDENTIFIER_FIELDS_RELY);

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
        tableName, TableProperties.READ_IDENTIFIER_FIELDS_RELY);
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
        tableName, TableProperties.READ_IDENTIFIER_FIELDS_RELY);
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
