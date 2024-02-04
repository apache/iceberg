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
package org.apache.iceberg.spark;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestSparkCatalogOperations extends CatalogTestBase {

  @BeforeEach
  public void createTable() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
  }

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testAlterTable() throws NoSuchTableException {
    BaseCatalog catalog = (BaseCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    Identifier identifier = Identifier.of(tableIdent.namespace().levels(), tableIdent.name());

    String fieldName = "location";
    String propsKey = "note";
    String propsValue = "jazz";
    Table table =
        catalog.alterTable(
            identifier,
            TableChange.addColumn(new String[] {fieldName}, DataTypes.StringType, true),
            TableChange.setProperty(propsKey, propsValue));

    assertThat(table).as("Should return updated table").isNotNull();

    StructField expectedField = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
    assertThat(table.schema().fields()[2])
        .as("Adding a column to a table should return the updated table with the new column")
        .isEqualTo(expectedField);

    assertThat(table.properties())
        .as(
            "Adding a property to a table should return the updated table with the new property with the new correct value")
        .containsEntry(propsKey, propsValue);
  }

  @TestTemplate
  public void testInvalidateTable() {
    // load table to CachingCatalog
    sql("SELECT count(1) FROM %s", tableName);

    // recreate table from another catalog or program
    Catalog anotherCatalog = validationCatalog;
    Schema schema = anotherCatalog.loadTable(tableIdent).schema();
    anotherCatalog.dropTable(tableIdent);
    anotherCatalog.createTable(tableIdent, schema);

    // invalidate and reload table
    sql("REFRESH TABLE %s", tableName);
    sql("SELECT count(1) FROM %s", tableName);
  }
}
