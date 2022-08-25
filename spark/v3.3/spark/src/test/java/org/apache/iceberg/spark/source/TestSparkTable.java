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

import java.util.Map;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.CatalogManager;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestSparkTable extends SparkCatalogTestBase {

  public TestSparkTable(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Before
  public void createTable() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
  }

  @After
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testTableEquality() throws NoSuchTableException {
    CatalogManager catalogManager = spark.sessionState().catalogManager();
    TableCatalog catalog = (TableCatalog) catalogManager.catalog(catalogName);
    Identifier identifier = Identifier.of(tableIdent.namespace().levels(), tableIdent.name());
    SparkTable table1 = (SparkTable) catalog.loadTable(identifier);
    SparkTable table2 = (SparkTable) catalog.loadTable(identifier);

    // different instances pointing to the same table must be equivalent
    Assert.assertNotSame("References must be different", table1, table2);
    Assert.assertEquals("Tables must be equivalent", table1, table2);
  }

  @Test
  public void testTableName() {
    CatalogManager catalogManager = spark.sessionState().catalogManager();
    TableCatalog catalog = (TableCatalog) catalogManager.catalog(catalogName);

    for (String fileFormat : Lists.newArrayList("parquet", "orc", "avro")) {
      for (String tableFormatVersion : Lists.newArrayList("1", "2")) {
        String testTableName =
            String.format("table_name_test_%s_v%s", fileFormat, tableFormatVersion);
        withTables(
            () -> {
              sql(
                  String.format(
                      "CREATE TABLE %s USING iceberg TBLPROPERTIES ('%s'='%s', '%s'='%s')",
                      tableName(testTableName),
                      TableProperties.DEFAULT_FILE_FORMAT,
                      fileFormat,
                      TableProperties.FORMAT_VERSION,
                      tableFormatVersion));

              TableIdentifier icebergIdentifier =
                  TableIdentifier.of(Namespace.of("default"), testTableName);
              Identifier sparkIdentifier =
                  Identifier.of(icebergIdentifier.namespace().levels(), icebergIdentifier.name());

              try {
                // Catalog#loadTable throw NoSuchTableException, however, the Action#invoke
                // does not expect any exceptions to be thrown, thus it wrapped inside
                // RuntimeException.
                String actualTableName = catalog.loadTable(sparkIdentifier).name();
                String expectedTableName =
                    String.format(
                        "iceberg/%s/v%s %s.%s",
                        fileFormat, tableFormatVersion, catalogName, icebergIdentifier);
                Assert.assertEquals(
                    "Table name mismatched for (%s file format, %s table format version)",
                    expectedTableName, actualTableName);
              } catch (NoSuchTableException e) {
                throw new RuntimeException(e);
              }
            },
            tableName(testTableName));
      }
    }
  }
}
