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
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.CatalogManager;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.LogicalWriteInfoImpl;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.StringEndsWith;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
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
    SparkTable table1 = loadTable();
    SparkTable table2 = loadTable();
    // different instances pointing to the same table must be equivalent
    Assert.assertNotSame("References must be different", table1, table2);
    Assert.assertEquals("Tables must be equivalent", table1, table2);
  }

  @Test
  public void testOverwriteFilterConversions() throws NoSuchTableException {
    SparkTable table = loadTable();
    Map<String, String> newOptions = Maps.newHashMap();
    CaseInsensitiveStringMap map = new CaseInsensitiveStringMap(newOptions);
    LogicalWriteInfo info = new LogicalWriteInfoImpl("", table.schema(), map);

    Filter[] filters1 = {EqualTo.apply("dummy", 1)};
    AssertHelpers.assertThrows(
        "Binding should fail",
        UnsupportedOperationException.class,
        "Cannot overwrite. Failed to bind expression to table schema",
        () -> ((SparkWriteBuilder) table.newWriteBuilder(info)).overwrite(filters1));

    Filter[] filters2 = {StringEndsWith.apply("data", "test")};
    AssertHelpers.assertThrows(
        "Binding should fail",
        UnsupportedOperationException.class,
        "Cannot overwrite. Failed convert filter to Iceberg expression",
        () -> ((SparkWriteBuilder) table.newWriteBuilder(info)).overwrite(filters2));
  }

  private SparkTable loadTable() throws NoSuchTableException {
    CatalogManager catalogManager = spark.sessionState().catalogManager();
    TableCatalog catalog = (TableCatalog) catalogManager.catalog(catalogName);
    Identifier identifier = Identifier.of(tableIdent.namespace().levels(), tableIdent.name());
    return (SparkTable) catalog.loadTable(identifier);
  }
}
