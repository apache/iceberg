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
package org.apache.iceberg.spark.sql;

import static org.apache.iceberg.CatalogProperties.CATALOG_IMPL;
import static org.apache.iceberg.CatalogProperties.REST_CATALOG_PURGE;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.TestBase;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRestDropPurgeTable extends TestBase {

  private SparkCatalog sparkCatalog;
  private static final Identifier id = Identifier.of(new String[] {"a"}, "db");

  @Parameter(index = 0)
  private boolean purge;

  @Parameters(name = "purge = {0}")
  protected static Object[][] parameters() {
    return new Object[][] {{true}, {false}};
  }

  @BeforeEach
  public void before() {
    sparkCatalog = new SparkCatalog();
    sparkCatalog.initialize(
        "test_rest_catalog_purge",
        new CaseInsensitiveStringMap(
            ImmutableMap.of(
                CATALOG_IMPL,
                MockCatalog.class.getName(),
                REST_CATALOG_PURGE,
                Boolean.toString(purge),
                MockCatalog.EXPECTATION,
                Boolean.toString(purge))));
    try {
      sparkCatalog.createNamespace(new String[] {"a"}, ImmutableMap.of());
      sparkCatalog.createTable(
          id, new Column[] {Column.create("a", DataTypes.FloatType)}, null, ImmutableMap.of());

    } catch (TableAlreadyExistsException
        | NoSuchNamespaceException
        | NamespaceAlreadyExistsException e) {
      throw new RuntimeException(e);
    }
  }

  @TestTemplate
  public void testDropPurgeTableForwardsPurge() {
    boolean dropped = sparkCatalog.purgeTable(id);
    // we assert that dropped is True to make sure the code path in purgeTable we're testing
    // actually ran
    assertThat(dropped).isTrue();
  }

  public static class MockCatalog extends RESTCatalog {
    private final InMemoryCatalog catalog;
    private static final String EXPECTATION = "expectation";
    private boolean expectation;

    public MockCatalog() {
      this.catalog = new InMemoryCatalog();
      this.catalog.initialize("test", new HashMap<>());
    }

    @Override
    public void initialize(String name, Map<String, String> props) {
      this.expectation = Boolean.parseBoolean(props.get(EXPECTATION));
    }

    @Override
    public Table createTable(TableIdentifier identifier, org.apache.iceberg.Schema schema) {
      return catalog.createTable(identifier, schema);
    }

    @Override
    public void createNamespace(Namespace ns, Map<String, String> props) {
      catalog.createNamespace(ns, props);
    }

    @Override
    public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
      return catalog.buildTable(identifier, schema);
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
      assertThat(purge).isEqualTo(expectation);
      return true;
    }

    @Override
    public Table loadTable(TableIdentifier identifier) {
      return catalog.loadTable(identifier);
    }
  }
}
