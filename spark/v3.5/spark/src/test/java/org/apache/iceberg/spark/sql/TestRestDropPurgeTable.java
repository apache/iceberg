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
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
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

/**
 * Tests for the REST catalog purge delegation feature.
 *
 * <p>This test verifies that when {@link org.apache.iceberg.CatalogProperties#REST_CATALOG_PURGE}
 * is enabled, Spark delegates DROP TABLE PURGE operations to the REST catalog instead of performing
 * client-side file deletion. This allows REST catalogs to implement features like UNDROP by
 * preserving table metadata.
 *
 * <p>The test uses a mock REST catalog that verifies the purge flag is correctly passed through
 * based on the configuration.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestRestDropPurgeTable extends TestBase {

  private SparkCatalog sparkCatalog;
  private static final Identifier TABLE_ID = Identifier.of(new String[] {"ns"}, "test_table");

  @Parameter(index = 0)
  private boolean delegatePurge;

  @Parameters(name = "delegatePurge = {0}")
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
                MockRESTCatalog.class.getName(),
                REST_CATALOG_PURGE,
                Boolean.toString(delegatePurge),
                MockRESTCatalog.EXPECTED_PURGE_FLAG,
                Boolean.toString(delegatePurge))));
    try {
      sparkCatalog.createNamespace(new String[] {"ns"}, ImmutableMap.of());
      sparkCatalog.createTable(
          TABLE_ID,
          new Column[] {Column.create("id", DataTypes.LongType)},
          null,
          ImmutableMap.of());

    } catch (TableAlreadyExistsException
        | NoSuchNamespaceException
        | NamespaceAlreadyExistsException e) {
      throw new RuntimeException(e);
    }
  }

  @TestTemplate
  public void testPurgeTableDelegation() {
    // When purgeTable is called, it should pass the purge flag to the catalog
    // based on the REST_CATALOG_PURGE configuration
    boolean dropped = sparkCatalog.purgeTable(TABLE_ID);

    // Verify the table was dropped successfully
    assertThat(dropped).as("Table should be dropped successfully").isTrue();

    // The MockRESTCatalog verifies internally that the purge flag matches expectations
  }

  /**
   * Mock REST catalog that verifies the purge flag is correctly passed.
   *
   * <p>This catalog validates that when REST_CATALOG_PURGE is enabled, the purge flag is true, and
   * when disabled, Spark handles deletion client-side (dropTable is called with purge=false).
   */
  public static class MockRESTCatalog extends RESTCatalog {
    private final InMemoryCatalog delegate;
    static final String EXPECTED_PURGE_FLAG = "expected-purge-flag";
    private boolean expectedPurgeFlag;

    public MockRESTCatalog() {
      this.delegate = new InMemoryCatalog();
      this.delegate.initialize("test", Maps.newHashMap());
    }

    @Override
    public void initialize(String name, Map<String, String> props) {
      this.expectedPurgeFlag = Boolean.parseBoolean(props.get(EXPECTED_PURGE_FLAG));
    }

    @Override
    public Table createTable(TableIdentifier identifier, org.apache.iceberg.Schema schema) {
      return delegate.createTable(identifier, schema);
    }

    @Override
    public void createNamespace(Namespace ns, Map<String, String> props) {
      delegate.createNamespace(ns, props);
    }

    @Override
    public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
      return delegate.buildTable(identifier, schema);
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
      // Verify the purge flag matches our expectation
      assertThat(purge)
          .as(
              "When REST_CATALOG_PURGE=%s, dropTable should be called with purge=%s",
              expectedPurgeFlag, expectedPurgeFlag)
          .isEqualTo(expectedPurgeFlag);
      return true;
    }

    @Override
    public Table loadTable(TableIdentifier identifier) {
      return delegate.loadTable(identifier);
    }
  }
}
