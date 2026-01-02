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
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.List;
import org.apache.iceberg.spark.udf.SqlFunctionCatalog;
import org.apache.iceberg.spark.udf.SqlFunctionSpec;
import org.apache.iceberg.spark.udf.SqlUdfCatalogFunction;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestSqlFunctionAnalyzerRewrite extends ExtensionsTestBase {

  public static class TestFunctionCatalog
      implements CatalogPlugin, SqlFunctionCatalog, FunctionCatalog, SupportsNamespaces {
    private String name;

    @Override
    public void initialize(String catalogName, CaseInsensitiveStringMap options) {
      this.name = catalogName;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Identifier[] listFunctions(String[] namespace) {
      return new Identifier[] {Identifier.of(new String[] {}, "add_one")};
    }

    @Override
    public UnboundFunction loadFunction(Identifier ident) throws NoSuchFunctionException {
      SqlFunctionSpec spec = loadSqlFunction(ident);
      return new SqlUdfCatalogFunction(name(), ident, spec);
    }

    @Override
    public SqlFunctionSpec loadSqlFunction(Identifier ident) throws NoSuchFunctionException {
      if (!"add_one".equalsIgnoreCase(ident.name())) {
        throw new NoSuchFunctionException(ident);
      }
      List<SqlFunctionSpec.Parameter> params =
          Collections.singletonList(new SqlFunctionSpec.Parameter("x", "int", null));
      return new SqlFunctionSpec(params, "int", "spark", "x + 1", true);
    }

    // SupportsNamespaces minimal implementation
    @Override
    public String[][] listNamespaces() {
      return new String[][] {new String[] {"default"}};
    }

    @Override
    public String[][] listNamespaces(String[] namespace) {
      return new String[0][];
    }

    @Override
    public java.util.Map<String, String> loadNamespaceMetadata(String[] namespace) {
      return Collections.emptyMap();
    }

    @Override
    public void createNamespace(String[] namespace, java.util.Map<String, String> metadata) {
      // no-op for tests
    }

    // Spark 4.0 signature includes cascade flag
    @Override
    public boolean dropNamespace(String[] namespace, boolean cascade) {
      return true;
    }

    @Override
    public void alterNamespace(
        String[] namespace, org.apache.spark.sql.connector.catalog.NamespaceChange... changes) {
      // no-op for tests
    }

    @Override
    public boolean namespaceExists(String[] namespace) {
      return namespace == null
          || namespace.length == 0
          || (namespace.length == 1 && "default".equalsIgnoreCase(namespace[0]));
    }
  }

  @BeforeEach
  public void setupDefaultCatalog() {
    spark.conf().set("spark.sql.catalog.test", TestFunctionCatalog.class.getName());
    spark.conf().set("spark.sql.defaultCatalog", "test");
  }

  @TestTemplate
  public void testScalarRewriteWithLiteral() {
    Object result = scalarSql("SELECT add_one(41)");
    assertThat(((Number) result).intValue()).isEqualTo(42);
  }

  @TestTemplate
  public void testScalarRewriteWithColumn() {
    Object result = scalarSql("SELECT sum(add_one(id)) FROM range(3)");
    assertThat(((Number) result).longValue()).isEqualTo(6L);
  }

  @TestTemplate
  public void testArityMismatch() {
    assertThatThrownBy(() -> sql("SELECT add_one()"))
        .hasMessageContaining("Invalid bound function")
        .hasMessageContaining("arguments");
    assertThatThrownBy(() -> sql("SELECT add_one(1, 2)"))
        .hasMessageContaining("Invalid bound function")
        .hasMessageContaining("arguments");
  }
}
