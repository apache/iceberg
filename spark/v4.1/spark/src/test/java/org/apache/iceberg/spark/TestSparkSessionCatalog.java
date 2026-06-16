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

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.ViewCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestSparkSessionCatalog extends TestBase {
  private final String envHmsUriKey = "spark.hadoop." + METASTOREURIS.varname;
  private final String catalogHmsUriKey = "spark.sql.catalog.spark_catalog.uri";
  private final String hmsUri = hiveConf.get(METASTOREURIS.varname);

  @BeforeAll
  public static void setUpCatalog() {
    spark
        .conf()
        .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
    spark.conf().set("spark.sql.catalog.spark_catalog.type", "hive");
  }

  @BeforeEach
  public void setupHmsUri() {
    spark.sessionState().catalogManager().reset();
    spark.conf().set(envHmsUriKey, hmsUri);
    spark.conf().set(catalogHmsUriKey, hmsUri);
  }

  @Test
  public void testValidateHmsUri() {
    // HMS uris match
    assertThat(spark.sessionState().catalogManager().v2SessionCatalog().defaultNamespace())
        .containsExactly("default");

    // HMS uris doesn't match
    spark.sessionState().catalogManager().reset();
    String catalogHmsUri = "RandomString";
    spark.conf().set(envHmsUriKey, hmsUri);
    spark.conf().set(catalogHmsUriKey, catalogHmsUri);

    assertThatThrownBy(() -> spark.sessionState().catalogManager().v2SessionCatalog())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            String.format(
                "Inconsistent Hive metastore URIs: %s (Spark session) != %s (spark_catalog)",
                hmsUri, catalogHmsUri));

    // no env HMS uri, only catalog HMS uri
    spark.sessionState().catalogManager().reset();
    spark.conf().set(catalogHmsUriKey, hmsUri);
    spark.conf().unset(envHmsUriKey);
    assertThat(spark.sessionState().catalogManager().v2SessionCatalog().defaultNamespace())
        .containsExactly("default");

    // no catalog HMS uri, only env HMS uri
    spark.sessionState().catalogManager().reset();
    spark.conf().set(envHmsUriKey, hmsUri);
    spark.conf().unset(catalogHmsUriKey);
    assertThat(spark.sessionState().catalogManager().v2SessionCatalog().defaultNamespace())
        .containsExactly("default");
  }

  @Test
  public void testLoadFunction() {
    String functionClass = "org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper";

    // load permanent UDF in Hive via FunctionCatalog
    spark.sql(String.format("CREATE FUNCTION perm_upper AS '%s'", functionClass));
    assertThat(scalarSql("SELECT perm_upper('xyz')"))
        .as("Load permanent UDF in Hive")
        .isEqualTo("XYZ");

    // load temporary UDF in Hive via FunctionCatalog
    spark.sql(String.format("CREATE TEMPORARY FUNCTION temp_upper AS '%s'", functionClass));
    assertThat(scalarSql("SELECT temp_upper('xyz')"))
        .as("Load temporary UDF in Hive")
        .isEqualTo("XYZ");

    // TODO: fix loading Iceberg built-in functions in SessionCatalog
  }

  @Test
  public void testListViewsDelegatesToSessionCatalog() {
    String viewName = "list_views_session_catalog";

    spark.sql("DROP VIEW IF EXISTS " + viewName);
    spark.sql("CREATE VIEW " + viewName + " AS SELECT 1 AS id");

    try {
      SparkSessionCatalog<?> catalog =
          (SparkSessionCatalog<?>) spark.sessionState().catalogManager().v2SessionCatalog();

      assertThat(catalog.listViews("default")).extracting(Identifier::name).contains(viewName);
    } finally {
      spark.sql("DROP VIEW IF EXISTS " + viewName);
    }
  }

  @Test
  public void listViewsMergesViewCatalogAndSessionCatalogViews() {
    String sessionViewName = "list_views_merge_session_catalog";
    String viewCatalogViewName = "list_views_merge_view_catalog";
    Identifier viewCatalogView = Identifier.of(new String[] {"default"}, viewCatalogViewName);

    spark.sql("DROP VIEW IF EXISTS " + sessionViewName);
    spark.sql("CREATE VIEW " + sessionViewName + " AS SELECT 1 AS id");

    try {
      SparkSessionCatalog<?> catalog =
          catalogWithIcebergCatalog(
              catalogProxy(IcebergViewCatalog.class, viewCatalogView),
              catalogProxy(SessionCatalog.class));

      assertThat(catalog.listViews("default"))
          .extracting(Identifier::name)
          .contains(sessionViewName, viewCatalogViewName);
    } finally {
      spark.sql("DROP VIEW IF EXISTS " + sessionViewName);
    }
  }

  @Test
  public void listViewsDelegatesToViewSessionCatalog() {
    Identifier sessionCatalogView =
        Identifier.of(new String[] {"default"}, "list_views_delegate_session_catalog");
    SparkSessionCatalog<?> catalog =
        catalogWithIcebergCatalog(
            catalogProxy(IcebergCatalog.class),
            catalogProxy(ViewSessionCatalog.class, sessionCatalogView));

    assertThat(catalog.listViews("default")).containsExactly(sessionCatalogView);
  }

  @Test
  public void listViewsFallsBackToSessionStateCatalog() {
    String viewName = "list_views_fallback_session_state";

    spark.sql("DROP VIEW IF EXISTS " + viewName);
    spark.sql("CREATE VIEW " + viewName + " AS SELECT 1 AS id");

    try {
      SparkSessionCatalog<?> catalog =
          catalogWithIcebergCatalog(
              catalogProxy(IcebergCatalog.class), catalogProxy(SessionCatalog.class));

      assertThat(catalog.listViews("default")).extracting(Identifier::name).contains(viewName);
      assertThat(catalog.listViews("default", "nested")).isEmpty();
    } finally {
      spark.sql("DROP VIEW IF EXISTS " + viewName);
    }
  }

  private SparkSessionCatalog<?> catalogWithIcebergCatalog(
      TableCatalog icebergCatalog, CatalogPlugin sessionCatalog) {
    SparkSessionCatalog<?> catalog = new TestableSparkSessionCatalog(icebergCatalog);
    catalog.initialize("test", CaseInsensitiveStringMap.empty());
    catalog.setDelegateCatalog(sessionCatalog);
    return catalog;
  }

  private static <T extends CatalogPlugin> T catalogProxy(
      Class<T> catalogClass, Identifier... views) {
    return catalogClass.cast(
        Proxy.newProxyInstance(
            TestSparkSessionCatalog.class.getClassLoader(),
            new Class<?>[] {catalogClass},
            new TestCatalogHandler(catalogClass.getSimpleName(), views)));
  }

  private interface IcebergCatalog extends TableCatalog {}

  private interface IcebergViewCatalog extends TableCatalog, ViewCatalog {}

  private interface SessionCatalog extends TableCatalog, FunctionCatalog, SupportsNamespaces {}

  private interface ViewSessionCatalog extends SessionCatalog, ViewCatalog {}

  private static class TestableSparkSessionCatalog extends SparkSessionCatalog<ViewSessionCatalog> {
    private final TableCatalog icebergCatalog;

    TestableSparkSessionCatalog(TableCatalog icebergCatalog) {
      this.icebergCatalog = icebergCatalog;
    }

    @Override
    protected TableCatalog buildSparkCatalog(String name, CaseInsensitiveStringMap options) {
      return icebergCatalog;
    }
  }

  private static class TestCatalogHandler implements InvocationHandler {
    private final String name;
    private final Identifier[] views;

    TestCatalogHandler(String name, Identifier[] views) {
      this.name = name;
      this.views = views;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] arguments) {
      if (method.getDeclaringClass() == Object.class) {
        return invokeObjectMethod(proxy, method, arguments);
      }

      switch (method.getName()) {
        case "initialize":
          return null;
        case "name":
          return name;
        case "listViews":
          return views;
        default:
          throw new UnsupportedOperationException("Unexpected catalog call: " + method);
      }
    }

    private Object invokeObjectMethod(Object proxy, Method method, Object[] arguments) {
      switch (method.getName()) {
        case "equals":
          return proxy == arguments[0];
        case "hashCode":
          return System.identityHashCode(proxy);
        case "toString":
          return name;
        default:
          throw new UnsupportedOperationException("Unexpected object call: " + method);
      }
    }
  }
}
