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

import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.source.HasIcebergCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestViewCatalogCache extends ExtensionsTestBase {
  private static final Namespace NAMESPACE = Namespace.of("default");
  private static final String TABLE_NAME = "table";
  private final Set<String> viewNames = Sets.newLinkedHashSet();

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        "spark_with_cached_views",
        SparkCatalogConfig.SPARK_WITH_VIEWS.implementation(),
        ImmutableMap.builder()
            .put(CatalogProperties.CATALOG_IMPL, InMemoryCatalog.class.getName())
            .put("default-namespace", "default")
            .put(CatalogProperties.CACHE_ENABLED, "true")
            .build()
      },
      {
        SparkCatalogConfig.SPARK_SESSION_WITH_VIEWS.catalogName(),
        SparkCatalogConfig.SPARK_SESSION_WITH_VIEWS.implementation(),
        ImmutableMap.builder()
            .put("type", "rest")
            .put("default-namespace", "default")
            .put(CatalogProperties.CACHE_ENABLED, "true")
            .put(CatalogProperties.URI, restCatalog.properties().get(CatalogProperties.URI))
            .build()
      },
    };
  }

  @BeforeEach
  @Override
  public void before() {
    super.before();
    spark.conf().set("spark.sql.defaultCatalog", catalogName);
    sql("USE %s", catalogName);
    sql("CREATE NAMESPACE IF NOT EXISTS %s", NAMESPACE);
    sql(
        "CREATE TABLE IF NOT EXISTS %s.%s (id INT, data STRING)%s",
        NAMESPACE, TABLE_NAME, catalogName.equals("spark_catalog") ? " USING iceberg" : "");
    sql("USE %s.%s", catalogName, NAMESPACE);

    assertThat(sparkCatalog().icebergCatalog()).isInstanceOf(CachingCatalog.class);
  }

  @AfterEach
  public void removeTable() {
    sql("USE %s", catalogName);
    viewNames.forEach(viewName -> sql("DROP VIEW IF EXISTS %s.%s", NAMESPACE, viewName));
    sql("DROP TABLE IF EXISTS %s.%s", NAMESPACE, TABLE_NAME);
    spark.sessionState().catalogManager().reset();
    spark.conf().unset("spark.sql.catalog.spark_catalog");
  }

  @TestTemplate
  public void replaceViewRefreshesCatalogCache() {
    String viewName = viewName("replaceView");
    TableIdentifier identifier = TableIdentifier.of(NAMESPACE, viewName);
    String originalSql = String.format("SELECT id FROM %s", TABLE_NAME);
    String replacementSql = String.format("SELECT data FROM %s", TABLE_NAME);

    sql("CREATE VIEW %s AS %s", viewName, originalSql);
    assertThat(viewCatalog().loadView(identifier).sqlFor("spark").sql()).isEqualTo(originalSql);

    sql("CREATE OR REPLACE VIEW %s AS %s", viewName, replacementSql);
    assertThat(viewCatalog().loadView(identifier).sqlFor("spark").sql()).isEqualTo(replacementSql);
  }

  @TestTemplate
  public void replaceViewDropsOmittedProperties() {
    String viewName = viewName("replaceProperties");
    TableIdentifier identifier = TableIdentifier.of(NAMESPACE, viewName);

    sql(
        "CREATE VIEW %s TBLPROPERTIES ('keep'='old', 'drop'='old') AS SELECT id FROM %s",
        viewName, TABLE_NAME);
    assertThat(viewCatalog().loadView(identifier).properties()).containsEntry("drop", "old");

    sql(
        "CREATE OR REPLACE VIEW %s TBLPROPERTIES ('keep'='new') AS SELECT id FROM %s",
        viewName, TABLE_NAME);
    assertThat(viewCatalog().loadView(identifier).properties())
        .containsEntry("keep", "new")
        .doesNotContainKey("drop");
  }

  @TestTemplate
  public void alterViewPropertiesRefreshesCatalogCache() {
    String viewName = viewName("alterProperties");
    TableIdentifier identifier = TableIdentifier.of(NAMESPACE, viewName);

    sql("CREATE VIEW %s AS SELECT id FROM %s", viewName, TABLE_NAME);
    assertThat(viewCatalog().loadView(identifier).properties()).doesNotContainKey("key");

    sql("ALTER VIEW %s SET TBLPROPERTIES ('key'='value')", viewName);
    assertThat(viewCatalog().loadView(identifier).properties()).containsEntry("key", "value");

    sql("ALTER VIEW %s UNSET TBLPROPERTIES ('key')", viewName);
    assertThat(viewCatalog().loadView(identifier).properties()).doesNotContainKey("key");
  }

  @TestTemplate
  public void renameViewRefreshesCatalogCache() {
    String viewName = viewName("renameView");
    String renamedView = viewName("renamedView");
    TableIdentifier identifier = TableIdentifier.of(NAMESPACE, viewName);
    TableIdentifier renamedIdentifier = TableIdentifier.of(NAMESPACE, renamedView);

    sql("CREATE VIEW %s AS SELECT id FROM %s", viewName, TABLE_NAME);
    assertThat(viewCatalog().viewExists(identifier)).isTrue();

    sql("ALTER VIEW %s RENAME TO %s", viewName, renamedView);
    assertThat(viewCatalog().viewExists(identifier)).isFalse();
    assertThat(viewCatalog().viewExists(renamedIdentifier)).isTrue();
  }

  private HasIcebergCatalog sparkCatalog() {
    return (HasIcebergCatalog) spark.sessionState().catalogManager().catalog(catalogName);
  }

  private ViewCatalog viewCatalog() {
    return sparkCatalog().icebergViewCatalog();
  }

  private String viewName(String prefix) {
    String viewName = prefix + ThreadLocalRandom.current().nextInt(1000000);
    viewNames.add(viewName);
    return viewName;
  }
}
