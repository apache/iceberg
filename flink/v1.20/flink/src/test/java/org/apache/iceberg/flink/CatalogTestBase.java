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
package org.apache.iceberg.flink;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.flink.util.ArrayUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTCatalogServer;
import org.apache.iceberg.rest.RESTServerExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public abstract class CatalogTestBase extends TestBase {

  /** Catalog implementations that SQL test suites can run against. */
  public enum CatalogType {
    HIVE("testhive"),
    HADOOP("testhadoop"),
    REST("testrest");

    private final String catalogNamePrefix;

    CatalogType(String catalogNamePrefix) {
      this.catalogNamePrefix = catalogNamePrefix;
    }

    String catalogName(Namespace namespace) {
      return namespace.isEmpty()
          ? catalogNamePrefix
          : catalogNamePrefix + "_" + Joiner.on('_').join(namespace.levels());
    }
  }

  protected static final String DATABASE = "db";
  @TempDir protected File hiveWarehouse;
  @TempDir protected File hadoopWarehouse;

  @RegisterExtension
  private static final RESTServerExtension REST_SERVER_EXTENSION =
      new RESTServerExtension(
          ImmutableMap.of(
              RESTCatalogServer.REST_PORT,
              RESTServerExtension.FREE_PORT,
              // In-memory sqlite database by default is private to the connection that created
              // it. If more than 1 jdbc connection backs the catalog, the connections can see
              // different database states, so limit the backend JdbcCatalog to a single
              // connection.
              CatalogProperties.CLIENT_POOL_SIZE,
              "1"));

  protected static RESTCatalog restCatalog;

  @Parameter(index = 0)
  protected CatalogType catalogType;

  @Parameter(index = 1)
  protected Namespace baseNamespace;

  protected Catalog validationCatalog;
  protected SupportsNamespaces validationNamespaceCatalog;
  protected Map<String, String> config = Maps.newHashMap();

  protected String catalogName;
  protected String flinkDatabase;
  protected Namespace icebergNamespace;
  protected boolean isHadoopCatalog;
  protected boolean isRestCatalog;

  @Parameters(name = "catalogType={0}, baseNamespace={1}")
  protected static List<Object[]> parameters() {
    return Arrays.asList(
        new Object[] {CatalogType.HIVE, Namespace.empty()},
        new Object[] {CatalogType.HADOOP, Namespace.empty()},
        new Object[] {CatalogType.HADOOP, Namespace.of("l0", "l1")});
  }

  @BeforeAll
  public static void initRestCatalog() {
    restCatalog = REST_SERVER_EXTENSION.client();
  }

  @BeforeEach
  public void before() {
    this.catalogName = catalogType.catalogName(baseNamespace);
    this.isHadoopCatalog = catalogType == CatalogType.HADOOP;
    this.isRestCatalog = catalogType == CatalogType.REST;
    if (isHadoopCatalog) {
      this.validationCatalog = new HadoopCatalog(hiveConf, "file:" + hadoopWarehouse.getPath());
    } else if (isRestCatalog) {
      this.validationCatalog = restCatalog;
    } else {
      this.validationCatalog = catalog;
    }
    this.validationNamespaceCatalog = (SupportsNamespaces) validationCatalog;

    config.put("type", "iceberg");
    if (!baseNamespace.isEmpty()) {
      config.put(FlinkCatalogFactory.BASE_NAMESPACE, baseNamespace.toString());
    }
    if (isHadoopCatalog) {
      config.put(FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, "hadoop");
    } else if (isRestCatalog) {
      config.put(FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, "rest");
      config.put(CatalogProperties.URI, restCatalog.properties().get(CatalogProperties.URI));
      // disable Flink-side catalog caching so validations that write directly through
      // restCatalog observe fresh metadata
      config.put(CatalogProperties.CACHE_ENABLED, "false");
    } else {
      config.put(FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, "hive");
      config.put(CatalogProperties.URI, getURI(hiveConf));
    }
    config.put(CatalogProperties.WAREHOUSE_LOCATION, String.format("file://%s", warehouseRoot()));
    config.put("extra-catalog-prop", "extra-value");

    this.flinkDatabase = catalogName + "." + DATABASE;
    this.icebergNamespace =
        Namespace.of(ArrayUtils.concat(baseNamespace.levels(), new String[] {DATABASE}));
    sql("CREATE CATALOG %s WITH %s", catalogName, toWithClause(config));
  }

  @AfterEach
  public void clean() {
    dropCatalog(catalogName, true);
  }

  protected String warehouseRoot() {
    if (isHadoopCatalog) {
      return hadoopWarehouse.getAbsolutePath();
    } else {
      return hiveWarehouse.getAbsolutePath();
    }
  }

  protected String getFullQualifiedTableName(String tableName) {
    final List<String> levels = Lists.newArrayList(icebergNamespace.levels());
    levels.add(tableName);
    return Joiner.on('.').join(levels);
  }

  static String getURI(HiveConf conf) {
    return conf.get(HiveConf.ConfVars.METASTOREURIS.varname);
  }
}
