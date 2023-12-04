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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.flink.util.ArrayUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class FlinkCatalogTestBase extends FlinkTestBase {

  protected static final String DATABASE = "db";
  private static TemporaryFolder hiveWarehouse = new TemporaryFolder();
  private static TemporaryFolder hadoopWarehouse = new TemporaryFolder();

  @BeforeClass
  public static void createWarehouse() throws IOException {
    hiveWarehouse.create();
    hadoopWarehouse.create();
  }

  @AfterClass
  public static void dropWarehouse() {
    hiveWarehouse.delete();
    hadoopWarehouse.delete();
  }

  @Before
  public void before() {
    sql("CREATE CATALOG %s WITH %s", catalogName, toWithClause(config));
  }

  @After
  public void clean() {
    dropCatalog(catalogName, true);
  }

  @Parameterized.Parameters(name = "catalogName = {0} baseNamespace = {1}")
  public static Iterable<Object[]> parameters() {
    return Lists.newArrayList(
        new Object[] {"testhive", Namespace.empty()},
        new Object[] {"testhadoop", Namespace.empty()},
        new Object[] {"testhadoop_basenamespace", Namespace.of("l0", "l1")});
  }

  protected final String catalogName;
  protected final Namespace baseNamespace;
  protected final Catalog validationCatalog;
  protected final SupportsNamespaces validationNamespaceCatalog;
  protected final Map<String, String> config = Maps.newHashMap();

  protected final String flinkDatabase;
  protected final Namespace icebergNamespace;
  protected final boolean isHadoopCatalog;

  public FlinkCatalogTestBase(String catalogName, Namespace baseNamespace) {
    this.catalogName = catalogName;
    this.baseNamespace = baseNamespace;
    this.isHadoopCatalog = catalogName.startsWith("testhadoop");
    this.validationCatalog =
        isHadoopCatalog
            ? new HadoopCatalog(hiveConf, "file:" + hadoopWarehouse.getRoot())
            : catalog;
    this.validationNamespaceCatalog = (SupportsNamespaces) validationCatalog;

    config.put("type", "iceberg");
    if (!baseNamespace.isEmpty()) {
      config.put(FlinkCatalogFactory.BASE_NAMESPACE, baseNamespace.toString());
    }
    if (isHadoopCatalog) {
      config.put(FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, "hadoop");
    } else {
      config.put(FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, "hive");
      config.put(CatalogProperties.URI, getURI(hiveConf));
    }
    config.put(CatalogProperties.WAREHOUSE_LOCATION, String.format("file://%s", warehouseRoot()));

    this.flinkDatabase = catalogName + "." + DATABASE;
    this.icebergNamespace =
        Namespace.of(ArrayUtils.concat(baseNamespace.levels(), new String[] {DATABASE}));
  }

  protected String warehouseRoot() {
    if (isHadoopCatalog) {
      return hadoopWarehouse.getRoot().getAbsolutePath();
    } else {
      return hiveWarehouse.getRoot().getAbsolutePath();
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

  static String toWithClause(Map<String, String> props) {
    StringBuilder builder = new StringBuilder();
    builder.append("(");
    int propCount = 0;
    for (Map.Entry<String, String> entry : props.entrySet()) {
      if (propCount > 0) {
        builder.append(",");
      }
      builder
          .append("'")
          .append(entry.getKey())
          .append("'")
          .append("=")
          .append("'")
          .append(entry.getValue())
          .append("'");
      propCount++;
    }
    builder.append(")");
    return builder.toString();
  }
}
