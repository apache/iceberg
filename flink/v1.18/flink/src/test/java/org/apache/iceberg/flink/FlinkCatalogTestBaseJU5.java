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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public abstract class FlinkCatalogTestBaseJU5 extends TestBase {

  protected static final String DATABASE = "db";
  @TempDir protected File hiveWarehouse;
  @TempDir protected File hadoopWarehouse;

  @Parameter(index = 0)
  protected String catalogName;

  @Parameter(index = 1)
  protected Namespace baseNamespace;

  protected Catalog validationCatalog;
  protected SupportsNamespaces validationNamespaceCatalog;
  protected Map<String, String> config = Maps.newHashMap();

  protected String flinkDatabase;
  protected Namespace icebergNamespace;
  protected boolean isHadoopCatalog;

  @Parameters(name = "catalogName={0} baseNamespace={1}")
  static List<Object[]> parametrs() {
    return Arrays.asList(
        new Object[] {"testhive", Namespace.empty()},
        new Object[] {"testhadoop", Namespace.empty()},
        new Object[] {"testhadoop_basenamespace", Namespace.of("l0", "l1")});
  }

  @BeforeEach
  public void before() {
    this.isHadoopCatalog = catalogName.startsWith("testhadoop");
    this.validationCatalog =
        isHadoopCatalog
            ? new HadoopCatalog(hiveConf, "file:" + hadoopWarehouse.getPath())
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
