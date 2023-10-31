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

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

public class TestFlinkCatalogFactory {

  private Map<String, String> props;

  @Before
  public void before() {
    props = Maps.newHashMap();
    props.put("type", "iceberg");
    props.put(CatalogProperties.WAREHOUSE_LOCATION, "/tmp/location");
  }

  @Test
  public void testCreateCatalogHive() {
    String catalogName = "hiveCatalog";
    props.put(
        FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, FlinkCatalogFactory.ICEBERG_CATALOG_TYPE_HIVE);

    Catalog catalog =
        FlinkCatalogFactory.createCatalogLoader(catalogName, props, new Configuration())
            .loadCatalog();

    Assertions.assertThat(catalog).isNotNull().isInstanceOf(HiveCatalog.class);
  }

  @Test
  public void testCreateCatalogHadoop() {
    String catalogName = "hadoopCatalog";
    props.put(
        FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, FlinkCatalogFactory.ICEBERG_CATALOG_TYPE_HADOOP);

    Catalog catalog =
        FlinkCatalogFactory.createCatalogLoader(catalogName, props, new Configuration())
            .loadCatalog();

    Assertions.assertThat(catalog).isNotNull().isInstanceOf(HadoopCatalog.class);
  }

  @Test
  public void testCreateCatalogCustom() {
    String catalogName = "customCatalog";
    props.put(CatalogProperties.CATALOG_IMPL, CustomHadoopCatalog.class.getName());

    Catalog catalog =
        FlinkCatalogFactory.createCatalogLoader(catalogName, props, new Configuration())
            .loadCatalog();

    Assertions.assertThat(catalog).isNotNull().isInstanceOf(CustomHadoopCatalog.class);
  }

  @Test
  public void testCreateCatalogCustomWithHiveCatalogTypeSet() {
    String catalogName = "customCatalog";
    props.put(CatalogProperties.CATALOG_IMPL, CustomHadoopCatalog.class.getName());
    props.put(
        FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, FlinkCatalogFactory.ICEBERG_CATALOG_TYPE_HIVE);
    Assertions.assertThatThrownBy(
            () -> FlinkCatalogFactory.createCatalogLoader(catalogName, props, new Configuration()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "Cannot create catalog customCatalog, both catalog-type and catalog-impl are set");
  }

  @Test
  public void testLoadCatalogUnknown() {
    String catalogName = "unknownCatalog";
    props.put(FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, "fooType");
    Assertions.assertThatThrownBy(
            () -> FlinkCatalogFactory.createCatalogLoader(catalogName, props, new Configuration()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageStartingWith("Unknown catalog-type: fooType");
  }

  public static class CustomHadoopCatalog extends HadoopCatalog {

    public CustomHadoopCatalog() {}

    public CustomHadoopCatalog(Configuration conf, String warehouseLocation) {
      setConf(conf);
      initialize(
          "custom", ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation));
    }
  }
}
