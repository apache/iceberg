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
package org.apache.iceberg.mr.hive;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestCatalogs {

  private Configuration conf;

  @BeforeEach
  public void before() {
    conf = new Configuration();
  }

  @Test
  public void testLoadCatalogDefault() {
    String catalogName = "barCatalog";
    Optional<Catalog> defaultCatalog = Catalogs.loadCatalog(conf, catalogName);
    assertThat(defaultCatalog).isPresent();
    assertThat(defaultCatalog.get()).isInstanceOf(HiveCatalog.class);
    Properties properties = new Properties();
    properties.put(InputFormatConfig.CATALOG_NAME, catalogName);
    assertThat(Catalogs.hiveCatalog(conf, properties)).isTrue();
  }

  @Test
  public void testLoadCatalogHive() {
    String catalogName = "barCatalog";
    conf.set(
        InputFormatConfig.catalogPropertyConfigKey(catalogName, CatalogUtil.ICEBERG_CATALOG_TYPE),
        CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE);
    Optional<Catalog> hiveCatalog = Catalogs.loadCatalog(conf, catalogName);
    assertThat(hiveCatalog).isPresent();
    assertThat(hiveCatalog.get()).isInstanceOf(HiveCatalog.class);
    Properties properties = new Properties();
    properties.put(InputFormatConfig.CATALOG_NAME, catalogName);
    assertThat(Catalogs.hiveCatalog(conf, properties)).isTrue();
  }
}
