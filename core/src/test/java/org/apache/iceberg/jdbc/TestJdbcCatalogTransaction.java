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
package org.apache.iceberg.jdbc;

import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogTransactionTests;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class TestJdbcCatalogTransaction extends CatalogTransactionTests<JdbcCatalog> {
  private JdbcCatalog catalog;

  @Override
  protected JdbcCatalog catalog() {
    return catalog;
  }

  @BeforeEach
  public void before() {
    catalog = new JdbcCatalog();

    String sqliteDb =
        "jdbc:sqlite:file::memory:?ic" + UUID.randomUUID().toString().replace("-", "");
    catalog.setConf(new Configuration());
    catalog.initialize(
        "jdbc-catalog",
        ImmutableMap.of(
            CatalogProperties.WAREHOUSE_LOCATION,
            metadataDir.toFile().getAbsolutePath(),
            CatalogProperties.URI,
            sqliteDb,
            JdbcCatalog.PROPERTY_PREFIX + "username",
            "user",
            JdbcCatalog.PROPERTY_PREFIX + "password",
            "password"));
  }

  @AfterEach
  public void after() {
    if (null != catalog) {
      catalog.close();
    }
  }
}
