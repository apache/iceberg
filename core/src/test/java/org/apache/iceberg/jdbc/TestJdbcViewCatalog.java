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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;

import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.ViewCatalogTests;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewOperations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TestJdbcViewCatalog extends ViewCatalogTests<JdbcCatalog> {

  private JdbcCatalog catalog;

  @TempDir private java.nio.file.Path tableDir;

  @BeforeEach
  public void before() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(
        CatalogProperties.URI,
        "jdbc:sqlite:file::memory:?ic" + UUID.randomUUID().toString().replace("-", ""));
    properties.put(JdbcCatalog.PROPERTY_PREFIX + "username", "user");
    properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", "password");
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, tableDir.toAbsolutePath().toString());
    properties.put(JdbcUtil.SCHEMA_VERSION_PROPERTY, JdbcUtil.SchemaVersion.V1.name());

    catalog = new JdbcCatalog();
    catalog.setConf(new Configuration());
    catalog.initialize("testCatalog", properties);
  }

  @Override
  protected JdbcCatalog catalog() {
    return catalog;
  }

  @Override
  protected Catalog tableCatalog() {
    return catalog;
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Test
  public void testCommitExceptionWithoutMessage() {
    TableIdentifier identifier = TableIdentifier.of("namespace1", "view");
    BaseView view =
        (BaseView)
            catalog
                .buildView(identifier)
                .withQuery("spark", "select * from tbl")
                .withSchema(SCHEMA)
                .withDefaultNamespace(Namespace.of("namespace1"))
                .create();
    ViewOperations ops = view.operations();
    ViewMetadata metadataV1 = ops.current();

    view.updateProperties().set("k1", "v1").commit();
    ops.refresh();

    try (MockedStatic<JdbcUtil> mockedStatic = Mockito.mockStatic(JdbcUtil.class)) {
      mockedStatic
          .when(() -> JdbcUtil.loadView(any(), any(), any(), any()))
          .thenThrow(new SQLException());
      assertThatThrownBy(() -> ops.commit(ops.current(), metadataV1))
          .isInstanceOf(UncheckedSQLException.class)
          .hasMessageStartingWith("Unknown failure");
    }
  }

  @Test
  public void testCommitExceptionWithMessage() {
    TableIdentifier identifier = TableIdentifier.of("namespace1", "view");
    BaseView view =
        (BaseView)
            catalog
                .buildView(identifier)
                .withQuery("spark", "select * from tbl")
                .withSchema(SCHEMA)
                .withDefaultNamespace(Namespace.of("namespace1"))
                .create();
    ViewOperations ops = view.operations();
    ViewMetadata metadataV1 = ops.current();

    view.updateProperties().set("k1", "v1").commit();
    ops.refresh();

    try (MockedStatic<JdbcUtil> mockedStatic = Mockito.mockStatic(JdbcUtil.class)) {
      mockedStatic
          .when(() -> JdbcUtil.loadView(any(), any(), any(), any()))
          .thenThrow(new SQLException("constraint failed"));
      assertThatThrownBy(() -> ops.commit(ops.current(), metadataV1))
          .isInstanceOf(AlreadyExistsException.class)
          .hasMessageStartingWith("View already exists: " + identifier);
    }
  }
}
