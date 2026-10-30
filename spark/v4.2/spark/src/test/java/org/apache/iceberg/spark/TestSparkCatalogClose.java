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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.ViewCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

class TestSparkCatalogClose extends TestBase {

  private static final CaseInsensitiveStringMap NO_OPTIONS =
      new CaseInsensitiveStringMap(Collections.emptyMap());

  @Test
  void closeClosesCachedIcebergCatalog() throws IOException {
    Catalog icebergCatalog = mock(Catalog.class, withSettings().extraInterfaces(Closeable.class));
    SparkCatalog catalog =
        new SparkCatalog() {
          @Override
          protected Catalog buildIcebergCatalog(String name, CaseInsensitiveStringMap options) {
            return icebergCatalog;
          }
        };
    catalog.initialize("test_catalog", NO_OPTIONS);
    assertThat(catalog.icebergCatalog()).isInstanceOf(CachingCatalog.class);

    catalog.close();

    verify((Closeable) icebergCatalog).close();
  }

  @Test
  void sessionCatalogCloseDoesNotCloseDelegate() throws IOException {
    TableCatalog icebergCatalog =
        mock(TableCatalog.class, withSettings().extraInterfaces(Closeable.class));
    TableCatalog sessionCatalog =
        mock(
            TableCatalog.class,
            withSettings()
                .extraInterfaces(
                    FunctionCatalog.class,
                    SupportsNamespaces.class,
                    ViewCatalog.class,
                    Closeable.class));
    SparkSessionCatalog<?> catalog = new TestingSparkSessionCatalog<>(icebergCatalog);
    catalog.initialize("spark_catalog", NO_OPTIONS);
    catalog.setDelegateCatalog(sessionCatalog);

    catalog.close();

    verify((Closeable) icebergCatalog).close();
    verify((Closeable) sessionCatalog, never()).close();
  }

  private static class TestingSparkSessionCatalog<
          T extends TableCatalog & FunctionCatalog & SupportsNamespaces & ViewCatalog>
      extends SparkSessionCatalog<T> {
    private final TableCatalog icebergCatalog;

    private TestingSparkSessionCatalog(TableCatalog icebergCatalog) {
      this.icebergCatalog = icebergCatalog;
    }

    @Override
    protected TableCatalog buildSparkCatalog(String name, CaseInsensitiveStringMap options) {
      return icebergCatalog;
    }
  }
}
