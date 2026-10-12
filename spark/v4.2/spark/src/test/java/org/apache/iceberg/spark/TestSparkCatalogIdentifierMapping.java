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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.view.View;
import org.apache.spark.sql.catalyst.analysis.ViewUtil;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.Test;

class TestSparkCatalogIdentifierMapping {

  @Test
  void usesCatalogIdentifierMappingForIcebergViews() {
    Identifier sparkIdentifier = Identifier.of(new String[] {"spark_namespace"}, "view");
    TableIdentifier icebergIdentifier = TableIdentifier.of("iceberg_namespace", "mapped_view");
    View view = mock(View.class);
    ViewCatalog viewCatalog = mock(ViewCatalog.class);
    when(viewCatalog.viewExists(icebergIdentifier)).thenReturn(true);
    when(viewCatalog.loadView(icebergIdentifier)).thenReturn(view);

    SparkCatalog catalog = new MappedSparkCatalog(icebergIdentifier, viewCatalog);

    assertThat(ViewUtil.loadIcebergView(catalog, sparkIdentifier).get()).isSameAs(view);
    verify(viewCatalog).viewExists(icebergIdentifier);
    verify(viewCatalog).loadView(icebergIdentifier);
  }

  private static class MappedSparkCatalog extends SparkCatalog {
    private final TableIdentifier icebergIdentifier;
    private final ViewCatalog viewCatalog;

    private MappedSparkCatalog(TableIdentifier icebergIdentifier, ViewCatalog viewCatalog) {
      this.icebergIdentifier = icebergIdentifier;
      this.viewCatalog = viewCatalog;
    }

    @Override
    protected TableIdentifier buildIdentifier(Identifier identifier) {
      return icebergIdentifier;
    }

    @Override
    public ViewCatalog icebergViewCatalog() {
      return viewCatalog;
    }
  }
}
