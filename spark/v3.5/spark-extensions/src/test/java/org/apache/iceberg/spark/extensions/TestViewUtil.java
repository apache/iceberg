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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import org.apache.iceberg.spark.source.HasIcebergCatalog;
import org.apache.spark.sql.catalyst.analysis.ViewUtil;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.ViewCatalog;
import org.junit.jupiter.api.Test;

public class TestViewUtil {

  @Test
  public void viewCatalogWithoutIcebergCatalogIsNotRecognized() {
    ViewCatalog viewCatalog = mock(ViewCatalog.class);
    assertThat(ViewUtil.isViewCatalog(viewCatalog)).isFalse();
  }

  @Test
  public void viewCatalogWithIcebergCatalogIsRecognized() {
    ViewCatalog viewCatalog =
        mock(ViewCatalog.class, withSettings().extraInterfaces(HasIcebergCatalog.class));
    assertThat(ViewUtil.isViewCatalog(viewCatalog)).isTrue();
  }

  @Test
  public void nonViewCatalogIsNotRecognized() {
    CatalogPlugin catalog = mock(CatalogPlugin.class);
    assertThat(ViewUtil.isViewCatalog(catalog)).isFalse();
  }
}
