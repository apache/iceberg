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

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public abstract class CatalogTestBase extends TestBaseWithCatalog {

  // these parameters are broken out to avoid changes that need to modify lots of test suites
  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        SparkCatalogConfig.HIVE.properties()
      },
      {
        SparkCatalogConfig.INMEMORY.catalogName(),
        SparkCatalogConfig.INMEMORY.implementation(),
        SparkCatalogConfig.INMEMORY.properties()
      },
      {
        SparkCatalogConfig.SPARK_SESSION.catalogName(),
        SparkCatalogConfig.SPARK_SESSION.implementation(),
        SparkCatalogConfig.SPARK_SESSION.properties()
      },
      {
        SparkCatalogConfig.REST.catalogName(),
        SparkCatalogConfig.REST.implementation(),
        ImmutableMap.builder()
            .putAll(SparkCatalogConfig.REST.properties())
            .put(CatalogProperties.URI, restCatalog.properties().get(CatalogProperties.URI))
            .build()
      }
    };
  }

  /**
   * Catalog parameters for tests that must inspect files on disk (for example, asserting that an
   * Iceberg-managed file exists via {@code java.io.File#exists()} or listing a directory with
   * Hadoop {@code FileSystem}). Drops the {@code testinmemory} catalog (its {@link
   * org.apache.iceberg.inmemory.InMemoryCatalog} cannot back a real filesystem) and the {@code
   * testrest} catalog (the shared REST server writes initial metadata through its own in-memory
   * {@link org.apache.iceberg.inmemory.InMemoryFileIO}, which a disk-only client cannot read).
   * Remaining catalogs strip the in-memory {@code FileIO} so {@code table.io()} round-trips through
   * real on-disk storage.
   */
  protected static Object[][] catalogParametersWithDiskBackedFileIo() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        SparkCatalogConfig.HIVE.propertiesWithoutFileIo()
      },
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties()
      },
      {
        SparkCatalogConfig.SPARK_SESSION.catalogName(),
        SparkCatalogConfig.SPARK_SESSION.implementation(),
        SparkCatalogConfig.SPARK_SESSION.propertiesWithoutFileIo()
      }
    };
  }
}
