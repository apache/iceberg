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

import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public enum SparkCatalogConfig {
  HIVE(
      "testhive",
      SparkCatalog.class.getName(),
      ImmutableMap.of(
          "type",
          "hive",
          "default-namespace",
          "default",
          CatalogProperties.FILE_IO_IMPL,
          InMemoryFileIO.class.getName(),
          InMemoryFileIO.DISK_FALLBACK,
          "true")),
  HADOOP(
      "testhadoop",
      SparkCatalog.class.getName(),
      ImmutableMap.of("type", "hadoop", "cache-enabled", "false")),
  INMEMORY(
      "testinmemory",
      SparkCatalog.class.getName(),
      ImmutableMap.of(
          CatalogProperties.CATALOG_IMPL,
          InMemoryCatalog.class.getName(),
          InMemoryCatalog.SHARED_STORE_ID,
          "testinmemory",
          "default-namespace",
          "default",
          "cache-enabled",
          "false")),
  REST(
      "testrest",
      SparkCatalog.class.getName(),
      ImmutableMap.of(
          "type",
          "rest",
          "cache-enabled",
          "false",
          CatalogProperties.FILE_IO_IMPL,
          InMemoryFileIO.class.getName(),
          InMemoryFileIO.DISK_FALLBACK,
          "true")),
  SPARK_SESSION(
      "spark_catalog",
      SparkSessionCatalog.class.getName(),
      ImmutableMap.<String, String>builder()
          .put("type", "hive")
          .put("default-namespace", "default")
          .put("parquet-enabled", "true")
          // Spark will delete tables using v1, leaving the cache out of sync
          .put("cache-enabled", "false")
          .put(CatalogProperties.FILE_IO_IMPL, InMemoryFileIO.class.getName())
          .put(InMemoryFileIO.DISK_FALLBACK, "true")
          .build()),
  SPARK_WITH_VIEWS(
      "spark_with_views",
      SparkCatalog.class.getName(),
      ImmutableMap.of(
          CatalogProperties.CATALOG_IMPL,
          InMemoryCatalog.class.getName(),
          InMemoryCatalog.SHARED_STORE_ID,
          "spark_with_views",
          "default-namespace",
          "default",
          "cache-enabled",
          "false")),
  SPARK_SESSION_WITH_VIEWS(
      "spark_catalog",
      SparkSessionCatalog.class.getName(),
      ImmutableMap.of(
          "type",
          "rest",
          "default-namespace",
          "default",
          "cache-enabled",
          "false",
          CatalogProperties.FILE_IO_IMPL,
          InMemoryFileIO.class.getName(),
          InMemoryFileIO.DISK_FALLBACK,
          "true")),
  SPARK_WITH_HIVE_VIEWS(
      "spark_hive_with_views",
      SparkCatalog.class.getName(),
      ImmutableMap.of(
          "type",
          "hive",
          "default-namespace",
          "default",
          "cache-enabled",
          "false",
          CatalogProperties.FILE_IO_IMPL,
          InMemoryFileIO.class.getName(),
          InMemoryFileIO.DISK_FALLBACK,
          "true")),
  SPARK_SESSION_WITH_UNIQUE_LOCATION(
      "spark_catalog",
      SparkSessionCatalog.class.getName(),
      ImmutableMap.<String, String>builder()
          .put("type", "hive")
          .put("default-namespace", "default")
          .put("parquet-enabled", "true")
          .put("unique-table-location", "true")
          .put("cache-enabled", "false")
          .put(CatalogProperties.FILE_IO_IMPL, InMemoryFileIO.class.getName())
          .put(InMemoryFileIO.DISK_FALLBACK, "true")
          .build()),
  HIVE_WITH_UNIQUE_LOCATION(
      "hive_with_unique_location",
      SparkCatalog.class.getName(),
      ImmutableMap.of(
          "type",
          "hive",
          "default-namespace",
          "default",
          "unique-table-location",
          "true",
          CatalogProperties.FILE_IO_IMPL,
          InMemoryFileIO.class.getName(),
          InMemoryFileIO.DISK_FALLBACK,
          "true"));

  private final String catalogName;
  private final String implementation;
  private final Map<String, String> properties;

  SparkCatalogConfig(String catalogName, String implementation, Map<String, String> properties) {
    this.catalogName = catalogName;
    this.implementation = implementation;
    this.properties = properties;
  }

  public String catalogName() {
    return catalogName;
  }

  public String implementation() {
    return implementation;
  }

  public Map<String, String> properties() {
    return properties;
  }

  /**
   * Returns this config's properties without the in-memory {@code FileIO} entries. Tests that rely
   * on raw on-disk file inspection (for example, opening Parquet, ORC, or Avro files directly
   * through Hadoop readers) use this overload to opt out of the in-memory swap.
   */
  public Map<String, String> propertiesWithoutFileIo() {
    Map<String, String> withoutFileIo = Maps.newHashMap(properties);
    withoutFileIo.remove(CatalogProperties.FILE_IO_IMPL);
    withoutFileIo.remove(InMemoryFileIO.DISK_FALLBACK);
    return ImmutableMap.copyOf(withoutFileIo);
  }
}
