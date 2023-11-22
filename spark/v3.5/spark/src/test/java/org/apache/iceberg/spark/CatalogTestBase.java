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

import java.io.File;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.provider.Arguments;

public abstract class CatalogTestBase extends TestBaseWithCatalog {

  // these parameters are broken out to avoid changes that need to modify lots of test suites
  public static Stream<Arguments> parameters() {
    return Stream.of(
        Arguments.of(
            SparkCatalogConfig.HIVE.catalogName(),
            SparkCatalogConfig.HIVE.implementation(),
            SparkCatalogConfig.HIVE.properties()),
        Arguments.of(
            SparkCatalogConfig.HADOOP.catalogName(),
            SparkCatalogConfig.HADOOP.implementation(),
            SparkCatalogConfig.HADOOP.properties()),
        Arguments.of(
            SparkCatalogConfig.SPARK.catalogName(),
            SparkCatalogConfig.SPARK.implementation(),
            SparkCatalogConfig.SPARK.properties()));
  }

  @TempDir public File temp;

  public CatalogTestBase(SparkCatalogConfig config) {
    super(config);
  }

  public CatalogTestBase(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }
}
