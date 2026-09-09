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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.catalog.Namespace;
import org.junit.jupiter.api.TestTemplate;

/**
 * Runs the {@link TestFlinkCatalogDatabase} suite against a REST catalog server backed by an
 * in-memory JDBC catalog.
 */
public class TestFlinkRESTCatalogDatabase extends TestFlinkCatalogDatabase {

  @Parameters(name = "catalogType={0}, baseNamespace={1}")
  protected static List<Object[]> parameters() {
    return Collections.singletonList(new Object[] {CatalogType.REST, Namespace.empty()});
  }

  @TestTemplate
  @Override
  public void testCreateNamespaceWithLocation() throws Exception {
    // The JDBC-backed REST catalog stores the 'location' property verbatim instead of
    // resolving it to a "file:" URI the way the Hive catalog does
    assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should not already exist")
        .isFalse();

    Path location = temporaryDirectory;
    sql("CREATE DATABASE %s WITH ('location'='%s')", flinkDatabase, location);
    assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should exist")
        .isTrue();
    Map<String, String> nsMetadata =
        validationNamespaceCatalog.loadNamespaceMetadata(icebergNamespace);
    assertThat(nsMetadata).containsEntry("location", location.toString());
  }
}
