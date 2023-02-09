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
package org.apache.iceberg.nessie;

import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.AbstractMap;
import java.util.Collections;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.ext.NessieClientFactory;
import org.projectnessie.client.ext.NessieClientUri;

public class TestMultipleClients extends BaseTestIceberg {

  private static final String BRANCH = "multiple-clients-test";
  private static final Schema schema =
      new Schema(Types.StructType.of(required(1, "id", Types.LongType.get())).fields());

  public TestMultipleClients() {
    super(BRANCH);
  }

  // another client that connects to the same nessie server.
  NessieCatalog anotherCatalog;

  @Override
  @BeforeEach
  public void beforeEach(NessieClientFactory clientFactory, @NessieClientUri URI nessieUri)
      throws IOException {
    super.beforeEach(clientFactory, nessieUri);
    anotherCatalog = initCatalog(branch);
  }

  @Override
  @AfterEach
  public void afterEach() throws Exception {
    dropTables(catalog);
    dropTables(anotherCatalog);

    super.afterEach();
    anotherCatalog.close();
  }

  @Test
  public void testListNamespaces() {
    catalog.createNamespace(Namespace.of("db1"), Collections.emptyMap());
    Assertions.assertThat(catalog.listNamespaces()).containsExactlyInAnyOrder(Namespace.of("db1"));

    // another client creates a namespace with the same nessie server
    anotherCatalog.createNamespace(Namespace.of("db2"), Collections.emptyMap());
    Assertions.assertThat(anotherCatalog.listNamespaces())
        .containsExactlyInAnyOrder(Namespace.of("db1"), Namespace.of("db2"));

    Assertions.assertThat(catalog.listNamespaces())
        .containsExactlyInAnyOrder(Namespace.of("db1"), Namespace.of("db2"));
  }

  @Test
  public void testLoadNamespaceMetadata() {
    catalog.createNamespace(Namespace.of("namespace1"), Collections.emptyMap());
    Assertions.assertThat(catalog.listNamespaces())
        .containsExactlyInAnyOrder(Namespace.of("namespace1"));

    // another client adds a metadata to the same namespace
    anotherCatalog.setProperties(Namespace.of("namespace1"), Collections.singletonMap("k1", "v1"));
    AbstractMap.SimpleEntry<String, String> entry = new AbstractMap.SimpleEntry<>("k1", "v1");
    Assertions.assertThat(anotherCatalog.loadNamespaceMetadata(Namespace.of("namespace1")))
        .containsExactly(entry);

    Assertions.assertThat(catalog.loadNamespaceMetadata(Namespace.of("namespace1")))
        .containsExactly(entry);
  }

  @Test
  public void testListTables() {
    catalog.createTable(TableIdentifier.parse("foo.tbl1"), schema);
    Assertions.assertThat(catalog.listTables(Namespace.of("foo")))
        .containsExactlyInAnyOrder(TableIdentifier.parse("foo.tbl1"));

    // another client creates a table with the same nessie server
    anotherCatalog.createTable(TableIdentifier.parse("foo.tbl2"), schema);
    Assertions.assertThat(anotherCatalog.listTables(Namespace.of("foo")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"), TableIdentifier.parse("foo.tbl2"));

    Assertions.assertThat(catalog.listTables(Namespace.of("foo")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"), TableIdentifier.parse("foo.tbl2"));
  }

  private static void dropTables(NessieCatalog nessieCatalog) {
    nessieCatalog
        .listNamespaces()
        .forEach(
            namespace ->
                nessieCatalog
                    .listTables(namespace)
                    .forEach(identifier -> dropTable(nessieCatalog, identifier)));
  }

  private static void dropTable(NessieCatalog nessieCatalog, TableIdentifier identifier) {
    Table table = nessieCatalog.loadTable(identifier);
    File tableLocation = tableLocation(table);
    nessieCatalog.dropTable(identifier, false);
    try {
      FileUtils.deleteDirectory(tableLocation);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static File tableLocation(Table table) {
    return new File(table.location().replaceFirst("file:", ""));
  }
}
