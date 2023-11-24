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

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.ext.NessieClientFactory;
import org.projectnessie.client.ext.NessieClientUri;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;

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

  @AfterEach
  public void afterEach() throws Exception {
    anotherCatalog.close();
  }

  @Test
  public void testListNamespaces() throws NessieConflictException, NessieNotFoundException {
    Assertions.assertThat(catalog.listNamespaces()).isEmpty();
    Assertions.assertThat(anotherCatalog.listNamespaces()).isEmpty();

    // listing a non-existent namespace should return empty
    Assertions.assertThat(catalog.listNamespaces(Namespace.of("db1"))).isEmpty();
    Assertions.assertThat(anotherCatalog.listNamespaces(Namespace.of("db1"))).isEmpty();

    catalog.createNamespace(Namespace.of("db1"), Collections.emptyMap());

    Assertions.assertThat(catalog.listNamespaces()).containsExactlyInAnyOrder(Namespace.of("db1"));
    Assertions.assertThat(anotherCatalog.listNamespaces())
        .containsExactlyInAnyOrder(Namespace.of("db1"));

    // another client creates a namespace with the same nessie server
    anotherCatalog.createNamespace(Namespace.of("db2"), Collections.emptyMap());

    Assertions.assertThat(catalog.listNamespaces())
        .containsExactlyInAnyOrder(Namespace.of("db1"), Namespace.of("db2"));
    Assertions.assertThat(anotherCatalog.listNamespaces())
        .containsExactlyInAnyOrder(Namespace.of("db1"), Namespace.of("db2"));

    api.deleteBranch().branch((Branch) api.getReference().refName(branch).get()).delete();

    Assertions.assertThatThrownBy(() -> catalog.listNamespaces())
        .hasMessageContaining(
            "Cannot list top-level Namespaces: ref '%s' is no longer valid", branch);
    Assertions.assertThatThrownBy(() -> anotherCatalog.listNamespaces(Namespace.of("db1")))
        .hasMessageContaining(
            "Cannot list child Namespaces from 'db1': ref '%s' is no longer valid", branch);
  }

  @Test
  public void testLoadNamespaceMetadata() throws NessieConflictException, NessieNotFoundException {

    Assertions.assertThatThrownBy(() -> catalog.loadNamespaceMetadata(Namespace.of("namespace1")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining("Namespace does not exist: namespace1");
    Assertions.assertThatThrownBy(
            () -> anotherCatalog.loadNamespaceMetadata(Namespace.of("namespace1")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining("Namespace does not exist: namespace1");

    catalog.createNamespace(Namespace.of("namespace1"), Collections.emptyMap());

    // both clients should see the namespace because we read the HEAD of the ref
    Assertions.assertThat(catalog.listNamespaces())
        .containsExactlyInAnyOrder(Namespace.of("namespace1"));
    Assertions.assertThat(anotherCatalog.listNamespaces())
        .containsExactlyInAnyOrder(Namespace.of("namespace1"));

    // the other client should not be able to update the namespace
    // because it is still on the old ref hash
    Assertions.assertThatThrownBy(
            () ->
                anotherCatalog.setProperties(
                    Namespace.of("namespace1"), Collections.singletonMap("k1", "v1")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining("Namespace does not exist: namespace1");
    // the same client adds a metadata to the namespace: expect success
    catalog.setProperties(Namespace.of("namespace1"), Collections.singletonMap("k1", "v1"));

    // load metadata from the same client and another client both should work fine
    // because we read the HEAD of the ref
    Assertions.assertThat(anotherCatalog.loadNamespaceMetadata(Namespace.of("namespace1")))
        .containsExactly(Map.entry("k1", "v1"));
    Assertions.assertThat(catalog.loadNamespaceMetadata(Namespace.of("namespace1")))
        .containsExactly(Map.entry("k1", "v1"));

    api.deleteBranch().branch((Branch) api.getReference().refName(branch).get()).delete();

    Assertions.assertThatThrownBy(() -> catalog.loadNamespaceMetadata(Namespace.of("namespace1")))
        .hasMessageContaining(
            "Cannot load Namespace 'namespace1': ref '%s' is no longer valid", branch);
    Assertions.assertThatThrownBy(
            () -> anotherCatalog.loadNamespaceMetadata(Namespace.of("namespace1")))
        .hasMessageContaining(
            "Cannot load Namespace 'namespace1': ref '%s' is no longer valid", branch);
  }

  @Test
  public void testListTables() {
    createTable(TableIdentifier.parse("foo.tbl1"), schema);
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

  @Test
  public void testCommits() {
    TableIdentifier identifier = TableIdentifier.parse("foo.tbl1");
    createTable(identifier, schema);
    Table tableFromCatalog = catalog.loadTable(identifier);
    tableFromCatalog.updateSchema().addColumn("x1", Types.LongType.get()).commit();

    Table tableFromAnotherCatalog = anotherCatalog.loadTable(identifier);
    tableFromAnotherCatalog.updateSchema().addColumn("x2", Types.LongType.get()).commit();

    tableFromCatalog.updateSchema().addColumn("x3", Types.LongType.get()).commit();
    tableFromAnotherCatalog.updateSchema().addColumn("x4", Types.LongType.get()).commit();

    Assertions.assertThat(catalog.loadTable(identifier).schema().columns()).hasSize(5);
    Assertions.assertThat(anotherCatalog.loadTable(identifier).schema().columns()).hasSize(5);
  }

  @Test
  public void testConcurrentCommitsWithRefresh() {
    TableIdentifier identifier = TableIdentifier.parse("foo.tbl1");
    createTable(identifier, schema);

    String hashBefore = catalog.currentHash();

    TableOperations ops1 = catalog.newTableOps(identifier);
    TableMetadata metadata1 =
        TableMetadata.buildFrom(ops1.current()).setProperties(ImmutableMap.of("k1", "v1")).build();

    // commit should succeed
    TableOperations ops2 = catalog.newTableOps(identifier);
    TableMetadata metadata2 =
        TableMetadata.buildFrom(ops2.current()).setProperties(ImmutableMap.of("k2", "v2")).build();
    ops2.commit(ops2.current(), metadata2);

    // refresh the catalog's client.
    String hashAfter = catalog.currentHash();
    Assertions.assertThat(hashBefore).isNotEqualTo(hashAfter);

    // client refresh should not affect the ongoing commits (commit should still fail due staleness)
    Assertions.assertThatThrownBy(() -> ops1.commit(ops1.current(), metadata1))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining(
            "Cannot commit: Reference hash is out of date. Update the reference 'multiple-clients-test' and try again");
  }
}
