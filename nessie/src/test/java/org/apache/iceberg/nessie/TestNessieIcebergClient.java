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
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;

public class TestNessieIcebergClient extends BaseTestIceberg {

  private static final String BRANCH = "test-nessie-client";

  public TestNessieIcebergClient() {
    super(BRANCH);
  }

  @Test
  public void testWithNullRefLoadsMain() throws NessieNotFoundException {
    NessieIcebergClient client = new NessieIcebergClient(api, null, null, ImmutableMap.of());
    Assertions.assertThat(client.getRef().getReference())
        .isEqualTo(api.getReference().refName("main").get());
  }

  @Test
  public void testWithNullHash() throws NessieNotFoundException {
    NessieIcebergClient client = new NessieIcebergClient(api, BRANCH, null, ImmutableMap.of());
    Assertions.assertThat(client.getRef().getReference())
        .isEqualTo(api.getReference().refName(BRANCH).get());
  }

  @Test
  public void testWithReference() throws NessieNotFoundException {
    NessieIcebergClient client = new NessieIcebergClient(api, "main", null, ImmutableMap.of());

    Assertions.assertThat(client.withReference(null, null)).isEqualTo(client);
    Assertions.assertThat(client.withReference("main", null)).isNotEqualTo(client);
    Assertions.assertThat(
            client.withReference("main", api.getReference().refName("main").get().getHash()))
        .isEqualTo(client);

    Assertions.assertThat(client.withReference(BRANCH, null)).isNotEqualTo(client);
    Assertions.assertThat(
            client.withReference(BRANCH, api.getReference().refName(BRANCH).get().getHash()))
        .isNotEqualTo(client);
  }

  @Test
  public void testWithReferenceAfterRecreatingBranch()
      throws NessieConflictException, NessieNotFoundException {
    String branch = "branchToBeDropped";
    createBranch(branch);
    NessieIcebergClient client = new NessieIcebergClient(api, branch, null, ImmutableMap.of());

    // just create a new commit on the branch and then delete & re-create it
    Namespace namespace = Namespace.of("a");
    client.createNamespace(namespace, ImmutableMap.of());
    Assertions.assertThat(client.listNamespaces(namespace)).isNotNull();
    client
        .getApi()
        .deleteBranch()
        .branch((Branch) client.getApi().getReference().refName(branch).get())
        .delete();
    createBranch(branch);

    // make sure the client uses the re-created branch
    Reference ref = client.getApi().getReference().refName(branch).get();
    Assertions.assertThat(client.withReference(branch, null).getRef().getReference())
        .isEqualTo(ref);
    Assertions.assertThat(client.withReference(branch, null)).isNotEqualTo(client);
  }

  @Test
  public void testCreateNamespace() throws NessieConflictException, NessieNotFoundException {
    String branch = "createNamespaceBranch";
    createBranch(branch);
    Map<String, String> catalogOptions =
        ImmutableMap.of(
            CatalogProperties.USER, "iceberg-user",
            CatalogProperties.APP_ID, "iceberg-nessie");

    NessieIcebergClient client = new NessieIcebergClient(api, branch, null, catalogOptions);

    Assertions.assertThatThrownBy(
            () -> client.createNamespace(Namespace.empty(), ImmutableMap.of()))
        .hasMessageContaining("Creating empty namespaces is not supported");

    Assertions.assertThatThrownBy(
            () -> client.createNamespace(Namespace.of("a", "b"), ImmutableMap.of()))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining("Cannot create Namespace 'a.b': parent namespace 'a' does not exist");

    Namespace ns = Namespace.of("a");
    client.createNamespace(ns, ImmutableMap.of());
    Assertions.assertThat(client.listNamespaces(ns)).isNotNull();

    List<LogResponse.LogEntry> entries =
        client.getApi().getCommitLog().refName(branch).get().getLogEntries();
    Assertions.assertThat(entries)
        .isNotEmpty()
        .first()
        .satisfies(
            entry -> {
              Assertions.assertThat(entry.getCommitMeta().getMessage())
                  .contains("create namespace a");
              Assertions.assertThat(entry.getCommitMeta().getAuthor()).isEqualTo("iceberg-user");
              Assertions.assertThat(entry.getCommitMeta().getProperties())
                  .containsEntry(NessieUtil.APPLICATION_TYPE, "iceberg")
                  .containsEntry(CatalogProperties.APP_ID, "iceberg-nessie");
            });

    // test cases where a conflicting key is added by this client

    Assertions.assertThatThrownBy(() -> client.createNamespace(ns, ImmutableMap.of()))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Namespace already exists: 'a'");

    Schema schema =
        new Schema(Types.StructType.of(required(1, "id", Types.LongType.get())).fields());
    TableMetadata table1 =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), SortOrder.unsorted(), null, ImmutableMap.of());
    client.commitTable(
        null, table1, "file:///tmp/iceberg", (String) null, ContentKey.of("a", "tbl"));

    Assertions.assertThatThrownBy(
            () -> client.createNamespace(Namespace.of("a", "tbl"), ImmutableMap.of()))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Another content object with name 'a.tbl' already exists");

    // test cases where a conflicting key is added by another client

    api.commitMultipleOperations()
        .branch((Branch) client.getApi().getReference().refName(branch).get())
        .commitMeta(NessieUtil.buildCommitMetadata("create namespace b", catalogOptions))
        .operation(
            Operation.Put.of(
                ContentKey.of("b"), org.projectnessie.model.Namespace.of(ContentKey.of("b"))))
        .commit();

    Assertions.assertThatThrownBy(
            () -> client.createNamespace(Namespace.of("b"), ImmutableMap.of()))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Namespace already exists: 'b'");

    IcebergTable table2 = IcebergTable.of("file:///tmp/iceberg", 1, 1, 1, 1);
    api.commitMultipleOperations()
        .branch((Branch) client.getApi().getReference().refName(branch).get())
        .commitMeta(NessieUtil.buildCommitMetadata("create table a.tbl2", catalogOptions))
        .operation(Operation.Put.of(ContentKey.of("a", "tbl2"), table2))
        .commit();

    Assertions.assertThatThrownBy(
            () -> client.createNamespace(Namespace.of("a", "tbl2"), ImmutableMap.of()))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Another content object with name 'a.tbl2' already exists");

    client
        .getApi()
        .deleteBranch()
        .branch((Branch) client.getApi().getReference().refName(branch).get())
        .delete();

    Assertions.assertThatThrownBy(
            () -> client.createNamespace(Namespace.of("c"), ImmutableMap.of()))
        .hasMessageContaining(
            "Cannot create Namespace 'c': ref 'createNamespaceBranch' is no longer valid");
  }

  @Test
  public void testDropNamespace() throws NessieConflictException, NessieNotFoundException {
    String branch = "dropNamespaceBranch";
    createBranch(branch);
    Map<String, String> catalogOptions =
        ImmutableMap.of(
            CatalogProperties.USER, "iceberg-user",
            CatalogProperties.APP_ID, "iceberg-nessie");

    NessieIcebergClient client = new NessieIcebergClient(api, branch, null, catalogOptions);

    Namespace parent = Namespace.of("a");
    Namespace child = Namespace.of("a", "b");

    Assertions.assertThat(client.dropNamespace(parent)).isFalse();
    Assertions.assertThat(client.dropNamespace(child)).isFalse();

    client.createNamespace(parent, ImmutableMap.of());
    client.createNamespace(child, ImmutableMap.of());

    Assertions.assertThat(client.dropNamespace(child)).isTrue();

    List<LogResponse.LogEntry> entries =
        client.getApi().getCommitLog().refName(branch).get().getLogEntries();
    Assertions.assertThat(entries)
        .isNotEmpty()
        .first()
        .satisfies(
            entry -> {
              Assertions.assertThat(entry.getCommitMeta().getMessage())
                  .contains("drop namespace a.b");
              Assertions.assertThat(entry.getCommitMeta().getAuthor()).isEqualTo("iceberg-user");
              Assertions.assertThat(entry.getCommitMeta().getProperties())
                  .containsEntry(NessieUtil.APPLICATION_TYPE, "iceberg")
                  .containsEntry(CatalogProperties.APP_ID, "iceberg-nessie");
            });

    client.createNamespace(child, ImmutableMap.of());

    Assertions.assertThatThrownBy(() -> client.dropNamespace(parent))
        .hasMessageContaining("Namespace 'a' is not empty.");

    client
        .getApi()
        .deleteBranch()
        .branch((Branch) client.getApi().getReference().refName(branch).get())
        .delete();

    Assertions.assertThat(client.dropNamespace(child)).isFalse();
  }

  @Test
  public void testInvalidClientApiVersion() throws IOException {
    try (NessieCatalog newCatalog = new NessieCatalog()) {
      newCatalog.setConf(hadoopConfig);
      ImmutableMap.Builder<String, String> options =
          ImmutableMap.<String, String>builder().put("client-api-version", "3");
      Assertions.assertThatThrownBy(() -> newCatalog.initialize("nessie", options.buildOrThrow()))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Unsupported client-api-version: 3. Can only be 1 or 2");
    }
  }
}
