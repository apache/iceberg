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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatRuntimeException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.assertj.core.data.Index;
import org.junit.jupiter.api.Test;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
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
    assertThat(client.getRef().getReference()).isEqualTo(api.getReference().refName("main").get());
  }

  @Test
  public void testWithNullHash() throws NessieNotFoundException {
    NessieIcebergClient client = new NessieIcebergClient(api, BRANCH, null, ImmutableMap.of());
    assertThat(client.getRef().getReference()).isEqualTo(api.getReference().refName(BRANCH).get());
  }

  @Test
  public void testWithReference() throws NessieNotFoundException {
    NessieIcebergClient client = new NessieIcebergClient(api, "main", null, ImmutableMap.of());

    assertThat(client.withReference(null, null)).isEqualTo(client);
    assertThat(client.withReference("main", null)).isNotEqualTo(client);
    assertThat(client.withReference("main", api.getReference().refName("main").get().getHash()))
        .isEqualTo(client);

    assertThat(client.withReference(BRANCH, null)).isNotEqualTo(client);
    assertThat(client.withReference(BRANCH, api.getReference().refName(BRANCH).get().getHash()))
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
    assertThat(client.listNamespaces(namespace)).isNotNull();
    client
        .getApi()
        .deleteBranch()
        .branch((Branch) api.getReference().refName(branch).get())
        .delete();
    createBranch(branch);

    // make sure the client uses the re-created branch
    Reference ref = client.getApi().getReference().refName(branch).get();
    assertThat(client.withReference(branch, null).getRef().getReference()).isEqualTo(ref);
    assertThat(client.withReference(branch, null)).isNotEqualTo(client);
  }

  @Test
  public void testCreateNamespace() throws NessieConflictException, NessieNotFoundException {
    String branch = "createNamespaceBranch";
    createBranch(branch);
    Map<String, String> catalogOptions =
        Map.of(
            CatalogProperties.USER, "iceberg-user",
            CatalogProperties.APP_ID, "iceberg-nessie");
    NessieIcebergClient client = new NessieIcebergClient(api, branch, null, catalogOptions);

    client.createNamespace(Namespace.of("a"), Map.of());
    assertThat(client.listNamespaces(Namespace.of("a"))).isNotNull();

    List<LogResponse.LogEntry> entries = api.getCommitLog().refName(branch).get().getLogEntries();
    assertThat(entries)
        .isNotEmpty()
        .first()
        .extracting(LogResponse.LogEntry::getCommitMeta)
        .extracting(CommitMeta::getMessage, CommitMeta::getAuthor, CommitMeta::getProperties)
        .containsExactly(
            "create namespace a",
            "iceberg-user",
            ImmutableMap.of(
                "application-type", "iceberg",
                "app-id", "iceberg-nessie"));
  }

  @Test
  public void testCreateNamespaceInvalid() throws NessieConflictException, NessieNotFoundException {
    String branch = "createNamespaceInvalidBranch";
    createBranch(branch);
    NessieIcebergClient client = new NessieIcebergClient(api, branch, null, Map.of());

    assertThatThrownBy(() -> client.createNamespace(Namespace.empty(), Map.of()))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining("Invalid namespace: ");

    assertThatThrownBy(() -> client.createNamespace(Namespace.of("a", "b"), Map.of()))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining("Cannot create namespace 'a.b': parent namespace 'a' does not exist");
  }

  @Test
  public void testCreateNamespaceConflict()
      throws NessieConflictException, NessieNotFoundException {
    String branch = "createNamespaceConflictBranch";
    createBranch(branch);
    NessieIcebergClient client = new NessieIcebergClient(api, branch, null, Map.of());

    client.createNamespace(Namespace.of("a"), Map.of());

    assertThatThrownBy(() -> client.createNamespace(Namespace.of("a"), Map.of()))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Namespace already exists: a");

    client.commitTable(
        null, newTableMetadata(), "file:///tmp/iceberg", null, ContentKey.of("a", "tbl"));

    assertThatThrownBy(() -> client.createNamespace(Namespace.of("a", "tbl"), Map.of()))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Another content object with name 'a.tbl' already exists");
  }

  @Test
  public void testCreateNamespaceExternalConflict()
      throws NessieConflictException, NessieNotFoundException {
    String branch = "createNamespaceExternalConflictBranch";
    createBranch(branch);
    NessieIcebergClient client = new NessieIcebergClient(api, branch, null, Map.of());

    org.projectnessie.model.Namespace nessieNs =
        org.projectnessie.model.Namespace.of(ContentKey.of("a"));
    commit(branch, "create namespace a", Operation.Put.of(ContentKey.of("a"), nessieNs));

    assertThatThrownBy(() -> client.createNamespace(Namespace.of("a"), Map.of()))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Namespace already exists: a");

    IcebergTable table = IcebergTable.of("file:///tmp/iceberg", 1, 1, 1, 1);
    commit(branch, "create table a.tbl2", Operation.Put.of(ContentKey.of("a", "tbl"), table));

    assertThatThrownBy(() -> client.createNamespace(Namespace.of("a", "tbl"), Map.of()))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Another content object with name 'a.tbl' already exists");
  }

  @Test
  public void testCreateNamespaceNonExistingRef()
      throws NessieConflictException, NessieNotFoundException {
    String branch = "createNamespaceNonExistingRefBranch";
    createBranch(branch);
    NessieIcebergClient client = new NessieIcebergClient(api, branch, null, Map.of());

    client.createNamespace(Namespace.of("a"), Map.of());

    api.deleteBranch().branch((Branch) api.getReference().refName(branch).get()).delete();

    assertThatThrownBy(() -> client.createNamespace(Namespace.of("b"), Map.of()))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(
            "Cannot create namespace 'b': ref 'createNamespaceNonExistingRefBranch' is no longer valid");
  }

  @Test
  public void testDropNamespace() throws NessieConflictException, NessieNotFoundException {
    String branch = "dropNamespaceBranch";
    createBranch(branch);
    Map<String, String> catalogOptions =
        Map.of(
            CatalogProperties.USER, "iceberg-user",
            CatalogProperties.APP_ID, "iceberg-nessie");
    NessieIcebergClient client = new NessieIcebergClient(api, branch, null, catalogOptions);

    Namespace parent = Namespace.of("a");
    Namespace child = Namespace.of("a", "b");

    assertThat(client.dropNamespace(parent)).isFalse();
    assertThat(client.dropNamespace(child)).isFalse();

    client.createNamespace(parent, Map.of());
    client.createNamespace(child, Map.of());

    assertThat(client.dropNamespace(child)).isTrue();
    assertThat(client.dropNamespace(parent)).isTrue();

    List<LogResponse.LogEntry> entries = api.getCommitLog().refName(branch).get().getLogEntries();
    assertThat(entries)
        .isNotEmpty()
        .extracting(LogResponse.LogEntry::getCommitMeta)
        .satisfies(
            meta -> {
              assertThat(meta.getMessage()).contains("drop namespace a");
              assertThat(meta.getAuthor()).isEqualTo("iceberg-user");
              assertThat(meta.getProperties())
                  .containsEntry(NessieUtil.APPLICATION_TYPE, "iceberg")
                  .containsEntry(CatalogProperties.APP_ID, "iceberg-nessie");
            },
            Index.atIndex(0))
        .satisfies(
            meta -> {
              assertThat(meta.getMessage()).contains("drop namespace a.b");
              assertThat(meta.getAuthor()).isEqualTo("iceberg-user");
              assertThat(meta.getProperties())
                  .containsEntry(NessieUtil.APPLICATION_TYPE, "iceberg")
                  .containsEntry(CatalogProperties.APP_ID, "iceberg-nessie");
            },
            Index.atIndex(1));
  }

  @Test
  public void testDropNamespaceNotEmpty() throws NessieConflictException, NessieNotFoundException {
    String branch = "dropNamespaceInvalidBranch";
    createBranch(branch);
    NessieIcebergClient client = new NessieIcebergClient(api, branch, null, Map.of());

    client.createNamespace(Namespace.of("a"), Map.of());
    client.createNamespace(Namespace.of("a", "b"), Map.of());

    assertThatThrownBy(() -> client.dropNamespace(Namespace.of("a")))
        .isInstanceOf(NamespaceNotEmptyException.class)
        .hasMessageContaining("Namespace 'a' is not empty.");
  }

  @Test
  public void testDropNamespaceConflict() throws NessieConflictException, NessieNotFoundException {
    String branch = "dropNamespaceConflictBranch";
    createBranch(branch);
    NessieIcebergClient client = new NessieIcebergClient(api, branch, null, Map.of());

    client.createNamespace(Namespace.of("a"), Map.of());

    client.commitTable(
        null, newTableMetadata(), "file:///tmp/iceberg", null, ContentKey.of("a", "tbl"));

    assertThatThrownBy(() -> client.dropNamespace(Namespace.of("a", "tbl")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining("Content object with name 'a.tbl' is not a namespace.");
  }

  @Test
  public void testDropNamespaceExternalConflict()
      throws NessieConflictException, NessieNotFoundException {
    String branch = "dropNamespaceExternalConflictBranch";
    createBranch(branch);
    NessieIcebergClient client = new NessieIcebergClient(api, branch, null, Map.of());

    client.createNamespace(Namespace.of("a"), Map.of());

    org.projectnessie.model.Namespace original = fetchNamespace(ContentKey.of("a"), branch);
    org.projectnessie.model.Namespace updated =
        org.projectnessie.model.Namespace.builder()
            .from(original)
            .properties(Map.of("k1", "v1"))
            .build();
    commit(branch, "update namespace a", Operation.Put.of(ContentKey.of("a"), updated));

    assertThatThrownBy(() -> client.dropNamespace(Namespace.of("a")))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(
            "Cannot drop namespace 'a': Values of existing and expected content for key 'a' are different.");
  }

  @Test
  public void testDropNamespaceNonExistingRef()
      throws NessieConflictException, NessieNotFoundException {
    String branch = "dropNamespaceNonExistingRefBranch";
    createBranch(branch);
    NessieIcebergClient client = new NessieIcebergClient(api, branch, null, Map.of());

    client.createNamespace(Namespace.of("a"), Map.of());

    api.deleteBranch().branch((Branch) api.getReference().refName(branch).get()).delete();

    assertThat(client.dropNamespace(Namespace.of("a"))).isFalse();
  }

  @Test
  public void testSetProperties() throws NessieConflictException, NessieNotFoundException {
    String branch = "setPropertiesBranch";
    createBranch(branch);
    Map<String, String> catalogOptions =
        Map.of(
            CatalogProperties.USER, "iceberg-user",
            CatalogProperties.APP_ID, "iceberg-nessie");
    NessieIcebergClient client = new NessieIcebergClient(api, branch, null, catalogOptions);

    Namespace ns = Namespace.of("a");
    client.createNamespace(ns, Map.of("k1", "v1a"));

    assertThat(client.setProperties(ns, Map.of("k1", "v1b", "k2", "v2"))).isTrue();

    assertThat(client.loadNamespaceMetadata(ns))
        .hasSize(2)
        .containsEntry("k1", "v1b")
        .containsEntry("k2", "v2");

    List<LogResponse.LogEntry> entries = api.getCommitLog().refName(branch).get().getLogEntries();
    assertThat(entries)
        .isNotEmpty()
        .first()
        .extracting(LogResponse.LogEntry::getCommitMeta)
        .satisfies(
            meta -> {
              assertThat(meta.getMessage()).contains("update namespace a");
              assertThat(meta.getAuthor()).isEqualTo("iceberg-user");
              assertThat(meta.getProperties())
                  .containsEntry(NessieUtil.APPLICATION_TYPE, "iceberg")
                  .containsEntry(CatalogProperties.APP_ID, "iceberg-nessie");
            });
  }

  @Test
  public void testSetPropertiesExternalConflict()
      throws NessieConflictException, NessieNotFoundException {
    String branch = "setPropertiesExternalConflictBranch";
    createBranch(branch);
    NessieIcebergClient client = new NessieIcebergClient(api, branch, null, Map.of());

    Namespace ns = Namespace.of("a");
    client.createNamespace(ns, Map.of("k1", "v1a"));

    ContentKey key = ContentKey.of("a");
    org.projectnessie.model.Namespace original = fetchNamespace(key, branch);
    org.projectnessie.model.Namespace updated =
        org.projectnessie.model.Namespace.builder()
            .from(original)
            .properties(Map.of("k1", "v1b", "k2", "v2"))
            .build();
    commit(branch, "update namespace a", Operation.Put.of(key, updated));

    // will generate a conflict and a retry
    assertThat(client.setProperties(ns, Map.of("k1", "v1c", "k3", "v3"))).isTrue();

    assertThat(client.loadNamespaceMetadata(ns))
        .hasSize(3)
        .containsEntry("k1", "v1c")
        .containsEntry("k2", "v2")
        .containsEntry("k3", "v3");
  }

  @Test
  public void testSetPropertiesNonExistingNs()
      throws NessieConflictException, NessieNotFoundException {
    String branch = "setPropertiesNonExistingNsBranch";
    createBranch(branch);
    NessieIcebergClient client = new NessieIcebergClient(api, branch, null, Map.of());

    client.createNamespace(Namespace.of("a"), Map.of());

    commit(branch, "delete namespace a", Operation.Delete.of(ContentKey.of("a")));

    assertThatThrownBy(() -> client.setProperties(Namespace.of("a"), Map.of("k1", "v1a")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining("Namespace does not exist: a");
  }

  @Test
  public void testSetPropertiesNonExistingRef()
      throws NessieConflictException, NessieNotFoundException {
    String branch = "setPropertiesNonExistingRefBranch";
    createBranch(branch);
    NessieIcebergClient client = new NessieIcebergClient(api, branch, null, Map.of());

    client.createNamespace(Namespace.of("a"), Map.of());

    api.deleteBranch().branch((Branch) api.getReference().refName(branch).get()).delete();

    assertThatThrownBy(() -> client.setProperties(Namespace.of("a"), Map.of("k1", "v1")))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(
            "Cannot update properties on namespace 'a': ref 'setPropertiesNonExistingRefBranch' is no longer valid");
  }

  @Test
  public void testRemoveProperties() throws NessieConflictException, NessieNotFoundException {
    String branch = "removePropertiesBranch";
    createBranch(branch);
    Map<String, String> catalogOptions =
        Map.of(
            CatalogProperties.USER, "iceberg-user",
            CatalogProperties.APP_ID, "iceberg-nessie");
    NessieIcebergClient client = new NessieIcebergClient(api, branch, null, catalogOptions);

    Namespace ns = Namespace.of("a");

    client.createNamespace(ns, Map.of("k1", "v1", "k2", "v2"));

    assertThat(client.removeProperties(ns, Set.of("k1"))).isTrue();

    assertThat(client.loadNamespaceMetadata(ns)).hasSize(1).containsOnlyKeys("k2");

    List<LogResponse.LogEntry> entries = api.getCommitLog().refName(branch).get().getLogEntries();
    assertThat(entries)
        .isNotEmpty()
        .first()
        .extracting(LogResponse.LogEntry::getCommitMeta)
        .satisfies(
            meta -> {
              assertThat(meta.getMessage()).contains("update namespace a");
              assertThat(meta.getAuthor()).isEqualTo("iceberg-user");
              assertThat(meta.getProperties())
                  .containsEntry(NessieUtil.APPLICATION_TYPE, "iceberg")
                  .containsEntry(CatalogProperties.APP_ID, "iceberg-nessie");
            });
  }

  @Test
  public void testRemovePropertiesExternalConflict()
      throws NessieConflictException, NessieNotFoundException {
    String branch = "removePropertiesExternalConflictBranch";
    createBranch(branch);
    NessieIcebergClient client = new NessieIcebergClient(api, branch, null, Map.of());

    Namespace ns = Namespace.of("a");
    client.createNamespace(ns, Map.of("k1", "v1"));

    ContentKey key = ContentKey.of("a");
    org.projectnessie.model.Namespace original = fetchNamespace(key, branch);
    org.projectnessie.model.Namespace updated =
        org.projectnessie.model.Namespace.builder()
            .from(original)
            .properties(Map.of("k2", "v2", "k3", "v3"))
            .build();
    commit(branch, "update namespace a", Operation.Put.of(key, updated));

    // will generate a conflict and a retry
    assertThat(client.removeProperties(ns, Set.of("k2"))).isTrue();

    assertThat(client.loadNamespaceMetadata(ns)).hasSize(1).containsOnlyKeys("k3");
  }

  @Test
  public void testRemovePropertiesNonExistingNs()
      throws NessieConflictException, NessieNotFoundException {
    String branch = "removePropertiesNonExistingNsBranch";
    createBranch(branch);
    NessieIcebergClient client = new NessieIcebergClient(api, branch, null, Map.of());

    client.createNamespace(Namespace.of("a"), Map.of("k1", "v1"));

    commit(branch, "delete namespace a", Operation.Delete.of(ContentKey.of("a")));

    assertThatThrownBy(() -> client.removeProperties(Namespace.of("a"), Set.of("k1")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining("Namespace does not exist: a");
  }

  @Test
  public void testRemovePropertiesNonExistingRef()
      throws NessieConflictException, NessieNotFoundException {
    String branch = "removePropertiesNonExistingRefBranch";
    createBranch(branch);
    NessieIcebergClient client = new NessieIcebergClient(api, branch, null, Map.of());

    client.createNamespace(Namespace.of("a"), Map.of("k1", "v1"));

    api.deleteBranch().branch((Branch) api.getReference().refName(branch).get()).delete();

    assertThatThrownBy(() -> client.removeProperties(Namespace.of("a"), Set.of("k1")))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(
            "Cannot update properties on namespace 'a': ref 'removePropertiesNonExistingRefBranch' is no longer valid");
  }

  @Test
  public void testInvalidClientApiVersion() throws IOException {
    try (NessieCatalog newCatalog = new NessieCatalog()) {
      newCatalog.setConf(hadoopConfig);
      ImmutableMap.Builder<String, String> options =
          ImmutableMap.<String, String>builder().put("client-api-version", "3");
      assertThatIllegalArgumentException()
          .isThrownBy(() -> newCatalog.initialize("nessie", options.buildOrThrow()))
          .withMessage("Unsupported client-api-version: 3. Can only be 1 or 2");
    }
  }

  @Test
  public void testInvalidClientApiVersionViaURI() throws IOException {
    try (NessieCatalog newCatalog = new NessieCatalog()) {
      newCatalog.setConf(hadoopConfig);
      ImmutableMap.Builder<String, String> options =
          ImmutableMap.<String, String>builder().put("uri", "some/uri/");
      assertThatIllegalArgumentException()
          .isThrownBy(() -> newCatalog.initialize("nessie", options.buildOrThrow()))
          .withMessage(
              "URI doesn't end with the version: some/uri/. Please configure `client-api-version` in the catalog properties explicitly.");

      ImmutableMap.Builder<String, String> newOptions =
          ImmutableMap.<String, String>builder().put("uri", "some/uri/v3");
      assertThatIllegalArgumentException()
          .isThrownBy(() -> newCatalog.initialize("nessie", newOptions.buildOrThrow()))
          .withMessage("Unsupported client-api-version: 3. Can only be 1 or 2");
    }
  }

  @Test
  public void testClientApiVersionOverride() {
    // for v1 URI use v2 version and vice versa.
    String version = apiVersion.equals("1") ? "2" : "1";

    NessieCatalog newCatalog = new NessieCatalog();
    newCatalog.setConf(hadoopConfig);
    ImmutableMap.Builder<String, String> options =
        ImmutableMap.<String, String>builder()
            .put(CatalogProperties.URI, uri)
            .put(CatalogProperties.WAREHOUSE_LOCATION, temp.toUri().toString())
            .put("client-api-version", version);
    newCatalog.initialize("nessie", options.buildOrThrow());
    // Since client-api-version is configured, API version should not be based on URI.
    assertThatRuntimeException()
        .isThrownBy(() -> newCatalog.loadTable(TableIdentifier.of("foo", "t1")))
        .withMessageStartingWith("API version mismatch, check URI prefix");
  }

  private void commit(String branch, String message, Operation... operations)
      throws NessieNotFoundException, NessieConflictException {
    Branch ref = (Branch) api.getReference().refName(branch).get();
    api.commitMultipleOperations()
        .branch(ref)
        .commitMeta(NessieUtil.buildCommitMetadata(message, Map.of()))
        .operations(Arrays.asList(operations))
        .commit();
  }

  private org.projectnessie.model.Namespace fetchNamespace(ContentKey key, String branch)
      throws NessieNotFoundException {
    Reference reference = api.getReference().refName(branch).get();
    Content content = api.getContent().key(key).reference(reference).get().get(key);
    return content.unwrap(org.projectnessie.model.Namespace.class).orElseThrow();
  }

  private static TableMetadata newTableMetadata() {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()));
    return TableMetadata.newTableMetadata(
        schema, PartitionSpec.unpartitioned(), SortOrder.unsorted(), null, Map.of());
  }
}
