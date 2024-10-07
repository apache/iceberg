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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewCatalogTests;
import org.apache.iceberg.view.ViewMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieApiVersions;
import org.projectnessie.client.ext.NessieClientFactory;
import org.projectnessie.client.ext.NessieClientUri;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.jaxrs.ext.NessieJaxRsExtension;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.inmemorytests.InmemoryBackendTestFactory;
import org.projectnessie.versioned.storage.testextension.NessieBackend;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

@ExtendWith(PersistExtension.class)
@NessieBackend(InmemoryBackendTestFactory.class)
@NessieApiVersions // test all versions
public class TestNessieViewCatalog extends ViewCatalogTests<NessieCatalog> {

  @NessiePersist static Persist persist;

  @RegisterExtension
  static NessieJaxRsExtension server = NessieJaxRsExtension.jaxRsExtension(() -> persist);

  @TempDir private Path temp;

  private NessieCatalog catalog;
  private NessieApiV1 api;
  private NessieApiVersion apiVersion;
  private Configuration hadoopConfig;
  private String initialHashOfDefaultBranch;
  private String uri;

  @BeforeEach
  public void setUp(NessieClientFactory clientFactory, @NessieClientUri URI nessieUri)
      throws NessieNotFoundException {
    api = clientFactory.make();
    apiVersion = clientFactory.apiVersion();
    initialHashOfDefaultBranch = api.getDefaultBranch().getHash();
    uri = nessieUri.toASCIIString();
    hadoopConfig = new Configuration();
    catalog = initNessieCatalog("main");
  }

  @AfterEach
  public void afterEach() throws IOException {
    resetData();
    try {
      if (catalog != null) {
        catalog.close();
      }
      api.close();
    } finally {
      catalog = null;
      api = null;
      hadoopConfig = null;
    }
  }

  private void resetData() throws NessieConflictException, NessieNotFoundException {
    Branch defaultBranch = api.getDefaultBranch();
    for (Reference r : api.getAllReferences().get().getReferences()) {
      if (r instanceof Branch && !r.getName().equals(defaultBranch.getName())) {
        api.deleteBranch().branch((Branch) r).delete();
      }
      if (r instanceof Tag) {
        api.deleteTag().tag((Tag) r).delete();
      }
    }
    api.assignBranch()
        .assignTo(Branch.of(defaultBranch.getName(), initialHashOfDefaultBranch))
        .branch(defaultBranch)
        .assign();
  }

  private NessieCatalog initNessieCatalog(String ref) {
    NessieCatalog newCatalog = new NessieCatalog();
    newCatalog.setConf(hadoopConfig);
    ImmutableMap<String, String> options =
        ImmutableMap.of(
            "ref",
            ref,
            CatalogProperties.URI,
            uri,
            CatalogProperties.WAREHOUSE_LOCATION,
            temp.toUri().toString(),
            "client-api-version",
            apiVersion == NessieApiVersion.V2 ? "2" : "1",
            CatalogProperties.VIEW_DEFAULT_PREFIX + "key1",
            "catalog-default-key1");
    newCatalog.initialize("nessie", options);
    return newCatalog;
  }

  @Override
  protected NessieCatalog catalog() {
    return catalog;
  }

  @Override
  protected Catalog tableCatalog() {
    return catalog;
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  // Overriding the below rename view testcases to exclude checking same view metadata after rename.
  // Nessie adds extra properties (like commit id) on every operation. Hence, view metadata will not
  // be same after rename.
  @Override
  @Test
  public void renameView() {
    TableIdentifier from = TableIdentifier.of("ns", "view");
    TableIdentifier to = TableIdentifier.of("ns", "renamedView");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(from.namespace());
    }

    assertThat(catalog().viewExists(from)).as("View should not exist").isFalse();

    View view =
        catalog()
            .buildView(from)
            .withSchema(SCHEMA)
            .withDefaultNamespace(from.namespace())
            .withQuery("spark", "select * from ns.tbl")
            .create();

    assertThat(catalog().viewExists(from)).as("View should exist").isTrue();

    ViewMetadata original = ((BaseView) view).operations().current();
    assertThat(original.metadataFileLocation()).isNotNull();

    catalog().renameView(from, to);

    assertThat(catalog().viewExists(from)).as("View should not exist with old name").isFalse();
    assertThat(catalog().viewExists(to)).as("View should exist with new name").isTrue();

    assertThat(catalog().dropView(from)).isFalse();
    assertThat(catalog().dropView(to)).isTrue();
    assertThat(catalog().viewExists(to)).as("View should not exist").isFalse();
  }

  @Override
  @Test
  public void renameViewUsingDifferentNamespace() {
    TableIdentifier from = TableIdentifier.of("ns", "view");
    TableIdentifier to = TableIdentifier.of("other_ns", "renamedView");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(from.namespace());
      catalog().createNamespace(to.namespace());
    }

    assertThat(catalog().viewExists(from)).as("View should not exist").isFalse();

    catalog()
        .buildView(from)
        .withSchema(SCHEMA)
        .withDefaultNamespace(from.namespace())
        .withQuery("spark", "select * from ns.tbl")
        .create();

    assertThat(catalog().viewExists(from)).as("View should exist").isTrue();

    catalog().renameView(from, to);

    assertThat(catalog().viewExists(from)).as("View should not exist with old name").isFalse();
    assertThat(catalog().viewExists(to)).as("View should exist with new name").isTrue();

    assertThat(catalog().dropView(from)).isFalse();
    assertThat(catalog().dropView(to)).isTrue();
    assertThat(catalog().viewExists(to)).as("View should not exist").isFalse();
  }
}
