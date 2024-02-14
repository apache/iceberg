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

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.LocationUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.client.api.NessieApiV1;
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
public class TestNessieCatalog extends CatalogTests<NessieCatalog> {

  @NessiePersist static Persist persist;

  @RegisterExtension
  static NessieJaxRsExtension server = NessieJaxRsExtension.jaxRsExtension(() -> persist);

  @TempDir public Path temp;

  private NessieCatalog catalog;
  private NessieApiV1 api;
  private Configuration hadoopConfig;
  private String initialHashOfDefaultBranch;
  private String uri;

  @BeforeEach
  public void setUp(NessieClientFactory clientFactory, @NessieClientUri URI nessieUri)
      throws NessieNotFoundException {
    api = clientFactory.make();
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
    Map<String, String> options =
        ImmutableMap.of(
            "type",
            "nessie",
            "ref",
            ref,
            CatalogProperties.URI,
            uri,
            CatalogProperties.WAREHOUSE_LOCATION,
            temp.toUri().toString());
    return (NessieCatalog) CatalogUtil.buildIcebergCatalog("nessie", options, hadoopConfig);
  }

  @Override
  protected NessieCatalog catalog() {
    return catalog;
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Override
  protected boolean supportsNestedNamespaces() {
    return true;
  }

  @Override
  protected boolean supportsNamespaceProperties() {
    return true;
  }

  @Override
  protected boolean supportsServerSideRetry() {
    // TODO: we do support retries, but a bunch of tests are currently failing
    return false;
  }

  @Test
  @Override
  @Disabled(
      "Nessie does not differentiate between table creates & updates, thus a concurrent transaction does not fail")
  public void testConcurrentCreateTransaction() {
    super.testConcurrentCreateTransaction();
  }

  @Test
  public void testWarehouseLocationWithTrailingSlash() {
    Assertions.assertThat(catalog.defaultWarehouseLocation(TABLE))
        .startsWith(
            LocationUtil.stripTrailingSlash(temp.toUri().toString())
                + "/"
                + TABLE.namespace()
                + "/"
                + TABLE.name());
  }
}
