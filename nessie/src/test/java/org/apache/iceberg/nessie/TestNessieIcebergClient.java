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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
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
