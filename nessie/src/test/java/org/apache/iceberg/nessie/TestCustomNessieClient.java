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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.util.function.Function;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TestCatalogUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.client.auth.NessieAuthentication;
import org.projectnessie.client.http.HttpClientBuilder;

public class TestCustomNessieClient extends BaseTestIceberg {

  public TestCustomNessieClient() {
    super("main");
  }

  @Test
  public void testNoCustomClient() {
    NessieCatalog catalog = new NessieCatalog();
    catalog.initialize(
        "nessie",
        ImmutableMap.of(
            CatalogProperties.WAREHOUSE_LOCATION,
            temp.toUri().toString(),
            CatalogProperties.URI,
            uri,
            "client-api-version",
            apiVersion));
  }

  @Test
  public void testUnnecessaryDefaultCustomClient() {
    NessieCatalog catalog = new NessieCatalog();
    catalog.initialize(
        "nessie",
        ImmutableMap.of(
            CatalogProperties.WAREHOUSE_LOCATION,
            temp.toUri().toString(),
            CatalogProperties.URI,
            uri,
            NessieConfigConstants.CONF_NESSIE_CLIENT_BUILDER_IMPL,
            HttpClientBuilder.class.getName(),
            "client-api-version",
            apiVersion));
  }

  @Test
  public void testNonExistentCustomClient() {
    String nonExistingClass = "non.existent.ClientBuilderImpl";
    assertThatThrownBy(
            () -> {
              NessieCatalog catalog = new NessieCatalog();
              catalog.initialize(
                  "nessie",
                  ImmutableMap.of(
                      CatalogProperties.WAREHOUSE_LOCATION,
                      temp.toUri().toString(),
                      CatalogProperties.URI,
                      uri,
                      NessieConfigConstants.CONF_NESSIE_CLIENT_BUILDER_IMPL,
                      nonExistingClass));
            })
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(nonExistingClass);
  }

  @Test
  public void testCustomClient() {
    assertThatThrownBy(
            () -> {
              NessieCatalog catalog = new NessieCatalog();
              catalog.initialize(
                  "nessie",
                  ImmutableMap.of(
                      CatalogProperties.WAREHOUSE_LOCATION,
                      temp.toUri().toString(),
                      CatalogProperties.URI,
                      uri,
                      NessieConfigConstants.CONF_NESSIE_CLIENT_BUILDER_IMPL,
                      DummyClientBuilderImpl.class.getName()));
            })
        .isInstanceOf(RuntimeException.class)
        .hasMessage("BUILD CALLED");
  }

  @Test
  public void testAlternativeInitializeWithNulls() {
    NessieCatalog catalog = new NessieCatalog();
    NessieIcebergClient client = new NessieIcebergClient(null, null, null, null);
    FileIO fileIO = new TestCatalogUtil.TestFileIONoArg();

    assertThatThrownBy(() -> catalog.initialize("nessie", null, null, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("client must be non-null");

    assertThatThrownBy(() -> catalog.initialize("nessie", client, null, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("fileIO must be non-null");

    assertThatThrownBy(() -> catalog.initialize("nessie", client, fileIO, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("catalogOptions must be non-null");
  }

  @SuppressWarnings("rawtypes")
  public static final class DummyClientBuilderImpl implements NessieClientBuilder {

    @SuppressWarnings("unused")
    public static DummyClientBuilderImpl builder() {
      return new DummyClientBuilderImpl();
    }

    @Override
    public NessieClientBuilder fromSystemProperties() {
      return this;
    }

    @Override
    public NessieClientBuilder withAuthentication(NessieAuthentication authentication) {
      return this;
    }

    @Override
    public NessieClientBuilder withUri(URI uri) {
      return this;
    }

    @Override
    public NessieClientBuilder withAuthenticationFromConfig(Function configuration) {
      return this;
    }

    @Override
    public NessieClientBuilder fromConfig(Function configuration) {
      return this;
    }

    @Override
    public NessieApi build(Class apiContract) {
      throw new RuntimeException("BUILD CALLED");
    }
  }
}
