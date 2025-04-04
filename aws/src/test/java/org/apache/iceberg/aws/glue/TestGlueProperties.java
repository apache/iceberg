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
package org.apache.iceberg.aws.glue;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.net.URI;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClientBuilder;

public class TestGlueProperties {

  @Test
  void testDefaultConstructor() {
    GlueProperties props = new GlueProperties();

    assertThat(props.glueCatalogId()).isNull();
    assertThat(props.glueCatalogSkipNameValidation()).isFalse();
    assertThat(props.glueCatalogSkipArchive()).isTrue();
    assertThat(props.glueLakeFormationEnabled()).isFalse();
  }

  @Test
  void testConstructorWithProperties() {
    Map<String, String> config =
        ImmutableMap.<String, String>builder()
            .put("glue.id", "123456789012")
            .put("glue.skip-archive", "false")
            .put("glue.skip-name-validation", "true")
            .put("glue.lakeformation-enabled", "true")
            .put("glue.endpoint", "https://custom-glue-endpoint.com")
            .build();

    GlueProperties props = new GlueProperties(config);

    assertThat(props.glueCatalogId()).isEqualTo("123456789012");
    assertThat(props.glueCatalogSkipArchive()).isFalse();
    assertThat(props.glueCatalogSkipNameValidation()).isTrue();
    assertThat(props.glueLakeFormationEnabled()).isTrue();
  }

  @Test
  void testApplyEndpointConfigurations() {
    Map<String, String> config =
        ImmutableMap.of("glue.endpoint", "https://custom-glue-endpoint.com");

    GlueProperties props = new GlueProperties(config);

    GlueClientBuilder builder = mock(GlueClientBuilder.class);
    props.applyEndpointConfigurations(builder);

    verify(builder).endpointOverride(URI.create("https://custom-glue-endpoint.com"));
  }

  @Test
  void testSettersWorkCorrectly() {
    GlueProperties props = new GlueProperties();
    props.setGlueCatalogId("test-id");
    props.setGlueCatalogSkipArchive(false);
    props.setGlueCatalogSkipNameValidation(true);
    props.setGlueLakeFormationEnabled(true);

    assertThat(props.glueCatalogId()).isEqualTo("test-id");
    assertThat(props.glueCatalogSkipArchive()).isFalse();
    assertThat(props.glueCatalogSkipNameValidation()).isTrue();
    assertThat(props.glueLakeFormationEnabled()).isTrue();
  }
}
