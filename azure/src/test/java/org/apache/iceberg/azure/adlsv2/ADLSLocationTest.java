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
package org.apache.iceberg.azure.adlsv2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.net.URISyntaxException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ADLSLocationTest {
  @ParameterizedTest
  @ValueSource(strings = {"abfs", "abfss"})
  public void testLocationParsing(String scheme) {
    String p1 = scheme + "://container@account.dfs.core.windows.net/path/to/file";
    ADLSLocation location = new ADLSLocation(p1);

    assertThat(location.storageEndpoint()).isEqualTo("account.dfs.core.windows.net");
    assertThat(location.container().get()).isEqualTo("container");
    assertThat(location.path()).isEqualTo("path/to/file");
  }

  @ParameterizedTest
  @ValueSource(strings = {"wasb", "wasbs"})
  public void testWasbLocationParsing(String scheme) {
    String p1 = scheme + "://container@account.blob.core.windows.net/path/to/file";
    ADLSLocation location = new ADLSLocation(p1);

    assertThat(location.storageEndpoint()).isEqualTo("account.blob.core.windows.net");
    assertThat(location.container().get()).isEqualTo("container");
    assertThat(location.path()).isEqualTo("path/to/file");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "abfs://container@account.dfs.core.windows.net/path%20to%20file",
        "wasb://container@account.blob.core.windows.net/path%20to%20file"
      })
  public void testEncodedString(String path) throws URISyntaxException {
    ADLSLocation location = new ADLSLocation(path);
    String expectedEndpoint = new URI(path).getHost();

    assertThat(location.storageEndpoint()).isEqualTo(expectedEndpoint);
    assertThat(location.container().get()).isEqualTo("container");
    assertThat(location.path()).isEqualTo("path%20to%20file");
  }

  @Test
  public void testMissingScheme() {
    assertThatThrownBy(() -> new ADLSLocation("/path/to/file"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid ADLS URI: /path/to/file");
  }

  @Test
  public void testInvalidScheme() {
    assertThatThrownBy(() -> new ADLSLocation("s3://bucket/path/to/file"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid ADLS URI: s3://bucket/path/to/file");
  }

  @Test
  public void testInvalidURI() {
    String invalidUri = "abfs://container@account.dfs.core.windows.net/#invalidPath#";
    assertThatThrownBy(() -> new ADLSLocation(invalidUri))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(String.format("Invalid ADLS URI: %s", invalidUri));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "abfs://account.dfs.core.windows.net/path/to/file",
        "wasb://account.blob.core.windows.net/path/to/file"
      })
  public void testNoContainer(String path) throws URISyntaxException {
    ADLSLocation location = new ADLSLocation(path);
    String expectedEndpoint = new URI(path).getHost();

    assertThat(location.storageEndpoint()).isEqualTo(expectedEndpoint);
    assertThat(location.container().isPresent()).isFalse();
    assertThat(location.path()).isEqualTo("path/to/file");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "abfs://container@account.dfs.core.windows.net",
        "wasb://container@account.blob.core.windows.net"
      })
  public void testNoPath(String path) throws URISyntaxException {
    ADLSLocation location = new ADLSLocation(path);
    String expectedEndpoint = new URI(path).getHost();

    assertThat(location.storageEndpoint()).isEqualTo(expectedEndpoint);
    assertThat(location.container().get()).isEqualTo("container");
    assertThat(location.path()).isEqualTo("");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "abfs://container@account.dfs.core.windows.net/path/to/file?query=foo#123",
        "wasb://container@account.blob.core.windows.net/path/to/file?query=foo#123"
      })
  public void testQueryAndFragment(String path) throws URISyntaxException {
    ADLSLocation location = new ADLSLocation(path);
    String expectedEndpoint = new URI(path).getHost();

    assertThat(location.storageEndpoint()).isEqualTo(expectedEndpoint);
    assertThat(location.container().get()).isEqualTo("container");
    assertThat(location.path()).isEqualTo("path/to/file");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "abfs://container@account.dfs.core.windows.net?query=foo#123",
        "wasb://container@account.blob.core.windows.net?query=foo#123"
      })
  public void testQueryAndFragmentNoPath(String path) throws URISyntaxException {
    ADLSLocation location = new ADLSLocation(path);
    String expectedEndpoint = new URI(path).getHost();

    assertThat(location.storageEndpoint()).isEqualTo(expectedEndpoint);
    assertThat(location.container().get()).isEqualTo("container");
    assertThat(location.path()).isEqualTo("");
  }
}
