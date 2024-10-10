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

import org.apache.iceberg.exceptions.ValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ADLSLocationTest {
  @ParameterizedTest
  @ValueSource(strings = {"abfs", "abfss"})
  public void testLocationParsing(String scheme) {
    String p1 = scheme + "://container@account.dfs.core.windows.net/path/to/file";
    ADLSLocation location = new ADLSLocation(p1);

    assertThat(location.storageAccount()).isEqualTo("account.dfs.core.windows.net");
    assertThat(location.container().get()).isEqualTo("container");
    assertThat(location.path()).isEqualTo("path/to/file");
  }

  @ParameterizedTest
  @ValueSource(strings = {"wasb", "wasbs"})
  public void testWasbLocationParsing(String scheme) {
    String p1 = scheme + "://container@account.blob.core.windows.net/path/to/file";
    ADLSLocation location = new ADLSLocation(p1);

    assertThat(location.storageAccount()).isEqualTo("account.dfs.core.windows.net");
    assertThat(location.container().get()).isEqualTo("container");
    assertThat(location.path()).isEqualTo("path/to/file");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "abfs://container@account.dfs.core.windows.net/path%20to%20file",
        "wasb://container@account.blob.core.windows.net/path%20to%20file"
      })
  public void testEncodedString(String path) {
    ADLSLocation location = new ADLSLocation(path);

    assertThat(location.storageAccount()).isEqualTo("account.dfs.core.windows.net");
    assertThat(location.container().get()).isEqualTo("container");
    assertThat(location.path()).isEqualTo("path%20to%20file");
  }

  @Test
  public void testMissingScheme() {
    assertThatThrownBy(() -> new ADLSLocation("/path/to/file"))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Invalid ADLS URI: /path/to/file");
  }

  @Test
  public void testInvalidScheme() {
    assertThatThrownBy(() -> new ADLSLocation("s3://bucket/path/to/file"))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Invalid ADLS URI: s3://bucket/path/to/file");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "abfs://account.dfs.core.windows.net/path/to/file",
        "wasb://account.blob.core.windows.net/path/to/file"
      })
  public void testNoContainer(String path) {
    ADLSLocation location = new ADLSLocation(path);

    assertThat(location.storageAccount()).isEqualTo("account.dfs.core.windows.net");
    assertThat(location.container().isPresent()).isFalse();
    assertThat(location.path()).isEqualTo("path/to/file");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "abfs://container@account.dfs.core.windows.net",
        "wasb://container@account.blob.core.windows.net"
      })
  public void testNoPath(String path) {
    ADLSLocation location = new ADLSLocation(path);

    assertThat(location.storageAccount()).isEqualTo("account.dfs.core.windows.net");
    assertThat(location.container().get()).isEqualTo("container");
    assertThat(location.path()).isEqualTo("");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "abfs://container@account.dfs.core.windows.net/path/to/file?query=foo#123",
        "wasb://container@account.blob.core.windows.net/path/to/file?query=foo#123"
      })
  public void testQueryAndFragment(String path) {
    ADLSLocation location = new ADLSLocation(path);

    assertThat(location.storageAccount()).isEqualTo("account.dfs.core.windows.net");
    assertThat(location.container().get()).isEqualTo("container");
    assertThat(location.path()).isEqualTo("path/to/file");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "abfs://container@account.dfs.core.windows.net?query=foo#123",
        "wasb://container@account.blob.core.windows.net?query=foo#123"
      })
  public void testQueryAndFragmentNoPath(String path) {
    ADLSLocation location = new ADLSLocation(path);

    assertThat(location.storageAccount()).isEqualTo("account.dfs.core.windows.net");
    assertThat(location.container().get()).isEqualTo("container");
    assertThat(location.path()).isEqualTo("");
  }
}
