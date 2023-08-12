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

import java.util.stream.Stream;
import org.apache.iceberg.exceptions.ValidationException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class ADLSv2LocationTest {
  @Test
  public void testLocationParsing() {
    Stream.of("abfs", "abfss")
        .forEach(
            scheme -> {
              String p1 = scheme + "://container@account.dfs.core.windows.net/path/to/file";
              ADLSv2Location location = new ADLSv2Location(p1);

              Assertions.assertThat(location.storageAccountUrl())
                  .isEqualTo("https://account.dfs.core.windows.net");
              Assertions.assertThat(location.container()).isEqualTo("container");
              Assertions.assertThat(location.path()).isEqualTo("path/to/file");
            });
  }

  @Test
  public void testEncodedString() {
    String p1 = "abfs://container@account.dfs.core.windows.net/path%20to%20file";
    ADLSv2Location location = new ADLSv2Location(p1);

    Assertions.assertThat(location.storageAccountUrl())
        .isEqualTo("https://account.dfs.core.windows.net");
    Assertions.assertThat(location.container()).isEqualTo("container");
    // Azure API will handle encoding as needed
    Assertions.assertThat(location.path()).isEqualTo("path to file");
  }

  @Test
  public void testMissingScheme() {
    Assertions.assertThatThrownBy(() -> new ADLSv2Location("/path/to/file"))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Invalid ADLSv2 URI, cannot determine scheme: /path/to/file");
  }

  @Test
  public void testInvalidScheme() {
    Assertions.assertThatThrownBy(() -> new ADLSv2Location("s3://bucket/path/to/file"))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Invalid ADLSv2 URI, invalid scheme: s3");
  }

  @Test
  public void testMissingContainer() {
    Assertions.assertThatThrownBy(
            () -> new ADLSv2Location("abfs://account.dfs.core.windows.net/path/to/file"))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Invalid ADLSv2 URI, container is null");
  }

  @Test
  public void testOnlyContainerNameLocation() {
    String p1 = "abfs://container@account.dfs.core.windows.net";
    ADLSv2Location location = new ADLSv2Location(p1);

    Assertions.assertThat(location.storageAccountUrl())
        .isEqualTo("https://account.dfs.core.windows.net");
    Assertions.assertThat(location.container()).isEqualTo("container");
    Assertions.assertThat(location.path()).isEqualTo("");
  }

  @Test
  public void testQueryAndFragment() {
    String p1 = "abfs://container@account.dfs.core.windows.net/path/to/file?query=foo#123";
    ADLSv2Location location = new ADLSv2Location(p1);

    Assertions.assertThat(location.storageAccountUrl())
        .isEqualTo("https://account.dfs.core.windows.net");
    Assertions.assertThat(location.container()).isEqualTo("container");
    Assertions.assertThat(location.path()).isEqualTo("path/to/file");
  }
}
