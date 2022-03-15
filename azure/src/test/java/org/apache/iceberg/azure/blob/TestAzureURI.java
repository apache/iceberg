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

package org.apache.iceberg.azure.blob;

import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.exceptions.ValidationException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestAzureURI {

  private static final String EXPECTED_ABFS_URI_FORM =
      "abfs://[<container name>@]<account name>.dfs.core.windows.net/<file path>";

  @Test
  public void testValidAzureURI() {
    final String expectedContainer = "testContainer";
    final String expectedStorageAccount = "testStorageAccount";
    final String expectedPath = "/dir1/dir2/dir3/hello.txt";
    final String expectedLocation =
        String.format("abfs://%s@%s.dfs.core.windows.net%s", expectedContainer, expectedStorageAccount, expectedPath);
    final AzureURI uri = AzureURI.from(expectedLocation);
    assertThat(uri.location()).isEqualTo(expectedLocation);
    assertThat(uri.container()).isEqualTo(expectedContainer);
    assertThat(uri.storageAccount()).isEqualTo(expectedStorageAccount);
    assertThat(uri.path()).isEqualTo(expectedPath);
  }

  @Test
  public void testNullLocation() {
    AssertHelpers.assertThrows(
        "Should not allow null location",
        NullPointerException.class,
        "Location cannot be null",
        () -> {
          AzureURI.from(null);
        });
  }

  @Test
  public void testInvalidScheme() {
    final String[] invalidSchemeURIs = {"abf://testContainer@testStorageAccount.dfs.core.windows.net/dir1/test.txt",
                                        "testContainer@testStorageAccount.dfs.core.windows.net/dir1/test.txt"};
    for (final String invalidSchemeURI : invalidSchemeURIs) {
      AssertHelpers.assertThrows(
          "Should not allow invalid scheme",
          ValidationException.class,
          String.format("Invalid Azure URI scheme, Expected scheme is 'afbs': %s", invalidSchemeURI),
          () -> {
            AzureURI.from(invalidSchemeURI);
          });
    }
  }

  @Test
  public void testUriSyntaxException() {
    final String invalidURI = ":/testContainer@testStorageAccount.dfs.core.windows.net";
    AssertHelpers.assertThrows(
        "Should not allow invalid URI syntax",
        ValidationException.class,
        String.format("Invalid Azure URI: %s", invalidURI),
        () -> {
          AzureURI.from(invalidURI);
        });
  }

  @Test
  public void testNullAuthority() {
    final String invalidAuthorityURI = "abfs:/testContainer@testStorageAccount.dfs.core.windows.net/dir1/test.txt";
    AssertHelpers.assertThrows(
        "Should not allow null authority",
        ValidationException.class,
        String.format("Invalid Azure URI. Expected form is '%s': %s", EXPECTED_ABFS_URI_FORM, invalidAuthorityURI),
        () -> {
          AzureURI.from(invalidAuthorityURI);
        });
  }

  @Test
  public void testMissingAuthorityDelimiter() {
    final String[] invalidAuthorityURIs = {"abfs://testContainer#testStorageAccount.dfs.core.windows.net/dir1/test.txt",
                                           "abfs://testStorageAccount.dfs.core.windows.net/dir1/test.txt"};
    for (final String invalidAuthorityURI : invalidAuthorityURIs) {
      AssertHelpers.assertThrows(
          "Should not allow invalid authority",
          ValidationException.class,
          String.format("Invalid Azure URI. Expected form is '%s': %s", EXPECTED_ABFS_URI_FORM, invalidAuthorityURI),
          () -> {
            AzureURI.from(invalidAuthorityURI);
          });
    }
  }

  @Test
  public void testEmptyContainerNameInAuthority() {
    final String invalidAuthorityURI = "abfs://@testStorageAccount.dfs.core.windows.net/dir1/test.txt";
    AssertHelpers.assertThrows(
        "Should not allow invalid container name",
        ValidationException.class,
        String.format("Invalid Azure URI. Expected form is '%s': %s", EXPECTED_ABFS_URI_FORM, invalidAuthorityURI),
        () -> {
          AzureURI.from(invalidAuthorityURI);
        });
  }

  @Test
  public void testEmptyStorageEndpointInAuthority() {
    final String invalidAuthorityURI = "abfs://testContainer@/dir1/test.txt";
    AssertHelpers.assertThrows(
        "Should not allow invalid storage endpoint",
        ValidationException.class,
        String.format("Invalid Azure URI. Expected form is '%s': %s", EXPECTED_ABFS_URI_FORM, invalidAuthorityURI),
        () -> {
          AzureURI.from(invalidAuthorityURI);
        });
  }

  @Test
  public void testMissingEndpointDelimiter() {
    final String invalidAuthorityURI = "abfs://testContainer@testStorageAccount/dir1/test.txt";
    AssertHelpers.assertThrows(
        "Should not allow invalid authority",
        ValidationException.class,
        String.format("Invalid Azure URI. Expected form is '%s': %s", EXPECTED_ABFS_URI_FORM, invalidAuthorityURI),
        () -> {
          AzureURI.from(invalidAuthorityURI);
        });
  }

  @Test
  public void testEmptyStorageNameInStorageEndpoint() {
    final String invalidAuthorityURI = "abfs://testContainer@.testStorageAccount/dir1/test.txt";
    AssertHelpers.assertThrows(
        "Should not allow invalid storage account name",
        ValidationException.class,
        String.format("Invalid Azure URI. Expected form is '%s': %s", EXPECTED_ABFS_URI_FORM, invalidAuthorityURI),
        () -> {
          AzureURI.from(invalidAuthorityURI);
        });
  }

  @Test
  public void testEmptyPath() {
    final String[] emptyPathURIs = {"abfs://testContainer@testStorageAccount.dfs.core.windows.net",
                                    "abfs://testContainer@testStorageAccount.dfs.core.windows.net/"};
    for (final String emptyPathURI : emptyPathURIs) {
      AssertHelpers.assertThrows(
          "Should not allow invalid file path",
          ValidationException.class,
          String.format("Invalid Azure URI, empty path: %s", emptyPathURI),
          () -> {
            AzureURI.from(emptyPathURI);
          });
    }
  }
}
