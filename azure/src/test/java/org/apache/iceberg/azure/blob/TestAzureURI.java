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
import org.assertj.core.api.Assertions;
import org.junit.Test;

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
    Assertions.assertThat(uri.location()).isEqualTo(expectedLocation);
    Assertions.assertThat(uri.container()).isEqualTo(expectedContainer);
    Assertions.assertThat(uri.storageAccount()).isEqualTo(expectedStorageAccount);
    Assertions.assertThat(uri.path()).isEqualTo(expectedPath);
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
          IllegalArgumentException.class,
          String.format("Invalid Azure URI. Expected form is '%s': %s", EXPECTED_ABFS_URI_FORM, invalidSchemeURI),
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
        IllegalArgumentException.class,
        String.format("Invalid Azure URI. Expected form is '%s': %s", EXPECTED_ABFS_URI_FORM, invalidURI),
        () -> {
          AzureURI.from(invalidURI);
        });
  }

  @Test
  public void testNullAuthority() {
    final String invalidAuthorityURI = "abfs:/testContainer@testStorageAccount.dfs.core.windows.net/dir1/test.txt";
    AssertHelpers.assertThrows(
        "Should not allow null authority",
        IllegalArgumentException.class,
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
          IllegalArgumentException.class,
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
        IllegalArgumentException.class,
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
        IllegalArgumentException.class,
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
        IllegalArgumentException.class,
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
        IllegalArgumentException.class,
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
          IllegalArgumentException.class,
          String.format("Invalid Azure URI. Expected form is '%s': %s", EXPECTED_ABFS_URI_FORM, emptyPathURI),
          () -> {
            AzureURI.from(emptyPathURI);
          });
    }
  }
}
