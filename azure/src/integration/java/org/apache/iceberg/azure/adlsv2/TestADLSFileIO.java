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

import static org.apache.iceberg.azure.AzureProperties.ADLS_SAS_TOKEN_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.Response;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import com.azure.storage.file.datalake.models.PathItem;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.OffsetDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedConstruction;

public class TestADLSFileIO extends AzuriteTestBase {

  @Test
  public void testFileOperations() throws IOException {
    String path = "path/to/file";
    String location = AZURITE_CONTAINER.location(path);
    ADLSFileIO io = createFileIO();
    DataLakeFileClient fileClient = AZURITE_CONTAINER.fileClient(path);

    assertThat(fileClient.exists()).isFalse();
    OutputFile outputFile = io.newOutputFile(location);
    try (OutputStream out = outputFile.create()) {
      out.write(123);
    }
    assertThat(fileClient.exists()).isTrue();

    InputFile inputFile = io.newInputFile(location);
    try (InputStream in = inputFile.newStream()) {
      int byteVal = in.read();
      assertThat(byteVal).isEqualTo(123);
    }

    io.deleteFile(location);
    assertThat(fileClient.exists()).isFalse();
  }

  @Test
  public void testBulkDeleteFiles() {
    String path1 = "path/to/file1";
    String location1 = AZURITE_CONTAINER.location(path1);
    AZURITE_CONTAINER.createFile(path1, new byte[] {123});
    assertThat(AZURITE_CONTAINER.fileClient(path1).exists()).isTrue();

    String path2 = "path/to/file2";
    String location2 = AZURITE_CONTAINER.location(path2);
    AZURITE_CONTAINER.createFile(path2, new byte[] {123});
    assertThat(AZURITE_CONTAINER.fileClient(path2).exists()).isTrue();

    ADLSFileIO io = createFileIO();
    io.deleteFiles(ImmutableList.of(location1, location2));

    assertThat(AZURITE_CONTAINER.fileClient(path1).exists()).isFalse();
    assertThat(AZURITE_CONTAINER.fileClient(path2).exists()).isFalse();
  }

  @Test
  public void testGetClient() {
    String location = AZURITE_CONTAINER.location("path/to/file");
    ADLSFileIO io = createFileIO();
    DataLakeFileSystemClient client = io.client(location);
    assertThat(client.exists()).isTrue();
  }

  @Test
  public void testApplyClientConfigurationWithSas() {
    try (MockedConstruction<AzureProperties> azurePropertiesMockedConstruction =
        mockAzurePropertiesConstruction()) {
      ADLSFileIO io = new ADLSFileIO();
      io.initialize(
          ImmutableMap.of(ADLS_SAS_TOKEN_PREFIX + "account.dfs.core.windows.net", "sasToken"));
      String location = AZURITE_CONTAINER.location("path/to/file");
      io.client(location);
      assertThat(azurePropertiesMockedConstruction.constructed()).hasSize(1);
      verify(azurePropertiesMockedConstruction.constructed().get(0))
          .applyClientConfiguration(
              eq("account.dfs.core.windows.net"), any(DataLakeFileSystemClientBuilder.class));
    }
  }

  /** Azurite does not support ADLSv2 directory operations yet so use mocks here. */
  @SuppressWarnings("unchecked")
  @Test
  public void testListPrefixOperations() {
    String prefix = "abfs://container@account.dfs.core.windows.net/dir";

    OffsetDateTime now = OffsetDateTime.now();
    PathItem dir =
        new PathItem("tag", now, 0L, "group", true, "dir", "owner", "permissions", now, null);
    PathItem file =
        new PathItem(
            "tag", now, 123L, "group", false, "dir/file", "owner", "permissions", now, null);

    PagedIterable<PathItem> response = mock(PagedIterable.class);
    when(response.stream()).thenReturn(ImmutableList.of(dir, file).stream());

    DataLakeFileSystemClient client = mock(DataLakeFileSystemClient.class);
    when(client.listPaths(any(), any())).thenReturn(response);

    ADLSFileIO io = spy(new ADLSFileIO());
    io.initialize(ImmutableMap.of());
    doReturn(client).when(io).client(anyString());

    Iterator<FileInfo> result = io.listPrefix(prefix).iterator();

    verify(client).listPaths(any(), any());

    // assert that only files were returned and not directories
    FileInfo fileInfo = result.next();
    assertThat(fileInfo.location()).isEqualTo("dir/file");
    assertThat(fileInfo.size()).isEqualTo(123L);
    assertThat(fileInfo.createdAtMillis()).isEqualTo(now.toInstant().toEpochMilli());

    assertThat(result.hasNext()).isFalse();
  }

  /** Azurite does not support ADLSv2 directory operations yet so use mocks here. */
  @SuppressWarnings("unchecked")
  @Test
  public void testDeletePrefixOperations() {
    String prefix = "abfs://container@account.dfs.core.windows.net/dir";

    Response<Void> response = mock(Response.class);

    DataLakeFileSystemClient client = mock(DataLakeFileSystemClient.class);
    when(client.deleteDirectoryWithResponse(any(), anyBoolean(), any(), any(), any()))
        .thenReturn(response);

    ADLSFileIO io = spy(new ADLSFileIO());
    io.initialize(ImmutableMap.of());
    doReturn(client).when(io).client(anyString());

    io.deletePrefix(prefix);

    // assert that recursive delete was called for the directory
    verify(client).deleteDirectoryWithResponse(eq("dir"), eq(true), any(), any(), any());
  }

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  public void testSerialization(TestHelpers.RoundTripSerializer<FileIO> roundTripSerializer)
      throws IOException, ClassNotFoundException {
    FileIO testFileIO = new ADLSFileIO();

    // gcs fileIO should be serializable when properties are passed as immutable map
    testFileIO.initialize(ImmutableMap.of("k1", "v1", "k2", "v2"));
    FileIO roundTripSerializedFileIO = roundTripSerializer.apply(testFileIO);

    assertThat(testFileIO.properties()).isEqualTo(roundTripSerializedFileIO.properties());
  }

  @Test
  public void noStorageCredentialConfigured() {
    ADLSFileIO fileIO = new ADLSFileIO();
    fileIO.setCredentials(ImmutableList.of());
    fileIO.initialize(
        ImmutableMap.of(
            sasTokenForAccount("account1"), "sasTokenFromProperties1",
            sasTokenForAccount("account2"), "sasTokenFromProperties2"));

    assertThat(fileIO.clientForStoragePath(abfsPath("my-container1", "account1", "/path/to/file")))
        .isSameAs(
            fileIO.clientForStoragePath(abfsPath("my-container2", "account2", "/path/to/file")))
        .isSameAs(fileIO.clientForStoragePath(wasbPath("container", "account", "/path/to/file")))
        .isSameAs(fileIO.clientForStoragePath(abfsPath("random", "account", "/path/to/file")));

    assertThat(
            fileIO
                .clientForStoragePath(abfsPath("my-container1", "account1", "/path/to/file"))
                .azureProperties())
        .extracting("adlsSasTokens")
        .asInstanceOf(InstanceOfAssertFactories.map(String.class, String.class))
        .containsEntry(accountHost("account1"), "sasTokenFromProperties1")
        .containsEntry(accountHost("account2"), "sasTokenFromProperties2");
  }

  @Test
  public void singleStorageCredentialConfigured() {
    StorageCredential adlsCredential =
        StorageCredential.create(
            abfsPath("custom-container", "account1", ""),
            ImmutableMap.of(sasTokenForAccount("account1"), "sasTokenFromCredential"));
    ADLSFileIO fileIO = new ADLSFileIO();
    fileIO.setCredentials(ImmutableList.of(adlsCredential));
    fileIO.initialize(
        ImmutableMap.of(
            sasTokenForAccount("account1"), "sasTokenFromProperties1",
            sasTokenForAccount("account2"), "sasTokenFromProperties2"));

    // verify that the generic ADLS client is used if the storage prefix doesn't match the prefixes
    // in the storage credentials
    assertThat(
            fileIO.clientForStoragePath(abfsPath("custom-container", "account1", "/path/to/file")))
        .isNotSameAs(
            fileIO.clientForStoragePath(abfsPath("my-container", "account1", "/path/to/file")));

    assertThat(fileIO.clientForStoragePath(abfsPath("my-container", "account1", "/path/to/file")))
        .isSameAs(
            fileIO.clientForStoragePath(abfsPath("my-container2", "account2", "/path/to/file")));

    // Custom credentials from storage credentials.
    assertThat(
            fileIO
                .clientForStoragePath(abfsPath("custom-container", "account1", "/path/to/file"))
                .azureProperties())
        .extracting("adlsSasTokens")
        .asInstanceOf(InstanceOfAssertFactories.map(String.class, String.class))
        .containsEntry(accountHost("account1"), "sasTokenFromCredential")
        .containsEntry(accountHost("account2"), "sasTokenFromProperties2");

    // Generic credentials from properties
    assertThat(
            fileIO
                .clientForStoragePath(abfsPath("my-container1", "account1", "/path/to/file"))
                .azureProperties())
        .extracting("adlsSasTokens")
        .asInstanceOf(InstanceOfAssertFactories.map(String.class, String.class))
        .containsEntry(accountHost("account1"), "sasTokenFromProperties1")
        .containsEntry(accountHost("account2"), "sasTokenFromProperties2");
  }

  @Test
  public void multipleStorageCredentialsConfigured() {
    StorageCredential adlsCredential1 =
        StorageCredential.create(
            abfsPath("custom-container", "account1", "/table1"),
            ImmutableMap.of(sasTokenForAccount("account1"), "sasTokenFromCredential1"));
    StorageCredential adlsCredential2 =
        StorageCredential.create(
            abfsPath("custom-container", "account1", "/table2"),
            ImmutableMap.of(sasTokenForAccount("account1"), "sasTokenFromCredential2"));
    ADLSFileIO fileIO = new ADLSFileIO();
    fileIO.setCredentials(ImmutableList.of(adlsCredential1, adlsCredential2));
    fileIO.initialize(
        ImmutableMap.of(
            sasTokenForAccount("account1"), "sasTokenFromProperties1",
            sasTokenForAccount("account2"), "sasTokenFromProperties2"));

    // verify that the generic ADLS client is used if the storage prefix doesn't match the prefixes
    // in the storage credentials
    assertThat(fileIO.clientForStoragePath(abfsPath("custom-container", "account1", "/table1")))
        .isNotSameAs(
            fileIO.clientForStoragePath(abfsPath("custom-container", "account1", "/table2")))
        .isNotSameAs(
            fileIO.clientForStoragePath(abfsPath("my-container", "account1", "/path/to/file")));

    // Custom credentials from storage credentials for prefix table1.
    assertThat(
            fileIO
                .clientForStoragePath(abfsPath("custom-container", "account1", "/table1"))
                .azureProperties())
        .extracting("adlsSasTokens")
        .asInstanceOf(InstanceOfAssertFactories.map(String.class, String.class))
        .containsEntry(accountHost("account1"), "sasTokenFromCredential1")
        .containsEntry(accountHost("account2"), "sasTokenFromProperties2");

    // Custom credentials from storage credentials for prefix table2.
    assertThat(
            fileIO
                .clientForStoragePath(abfsPath("custom-container", "account1", "/table2"))
                .azureProperties())
        .extracting("adlsSasTokens")
        .asInstanceOf(InstanceOfAssertFactories.map(String.class, String.class))
        .containsEntry(accountHost("account1"), "sasTokenFromCredential2")
        .containsEntry(accountHost("account2"), "sasTokenFromProperties2");

    // Generic credentials from properties
    assertThat(
            fileIO
                .clientForStoragePath(abfsPath("my-container1", "account1", "/path/to/file"))
                .azureProperties())
        .extracting("adlsSasTokens")
        .asInstanceOf(InstanceOfAssertFactories.map(String.class, String.class))
        .containsEntry(accountHost("account1"), "sasTokenFromProperties1")
        .containsEntry(accountHost("account2"), "sasTokenFromProperties2");
  }

  @Test
  public void resolvingFileIOLoadWithStorageCredentials()
      throws IOException, ClassNotFoundException {
    StorageCredential credential =
        StorageCredential.create("abfs://foo/bar", Map.of("key1", "val1"));
    List<StorageCredential> storageCredentials = ImmutableList.of(credential);
    ResolvingFileIO resolvingFileIO = new ResolvingFileIO();
    resolvingFileIO.setCredentials(storageCredentials);
    resolvingFileIO.initialize(ImmutableMap.of());

    validateResolvingFileIO(resolvingFileIO, storageCredentials);

    // make sure credentials are still present after kryo serde
    ResolvingFileIO resolvingIO = TestHelpers.KryoHelpers.roundTripSerialize(resolvingFileIO);
    validateResolvingFileIO(resolvingIO, storageCredentials);

    // make sure credentials are still present after java serde
    resolvingIO = TestHelpers.roundTripSerialize(resolvingFileIO);
    validateResolvingFileIO(resolvingIO, storageCredentials);
  }

  private void validateResolvingFileIO(
      ResolvingFileIO resolvingIO, List<StorageCredential> storageCredentials) {
    assertThat(resolvingIO.credentials()).isEqualTo(storageCredentials);
    FileIO result =
        DynMethods.builder("io")
            .hiddenImpl(ResolvingFileIO.class, String.class)
            .build(resolvingIO)
            .invoke("abfs://foo/bar");
    ObjectAssert<ADLSFileIO> io =
        assertThat(result)
            .isInstanceOf(ADLSFileIO.class)
            .asInstanceOf(InstanceOfAssertFactories.type(ADLSFileIO.class));
    io.extracting(ADLSFileIO::credentials).isEqualTo(storageCredentials);
    io.satisfies(
        fileIO -> {
          // make sure there are two separate clients for different prefixes
          assertThat(fileIO.clientForStoragePath("abfs://foo/bar"))
              .isNotSameAs(fileIO.clientForStoragePath("abfs://foo"));
        });
  }

  private String sasTokenForAccount(String account) {
    return ADLS_SAS_TOKEN_PREFIX + accountHost(account);
  }

  private String accountHost(String account) {
    return account + ".dfs.core.windows.net";
  }

  private String abfsPath(String container, String account, String path) {
    return String.format("abfs://%s@%s.dfs.core.windows.net%s", container, account, path);
  }

  private String wasbPath(String container, String account, String path) {
    return String.format("wasb://%s@%s.blob.core.windows.net%s", container, account, path);
  }
}
