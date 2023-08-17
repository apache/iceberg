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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.Response;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.models.PathItem;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class ADLSFileIOTest {
  @Test
  public void testFileOperations() {
    String location = "abfs://container@account.dfs.core.windows.net/path/to/file";

    DataLakeFileClient fileClient = mock(DataLakeFileClient.class);

    DataLakeFileSystemClient client = mock(DataLakeFileSystemClient.class);
    doReturn(fileClient).when(client).getFileClient(any());

    ADLSFileIO io = spy(new ADLSFileIO());
    io.initialize(ImmutableMap.of());
    doReturn(client).when(io).client(any(ADLSLocation.class));

    InputFile in = io.newInputFile(location);
    verify(fileClient, times(0)).openInputStream(any());

    io.newOutputFile(location);
    verify(fileClient, times(0)).getOutputStream(any());

    io.deleteFile(in);
    verify(fileClient).delete();
  }

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
    doReturn(client).when(io).client(any(ADLSLocation.class));

    Iterator<FileInfo> result = io.listPrefix(prefix).iterator();

    verify(client).listPaths(any(), any());

    // assert that only files were returned and not directories
    FileInfo fileInfo = result.next();
    assertThat(fileInfo.location()).isEqualTo("dir/file");
    assertThat(fileInfo.size()).isEqualTo(123L);
    assertThat(fileInfo.createdAtMillis()).isEqualTo(now.toInstant().toEpochMilli());

    assertThat(result.hasNext()).isFalse();
  }

  @Test
  public void testDeletePrefixOperations() {
    String prefix = "abfs://container@account.dfs.core.windows.net/dir";

    Response<Void> response = mock(Response.class);

    DataLakeFileSystemClient client = mock(DataLakeFileSystemClient.class);
    when(client.deleteDirectoryWithResponse(any(), anyBoolean(), any(), any(), any()))
        .thenReturn(response);

    ADLSFileIO io = spy(new ADLSFileIO());
    io.initialize(ImmutableMap.of());
    doReturn(client).when(io).client(any(ADLSLocation.class));

    io.deletePrefix(prefix);

    // assert that recursive delete was called for the directory
    verify(client).deleteDirectoryWithResponse(eq("dir"), eq(true), any(), any(), any());
  }

  @Test
  public void testBulkDeleteFiles() {
    BlobBatchClient batchClient1 = mock(BlobBatchClient.class);
    BlobBatchClient batchClient2 = mock(BlobBatchClient.class);

    ADLSFileIO io = spy(new ADLSFileIO());
    io.initialize(ImmutableMap.of());
    doReturn(batchClient1).when(io).batchClient(startsWith("account1"));
    doReturn(batchClient2).when(io).batchClient(startsWith("account2"));

    String file1 = "abfs://container1@account1.dfs.core.windows.net/dir/file1";
    String file2 = "abfs://container2@account1.dfs.core.windows.net/dir/file2";
    String file3 = "abfs://container3@account2.dfs.core.windows.net/dir/file3";

    io.deleteFiles(ImmutableList.of(file1, file2, file3));

    ArgumentCaptor<List<String>> listCaptor = ArgumentCaptor.forClass(List.class);
    verify(batchClient1).deleteBlobs(listCaptor.capture(), any());

    List<String> blobUrls = listCaptor.getValue();
    assertThat(blobUrls).hasSize(2);
    assertThat(blobUrls)
        .contains(
            "https://account1.dfs.core.windows.net/container1/dir/file1",
            "https://account1.dfs.core.windows.net/container2/dir/file2");

    verify(batchClient2).deleteBlobs(listCaptor.capture(), any());

    blobUrls = listCaptor.getValue();
    assertThat(blobUrls).hasSize(1);
    assertThat(blobUrls).contains("https://account2.dfs.core.windows.net/container3/dir/file3");
  }

  @Test
  public void testGetClient() {
    String location = "abfs://container@account.dfs.core.windows.net/path/to/file";

    ADLSFileIO io = spy(new ADLSFileIO());
    io.initialize(ImmutableMap.of());

    io.client(location);
    verify(io).client(any(ADLSLocation.class));
  }

  @Test
  public void testKryoSerialization() throws IOException {
    FileIO testFileIO = new ADLSFileIO();

    // gcs fileIO should be serializable when properties are passed as immutable map
    testFileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO = TestHelpers.KryoHelpers.roundTripSerialize(testFileIO);

    assertThat(testFileIO.properties()).isEqualTo(roundTripSerializedFileIO.properties());
  }

  @Test
  public void testJavaSerialization() throws IOException, ClassNotFoundException {
    FileIO testFileIO = new ADLSFileIO();

    // gcs fileIO should be serializable when properties are passed as immutable map
    testFileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO = TestHelpers.roundTripSerialize(testFileIO);

    assertThat(testFileIO.properties()).isEqualTo(roundTripSerializedFileIO.properties());
  }
}
