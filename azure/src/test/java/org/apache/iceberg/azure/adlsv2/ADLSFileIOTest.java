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
import com.azure.storage.file.datalake.models.PathItem;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.OffsetDateTime;
import java.util.Iterator;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class ADLSFileIOTest extends BaseAzuriteTest {

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
    doReturn(client).when(io).client(any(ADLSLocation.class));

    io.deletePrefix(prefix);

    // assert that recursive delete was called for the directory
    verify(client).deleteDirectoryWithResponse(eq("dir"), eq(true), any(), any(), any());
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
