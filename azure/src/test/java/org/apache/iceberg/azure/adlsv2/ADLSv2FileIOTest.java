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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.azure.storage.file.datalake.DataLakeFileClient;
import java.io.IOException;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class ADLSv2FileIOTest {
  @Test
  public void testFileOperations() {
    String location = "abfs://container@account.dfs.core.windows.net/path/to/file";

    ADLSv2FileIO io = spy(new ADLSv2FileIO());
    io.initialize(ImmutableMap.of());

    DataLakeFileClient fileClient = mock(DataLakeFileClient.class);
    doReturn(fileClient).when(io).client(any());

    InputFile in = io.newInputFile(location);
    verify(fileClient, times(0)).openInputStream(any());

    io.newOutputFile(location);
    verify(fileClient, times(0)).getOutputStream(any());

    io.deleteFile(in);
    verify(fileClient).delete();
  }

  @Test
  public void testKryoSerialization() throws IOException {
    FileIO testFileIO = new ADLSv2FileIO();

    // gcs fileIO should be serializable when properties are passed as immutable map
    testFileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO = TestHelpers.KryoHelpers.roundTripSerialize(testFileIO);

    assertThat(testFileIO.properties()).isEqualTo(roundTripSerializedFileIO.properties());
  }

  @Test
  public void testJavaSerialization() throws IOException, ClassNotFoundException {
    FileIO testFileIO = new ADLSv2FileIO();

    // gcs fileIO should be serializable when properties are passed as immutable map
    testFileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO = TestHelpers.roundTripSerialize(testFileIO);

    assertThat(testFileIO.properties()).isEqualTo(roundTripSerializedFileIO.properties());
  }
}
