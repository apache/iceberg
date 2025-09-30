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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.implementation.models.InternalDataLakeFileOpenInputStreamResult;
import java.io.IOException;
import java.io.InputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TestADLSInputStream {

  @Mock private DataLakeFileClient fileClient;
  @Mock private InputStream inputStream;

  private ADLSInputStream adlsInputStream;

  @BeforeEach
  void before() {
    InternalDataLakeFileOpenInputStreamResult openInputStreamResult =
        new InternalDataLakeFileOpenInputStreamResult(inputStream, mock());
    when(fileClient.openInputStream(any())).thenReturn(openInputStreamResult);
    adlsInputStream = new ADLSInputStream(fileClient, 0L, mock(), mock());
  }

  @Test
  void testReadFullyClosesTheStream() throws IOException {
    adlsInputStream.readFully(0, new byte[0]);

    verify(inputStream).close();
  }

  @Test
  void testReadTailClosesTheStream() throws IOException {
    adlsInputStream.readTail(new byte[0], 0, 0);

    verify(inputStream).close();
  }
}
