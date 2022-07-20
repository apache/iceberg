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

package org.apache.iceberg.io;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestDataWriter {

  DataWriter<String> dataWriter;
  @Mock
  FileAppender<String> appender;
  @Mock
  StructLike partition;
  @Mock
  EncryptionKeyMetadata keyMetadata;
  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private File file;

  @Before
  public void before() throws IOException {
    file = temp.newFile();
    file.deleteOnExit();
    dataWriter = new DataWriter<>(appender,
        FileFormat.PARQUET,
        file.getPath(),
        PartitionSpec.unpartitioned(),
        partition,
        keyMetadata);
  }

  @Test
  public void write() {
    String row = "first_row";
    dataWriter.write(row);
    Mockito.verify(appender, Mockito.times(1)).add(row);
  }

  @Test
  public void close() throws IOException {
    Mockito.when(appender.length()).thenReturn(1L);
    Mockito.when(appender.metrics()).thenReturn(Mockito.mock(Metrics.class));
    Mockito.when(appender.splitOffsets()).thenReturn(Arrays.asList(1L));
    dataWriter.close();
    DataFile dataFile = dataWriter.toDataFile();
    Assert.assertEquals(file.getPath(), dataFile.path());
  }

  @Test
  public void closeFailureShouldNotReturnDataFile() throws IOException {
    Mockito.doThrow(new IllegalStateException("mock close failure of appender"))
        .when(appender).close();
    Assertions.assertThatThrownBy(() -> {
      try {
        dataWriter.close();
      } catch (Exception e) {
        // ignore mocked failure on first call to close
      }
      dataWriter.close();
      dataWriter.toDataFile();
    }).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void toDataFile() {
    Assertions.assertThatThrownBy(() -> dataWriter.toDataFile())
        .isInstanceOf(IllegalStateException.class);
  }
}
