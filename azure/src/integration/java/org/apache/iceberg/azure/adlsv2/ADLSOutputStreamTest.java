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

import com.azure.storage.file.datalake.DataLakeFileClient;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Random;
import java.util.UUID;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.metrics.MetricsContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ADLSOutputStreamTest extends BaseAzuriteTest {

  private final Random random = new Random(1);
  private final AzureProperties azureProperties = new AzureProperties();

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testWrite(boolean arrayWrite) {
    // Test small file write
    writeAndVerify(randomData(1024), arrayWrite);

    // Test large file
    writeAndVerify(randomData(10 * 1024 * 1024), arrayWrite);
  }

  @Test
  public void testMultipleClose() throws IOException {
    DataLakeFileClient fileClient = AZURITE_CONTAINER.fileClient(randomPath());
    ADLSOutputStream stream =
        new ADLSOutputStream(fileClient, azureProperties, MetricsContext.nullMetrics());
    stream.close();
    stream.close();
  }

  private void writeAndVerify(byte[] data, boolean arrayWrite) {
    String path = randomPath();
    DataLakeFileClient fileClient = AZURITE_CONTAINER.fileClient(path);

    try (ADLSOutputStream stream =
        new ADLSOutputStream(fileClient, azureProperties, MetricsContext.nullMetrics())) {
      if (arrayWrite) {
        stream.write(data);
        assertThat(stream.getPos()).isEqualTo(data.length);
      } else {
        for (int i = 0; i < data.length; i++) {
          stream.write(data[i]);
          assertThat(stream.getPos()).isEqualTo(i + 1);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    byte[] actual = new byte[data.length];

    try (InputStream in = fileClient.openInputStream().getInputStream()) {
      IOUtil.readFully(in, actual, 0, data.length);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    assertThat(actual).isEqualTo(data);
  }

  private String randomPath() {
    return "dir/" + UUID.randomUUID();
  }

  private byte[] randomData(int size) {
    byte[] result = new byte[size];
    random.nextBytes(result);
    return result;
  }
}
