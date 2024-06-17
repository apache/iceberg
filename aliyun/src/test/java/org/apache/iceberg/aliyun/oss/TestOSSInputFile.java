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
package org.apache.iceberg.aliyun.oss;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.aliyun.oss.OSS;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.aliyun.AliyunProperties;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.junit.jupiter.api.Test;

public class TestOSSInputFile extends AliyunOSSTestBase {
  private final OSS ossClient = ossClient().get();
  private final OSS ossMock = mock(OSS.class, delegatesTo(ossClient));

  private final AliyunProperties aliyunProperties = new AliyunProperties();
  private final Random random = ThreadLocalRandom.current();

  @Test
  public void testReadFile() throws Exception {
    OSSURI uri = randomURI();

    int dataSize = 1024 * 1024 * 10;
    byte[] data = randomData(dataSize);
    writeOSSData(uri, data);

    readAndVerify(uri, data);
  }

  @Test
  public void testOSSInputFile() {
    OSSURI uri = randomURI();
    assertThatThrownBy(
            () ->
                new OSSInputFile(
                    ossClient().get(), uri, aliyunProperties, -1, MetricsContext.nullMetrics()))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid file length");
  }

  @Test
  public void testExists() {
    OSSURI uri = randomURI();

    InputFile inputFile =
        new OSSInputFile(ossMock, uri, aliyunProperties, MetricsContext.nullMetrics());
    assertThat(inputFile.exists()).as("OSS file should not exist").isFalse();
    verify(ossMock, times(1)).getSimplifiedObjectMeta(uri.bucket(), uri.key());
    reset(ossMock);

    int dataSize = 1024;
    byte[] data = randomData(dataSize);
    writeOSSData(uri, data);

    assertThat(inputFile.exists()).as("OSS file should  exist").isTrue();
    inputFile.exists();
    verify(ossMock, times(1)).getSimplifiedObjectMeta(uri.bucket(), uri.key());
    reset(ossMock);
  }

  @Test
  public void testGetLength() {
    OSSURI uri = randomURI();

    int dataSize = 8;
    byte[] data = randomData(dataSize);
    writeOSSData(uri, data);

    verifyLength(ossMock, uri, data, true);
    verify(ossMock, times(0)).getSimplifiedObjectMeta(uri.bucket(), uri.key());
    reset(ossMock);

    verifyLength(ossMock, uri, data, false);
    verify(ossMock, times(1)).getSimplifiedObjectMeta(uri.bucket(), uri.key());
    reset(ossMock);
  }

  private void readAndVerify(OSSURI uri, byte[] data) throws IOException {
    InputFile inputFile =
        new OSSInputFile(ossClient().get(), uri, aliyunProperties, MetricsContext.nullMetrics());
    assertThat(inputFile.exists()).as("OSS file should exist").isTrue();
    assertThat(inputFile.getLength()).as("Should have expected file length").isEqualTo(data.length);

    byte[] actual = new byte[data.length];
    try (SeekableInputStream in = inputFile.newStream()) {
      ByteStreams.readFully(in, actual);
    }

    assertThat(actual).as("Should have same object content").isEqualTo(data);
  }

  private void verifyLength(OSS ossClientMock, OSSURI uri, byte[] data, boolean isCache) {
    InputFile inputFile;
    if (isCache) {
      inputFile =
          new OSSInputFile(
              ossClientMock, uri, aliyunProperties, data.length, MetricsContext.nullMetrics());
    } else {
      inputFile =
          new OSSInputFile(ossClientMock, uri, aliyunProperties, MetricsContext.nullMetrics());
    }
    inputFile.getLength();
    assertThat(inputFile.getLength()).as("Should have expected file length").isEqualTo(data.length);
  }

  private OSSURI randomURI() {
    return new OSSURI(location(String.format("%s.dat", UUID.randomUUID())));
  }

  private byte[] randomData(int size) {
    byte[] data = new byte[size];
    random.nextBytes(data);
    return data;
  }

  private void writeOSSData(OSSURI uri, byte[] data) {
    ossClient.putObject(uri.bucket(), uri.key(), new ByteArrayInputStream(data));
  }
}
