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
package org.apache.iceberg.tencentcloud.cos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.qcloud.cos.COS;
import com.qcloud.cos.model.ObjectMetadata;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCosInputFile {
  private final COS cosMock = mock(COS.class, delegatesTo(TestUtility.cos()));
  private final Random random = ThreadLocalRandom.current();

  @Test
  public void testReadFile() throws Exception {
    CosURI uri = randomURI();

    int dataSize = 1024 * 1024 * 10;
    byte[] data = randomData(dataSize);
    writeCosData(uri, data);

    readAndVerify(uri, data);
  }

  @Test
  public void testCosInputFile() {
    CosURI uri = randomURI();
    Assertions.assertThatThrownBy(
            () ->
                new CosInputFile(
                    cosMock, uri, TestUtility.cosProperties(), -1, MetricsContext.nullMetrics()))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid file length");
  }

  @Test
  public void testExists() {
    CosURI uri = randomURI();

    InputFile inputFile =
        new CosInputFile(cosMock, uri, TestUtility.cosProperties(), MetricsContext.nullMetrics());
    assertThat(inputFile.exists()).as("Cos file should not exist").isFalse();
    verify(cosMock, times(1)).getObjectMetadata(uri.bucket(), uri.key());
    reset(cosMock);

    int dataSize = 1024;
    byte[] data = randomData(dataSize);
    writeCosData(uri, data);

    assertThat(inputFile.exists()).as("Cos file should exist").isTrue();
    inputFile.exists();
    verify(cosMock, times(1)).getObjectMetadata(uri.bucket(), uri.key());
    reset(cosMock);
  }

  @Test
  public void testGetLength() {
    CosURI uri = randomURI();

    int dataSize = 8;
    byte[] data = randomData(dataSize);
    writeCosData(uri, data);

    verifyLength(cosMock, uri, data, true);
    verify(cosMock, times(0)).getObjectMetadata(uri.bucket(), uri.key());
    reset(cosMock);

    verifyLength(cosMock, uri, data, false);
    verify(cosMock, times(1)).getObjectMetadata(uri.bucket(), uri.key());
    reset(cosMock);
  }

  private void readAndVerify(CosURI uri, byte[] data) throws IOException {
    InputFile inputFile =
        new CosInputFile(cosMock, uri, TestUtility.cosProperties(), MetricsContext.nullMetrics());
    assertThat(inputFile.exists()).as("Cos file should exist").isTrue();
    assertThat(inputFile.getLength()).as("Should have expected file length").isEqualTo(data.length);

    byte[] actual = new byte[data.length];
    try (SeekableInputStream in = inputFile.newStream()) {
      ByteStreams.readFully(in, actual);
    }
    assertThat(data).as("Should have same object content").containsExactly(actual);
  }

  private void verifyLength(COS cosClientMock, CosURI uri, byte[] data, boolean isCache) {
    InputFile inputFile;
    if (isCache) {
      inputFile =
          new CosInputFile(
              cosClientMock,
              uri,
              TestUtility.cosProperties(),
              data.length,
              MetricsContext.nullMetrics());
    } else {
      inputFile =
          new CosInputFile(
              cosClientMock, uri, TestUtility.cosProperties(), MetricsContext.nullMetrics());
    }
    inputFile.getLength();
    assertThat(inputFile.getLength()).as("Should have expected file length").isEqualTo(data.length);
  }

  private CosURI randomURI() {
    return new CosURI(TestUtility.location(String.format("%s.dat", UUID.randomUUID())));
  }

  private byte[] randomData(int size) {
    byte[] data = new byte[size];
    random.nextBytes(data);
    return data;
  }

  private void writeCosData(CosURI uri, byte[] data) {
    cosMock.putObject(
        uri.bucket(), uri.key(), new ByteArrayInputStream(data), new ObjectMetadata());
  }
}
