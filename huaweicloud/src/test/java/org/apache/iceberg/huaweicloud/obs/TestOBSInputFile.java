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
package org.apache.iceberg.huaweicloud.obs;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.obs.services.IObsClient;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.huaweicloud.HuaweicloudProperties;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.Test;

public class TestOBSInputFile extends HuaweicloudOBSTestBase {
  private final IObsClient obsClient = obsClient().get();
  private final IObsClient obsMock = mock(IObsClient.class, delegatesTo(obsClient));

  private final HuaweicloudProperties huaweicloudProperties = new HuaweicloudProperties();
  private final Random random = ThreadLocalRandom.current();

  @Test
  public void testReadFile() throws Exception {
    OBSURI uri = randomURI();

    int dataSize = 1024 * 1024 * 10;
    byte[] data = randomData(dataSize);
    writeOBSData(uri, data);

    readAndVerify(uri, data);
  }

  @Test
  public void testOBSInputFile() {
    OBSURI uri = randomURI();
    AssertHelpers.assertThrows(
        "File length should not be negative",
        ValidationException.class,
        "Invalid file length",
        () ->
            new OBSInputFile(
                obsClient().get(), uri, huaweicloudProperties, -1, MetricsContext.nullMetrics()));
  }

  @Test
  public void testExists() {
    OBSURI uri = randomURI();

    InputFile inputFile =
        new OBSInputFile(obsMock, uri, huaweicloudProperties, MetricsContext.nullMetrics());
    Assert.assertFalse("OBS file should not exist", inputFile.exists());
    verify(obsMock, times(1)).getObjectMetadata(uri.bucket(), uri.key());
    reset(obsMock);

    int dataSize = 1024;
    byte[] data = randomData(dataSize);
    writeOBSData(uri, data);

    Assert.assertTrue("OBS file should exist", inputFile.exists());
    inputFile.exists();
    verify(obsMock, times(1)).getObjectMetadata(uri.bucket(), uri.key());
    reset(obsMock);
  }

  @Test
  public void testGetLength() {
    OBSURI uri = randomURI();

    int dataSize = 8;
    byte[] data = randomData(dataSize);
    writeOBSData(uri, data);

    verifyLength(obsMock, uri, data, true);
    verify(obsMock, times(0)).getObjectMetadata(uri.bucket(), uri.key());
    reset(obsMock);

    verifyLength(obsMock, uri, data, false);
    verify(obsMock, times(1)).getObjectMetadata(uri.bucket(), uri.key());
    reset(obsMock);
  }

  private void readAndVerify(OBSURI uri, byte[] data) throws IOException {
    InputFile inputFile =
        new OBSInputFile(
            obsClient().get(), uri, huaweicloudProperties, MetricsContext.nullMetrics());
    Assert.assertTrue("OBS file should exist", inputFile.exists());
    Assert.assertEquals("Should have expected file length", data.length, inputFile.getLength());

    byte[] actual = new byte[data.length];
    try (SeekableInputStream in = inputFile.newStream()) {
      ByteStreams.readFully(in, actual);
    }
    Assert.assertArrayEquals("Should have same object content", data, actual);
  }

  private void verifyLength(IObsClient obsClientMock, OBSURI uri, byte[] data, boolean isCache) {
    InputFile inputFile;
    if (isCache) {
      inputFile =
          new OBSInputFile(
              obsClientMock, uri, huaweicloudProperties, data.length, MetricsContext.nullMetrics());
    } else {
      inputFile =
          new OBSInputFile(obsClientMock, uri, huaweicloudProperties, MetricsContext.nullMetrics());
    }
    inputFile.getLength();
    Assert.assertEquals("Should have expected file length", data.length, inputFile.getLength());
  }

  private OBSURI randomURI() {
    return new OBSURI(location(String.format("%s.dat", UUID.randomUUID())));
  }

  private byte[] randomData(int size) {
    byte[] data = new byte[size];
    random.nextBytes(data);
    return data;
  }

  private void writeOBSData(OBSURI uri, byte[] data) {
    obsClient.putObject(uri.bucket(), uri.key(), new ByteArrayInputStream(data));
  }
}
