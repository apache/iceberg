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

import com.obs.services.IObsClient;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.huaweicloud.HuaweicloudProperties;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.Test;

public class TestOBSOutputFile extends HuaweicloudOBSTestBase {

  private final IObsClient obsClient = obsClient().get();
  private final Random random = ThreadLocalRandom.current();
  private final HuaweicloudProperties huaweicloudProperties = new HuaweicloudProperties();

  @Test
  public void testWriteFile() throws IOException {
    OBSURI uri = randomURI();
    int dataSize = 1024 * 10;
    byte[] data = randomData(dataSize);

    OutputFile out = OBSOutputFile.fromLocation(obsClient, uri.location(), huaweicloudProperties);
    try (OutputStream os = out.create();
        InputStream is = new ByteArrayInputStream(data)) {
      ByteStreams.copy(is, os);
    }

    Assert.assertTrue("OBS file should exist", obsClient.doesObjectExist(uri.bucket(), uri.key()));
    Assert.assertEquals("Object length should match", obsDataLength(uri), dataSize);

    byte[] actual = obsDataContent(uri, dataSize);
    Assert.assertArrayEquals("Object content should match", data, actual);
  }

  @Test
  public void testFromLocation() {
    AssertHelpers.assertThrows(
        "Should catch null location when creating obs output file",
        NullPointerException.class,
        "location cannot be null",
        () -> OBSOutputFile.fromLocation(obsClient, null, huaweicloudProperties));
  }

  @Test
  public void testCreate() {
    OBSURI uri = randomURI();
    int dataSize = 8;
    byte[] data = randomData(dataSize);

    writeOBSData(uri, data);

    OutputFile out = OBSOutputFile.fromLocation(obsClient, uri.location(), huaweicloudProperties);
    AssertHelpers.assertThrows(
        "Should complain about location already exists",
        AlreadyExistsException.class,
        "Location already exists",
        out::create);
  }

  @Test
  public void testCreateOrOverwrite() throws IOException {
    OBSURI uri = randomURI();
    int dataSize = 8;
    byte[] data = randomData(dataSize);

    writeOBSData(uri, data);

    int expectSize = 1024;
    byte[] expect = randomData(expectSize);

    OutputFile out = OBSOutputFile.fromLocation(obsClient, uri.location(), huaweicloudProperties);
    try (OutputStream os = out.createOrOverwrite();
        InputStream is = new ByteArrayInputStream(expect)) {
      ByteStreams.copy(is, os);
    }

    Assert.assertEquals(
        String.format("Should overwrite object length from %d to %d", dataSize, expectSize),
        expectSize,
        obsDataLength(uri));

    byte[] actual = obsDataContent(uri, expectSize);
    Assert.assertArrayEquals("Should overwrite object content", expect, actual);
  }

  @Test
  public void testLocation() {
    OBSURI uri = randomURI();
    OutputFile out =
        new OBSOutputFile(obsClient, uri, huaweicloudProperties, MetricsContext.nullMetrics());
    Assert.assertEquals("Location should match", uri.location(), out.location());
  }

  @Test
  public void testToInputFile() throws IOException {
    int dataSize = 1024 * 10;
    byte[] data = randomData(dataSize);

    OutputFile out =
        new OBSOutputFile(
            obsClient, randomURI(), huaweicloudProperties, MetricsContext.nullMetrics());
    try (OutputStream os = out.create();
        InputStream is = new ByteArrayInputStream(data)) {
      ByteStreams.copy(is, os);
    }

    InputFile in = out.toInputFile();
    Assert.assertTrue("Should be an instance of OBSInputFile", in instanceof OBSInputFile);
    Assert.assertTrue("OBS file should exist", in.exists());
    Assert.assertEquals("Should have expected location", out.location(), in.location());
    Assert.assertEquals("Should have expected length", dataSize, in.getLength());

    byte[] actual = new byte[dataSize];
    try (InputStream as = in.newStream()) {
      ByteStreams.readFully(as, actual);
    }
    Assert.assertArrayEquals("Should have expected content", data, actual);
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

  private long obsDataLength(OBSURI uri) {
    return obsClient.getObject(uri.bucket(), uri.key()).getMetadata().getContentLength();
  }

  private byte[] obsDataContent(OBSURI uri, int dataSize) throws IOException {
    try (InputStream is = obsClient.getObject(uri.bucket(), uri.key()).getObjectContent()) {
      byte[] actual = new byte[dataSize];
      ByteStreams.readFully(is, actual);
      return actual;
    }
  }
}
