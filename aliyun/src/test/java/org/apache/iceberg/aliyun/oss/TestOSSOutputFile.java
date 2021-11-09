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

import com.aliyun.oss.OSS;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.aliyun.AliyunProperties;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.Test;

public class TestOSSOutputFile extends AliyunOSSTestBase {

  private final OSS ossClient = ossClient().get();
  private final Random random = ThreadLocalRandom.current();
  private final AliyunProperties aliyunProperties = new AliyunProperties();

  @Test
  public void testWriteFile() throws IOException {
    OSSURI uri = randomURI();
    int dataSize = 1024 * 10;
    byte[] data = randomData(dataSize);

    InputStream is = new ByteArrayInputStream(data);
    OutputFile out = OSSOutputFile.fromLocation(ossClient, uri.location(), aliyunProperties);
    try (OutputStream os = out.create()) {
      ByteStreams.copy(is, os);
    }

    Assert.assertTrue("OSS file should exist", ossClient.doesObjectExist(uri.bucket(), uri.key()));
    Assert.assertEquals("Object length should match", ossDataLength(uri), dataSize);

    byte[] actual = new byte[dataSize];
    ByteStreams.readFully(ossDataContent(uri), actual);
    Assert.assertArrayEquals("Object content should match", data, actual);
  }

  @Test
  public void testFromLocation() {
    AssertHelpers.assertThrows(
        "Should catch null location when creating oss output file",
        NullPointerException.class,
        "location cannot be null",
        () -> OSSOutputFile.fromLocation(ossClient, null, aliyunProperties));
  }

  @Test
  public void testCreate() {
    OSSURI uri = randomURI();
    int dataSize = 8;
    byte[] data = randomData(dataSize);

    writeOSSData(uri, data);

    OutputFile out = OSSOutputFile.fromLocation(ossClient, uri.location(), aliyunProperties);
    AssertHelpers.assertThrows("Should complain about location already exists",
        AlreadyExistsException.class, "Location already exists", out::create);
  }

  @Test
  public void testCreateOrOverwrite() throws Exception {
    OSSURI uri = randomURI();
    int dataSize = 8;
    byte[] data = randomData(dataSize);

    writeOSSData(uri, data);

    int actualSize = 1024;
    byte[] actual = randomData(actualSize);
    InputStream is = new ByteArrayInputStream(actual);
    OutputFile out = OSSOutputFile.fromLocation(ossClient, uri.location(), aliyunProperties);
    try (OutputStream os = out.createOrOverwrite()) {
      ByteStreams.copy(is, os);
    }

    Assert.assertEquals(String.format("Should overwrite object length from %d to %d", dataSize, actualSize),
        ossDataLength(uri), actualSize);

    byte[] expect = new byte[actualSize];
    ByteStreams.readFully(ossDataContent(uri), expect);
    Assert.assertArrayEquals("Should overwrite object content", actual, expect);
  }

  @Test
  public void testLocation() {
    OSSURI uri = randomURI();
    OutputFile out = new OSSOutputFile(ossClient, uri, aliyunProperties);
    Assert.assertEquals("Location should match", uri.location(), out.location());
  }

  @Test
  public void testToInputFile() throws IOException {
    int dataSize = 1024 * 10;
    byte[] data = randomData(dataSize);

    InputStream is = new ByteArrayInputStream(data);
    OutputFile out = new OSSOutputFile(ossClient, randomURI(), aliyunProperties);
    try (OutputStream os = out.create()) {
      ByteStreams.copy(is, os);
    }

    InputFile in = out.toInputFile();
    Assert.assertTrue("Should be an instance of OSSInputFile", in instanceof OSSInputFile);
    Assert.assertTrue("OSS file should exist", in.exists());
    Assert.assertEquals("Should have expected location", out.location(), in.location());
    Assert.assertEquals("Should have expected length", dataSize, in.getLength());

    byte[] actual = new byte[dataSize];
    try (InputStream as = in.newStream()) {
      ByteStreams.readFully(as, actual);
    }
    Assert.assertArrayEquals("Should have expected content", data, actual);
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

  private long ossDataLength(OSSURI uri) {
    return ossClient.getObject(uri.bucket(), uri.key()).getObjectMetadata().getContentLength();
  }

  private InputStream ossDataContent(OSSURI uri) {
    return ossClient.getObject(uri.bucket(), uri.key()).getObjectContent();
  }
}
