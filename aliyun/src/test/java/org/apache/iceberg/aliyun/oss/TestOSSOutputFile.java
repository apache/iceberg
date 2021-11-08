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
import java.io.OutputStream;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.io.IOUtils;
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

    int dataSize = 1024 * 1024 * 10;
    byte[] data = randomData(dataSize);

    OutputFile out = OSSOutputFile.fromLocation(ossClient, uri.location(), aliyunProperties);
    try (OutputStream os = out.createOrOverwrite()) {
      IOUtils.write(data, os);
    }
    InputFile in = out.toInputFile();
    Assert.assertTrue("OSS file should exist", in.exists());

    byte[] actual = new byte[dataSize];
    ByteStreams.readFully(ossClient.getObject(uri.bucket(), uri.key()).getObjectContent(), actual);
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
    AssertHelpers.assertThrows("Should complain about location already exists", AlreadyExistsException.class,
        "Location already exists",
        () -> out.create());
  }

  @Test
  public void testCreateOrOverwrite() throws Exception {
    OSSURI uri = randomURI();

    int dataSize = 8;
    byte[] data = randomData(dataSize);
    writeOSSData(uri, data);

    int actualSize = 1024;
    byte[] actual = randomData(actualSize);
    OutputFile out = OSSOutputFile.fromLocation(ossClient, uri.location(), aliyunProperties);
    try (OutputStream os = out.createOrOverwrite()) {
      IOUtils.write(actual, os);
    }

    Assert.assertEquals(String.format("Should overwrite object length from %d to %d", dataSize, actualSize),
        ossClient.getObject(uri.bucket(), uri.key()).getObjectMetadata().getContentLength(), actualSize);

    byte[] expect = new byte[actualSize];
    ByteStreams.readFully(ossClient.getObject(uri.bucket(), uri.key()).getObjectContent(), expect);
    Assert.assertArrayEquals("Should overwrite object content", actual, expect);
  }

  @Test
  public void testLocation() {
    OSSURI uri = randomURI();
    OutputFile out = new OSSOutputFile(ossClient, uri, aliyunProperties);
    Assert.assertEquals("Location should match", uri.location(), out.location());
  }

  @Test
  public void testToInputFile() {
    OutputFile out = new OSSOutputFile(ossClient, randomURI(), aliyunProperties);
    InputFile in = out.toInputFile();
    Assert.assertTrue("Should be an instance of OSSInputFile", in instanceof OSSInputFile);
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
