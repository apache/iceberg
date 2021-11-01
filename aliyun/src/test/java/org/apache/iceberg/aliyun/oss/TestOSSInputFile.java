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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.Test;

public class TestOSSInputFile extends AliyunOSSTestBase {
  private final Random random = ThreadLocalRandom.current();

  @Test
  public void testReadFile() throws Exception {
    OSSURI uri = new OSSURI(location("readfile.dat"));
    int dataSize = 1024 * 1024 * 10;
    byte[] data = randomData(dataSize);

    writeOSSData(uri, data);

    readAndVerify(uri, data);
  }

  @Test
  public void testFileExists() {
    OSSURI uri = new OSSURI(location("file.dat"));
    InputFile inputFile = new OSSInputFile(ossClient().get(), uri);
    Assert.assertFalse("OSS file should not exist", inputFile.exists());

    int dataSize = 1024;
    byte[] data = randomData(dataSize);
    writeOSSData(uri, data);

    Assert.assertTrue("OSS file should exist", inputFile.exists());

    deleteOSSData(uri);
    Assert.assertFalse("OSS file should not exist", inputFile.exists());
  }

  @Test
  public void testGetLength() {
    OSSURI uri = new OSSURI(location("filelength.dat"));
    AssertHelpers.assertThrows("File length should not be negative", ValidationException.class,
        "Invalid file length", () -> new OSSInputFile(ossClient().get(), uri, -1));

    InputFile inputFile = new OSSInputFile(ossClient().get(), uri, 1024);
    Assert.assertEquals("Should have expected file length", 1024, inputFile.getLength());

    int dataSize = 8;
    byte[] data = randomData(dataSize);
    writeOSSData(uri, data);
    inputFile = new OSSInputFile(ossClient().get(), uri);
    Assert.assertEquals("Should have expected file length", data.length, inputFile.getLength());
  }

  private void readAndVerify(OSSURI uri, byte[] data) throws IOException {
    InputFile inputFile = new OSSInputFile(ossClient().get(), uri);
    Assert.assertTrue("OSS file should exist", inputFile.exists());
    Assert.assertEquals("Should have expected file length", data.length, inputFile.getLength());

    byte[] actual = new byte[data.length];
    try (SeekableInputStream in = inputFile.newStream()) {
      ByteStreams.readFully(in, actual);
    }
    Assert.assertArrayEquals("Should have same object content", data, actual);
  }

  private byte[] randomData(int size) {
    byte[] data = new byte[size];
    random.nextBytes(data);
    return data;
  }

  private void writeOSSData(OSSURI uri, byte[] data) {
    ossClient().get().putObject(uri.bucket(), uri.key(), new ByteArrayInputStream(data));
  }

  private void deleteOSSData(OSSURI uri) {
    ossClient().get().deleteObject(uri.bucket(), uri.key());
  }
}
