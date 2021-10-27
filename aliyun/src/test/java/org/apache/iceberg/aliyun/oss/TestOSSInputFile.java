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
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
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

  private void readAndVerify(OSSURI uri, byte[] data) throws IOException {
    InputFile inputFile = new OSSInputFile(ossClient().get(), uri);
    Assert.assertTrue("OSS file should exist", inputFile.exists());
    Assert.assertEquals("Should have expected file length", data.length, inputFile.getLength());

    byte[] actual = new byte[data.length];
    try (SeekableInputStream in = inputFile.newStream()) {
      IOUtils.readFully(in, actual);
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
}
