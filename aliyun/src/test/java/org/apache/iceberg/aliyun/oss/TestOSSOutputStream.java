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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.aliyun.AliyunProperties;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestOSSOutputStream extends OSSTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestOSSOutputStream.class);

  private final Random random = new Random(1);
  private final Path tmpDir = Files.createTempDirectory("oss-file-io-test-");

  public TestOSSOutputStream() throws IOException {
  }

  @Test
  public void testWriteInt() throws IOException {
    OSSURI uri = randomURI();

    AliyunProperties props = new AliyunProperties(ImmutableMap.of(
        AliyunProperties.OSS_MULTIPART_SIZE, Integer.toString(100 * 1024),
        AliyunProperties.OSS_MULTIPART_THRESHOLD_SIZE, Integer.toString(5 * 1024 * 1024),
        AliyunProperties.OSS_STAGING_DIRECTORY, tmpDir.toString()
    ));

    for (int i = 0; i < 2; i++) {
      boolean arrayWrite = i % 2 == 0;
      // Write small file that is less than multi part size.
      writeAndVerify(uri, props, data256(), arrayWrite);

      // Write file which will be uploaded in 1 part.
      writeAndVerify(uri, props, randomData(100 * 1024), arrayWrite);
      writeAndVerify(uri, props, randomData(100 * 1024 + 1), arrayWrite);

      // Write file which will be uploaded in 3 parts.
      writeAndVerify(uri, props, randomData(300 * 1024), arrayWrite);

      // Write file large enough to trigger multipart upload.
      // TODO the current local oss does not support multipart upload.
      writeAndVerify(uri, props, randomData(10 * 1024 * 1024), arrayWrite);

      // Test uploading many parts
      writeAndVerify(uri, props, randomData(30 * 1024 * 1024), arrayWrite);
    }
  }

  private void writeAndVerify(OSSURI uri, AliyunProperties props, byte[] data, boolean arrayWrite) throws IOException {
    LOG.info("Write and verify for arguments uri: {}, data length: {}, arrayWrite: {}", uri, data.length,
        arrayWrite);

    try (OSSOutputStream out = new OSSOutputStream(oss().get(), uri, props)) {
      if (arrayWrite) {
        out.write(data);
        Assert.assertEquals(data.length, out.getPos());
      } else {
        for (int i = 0; i < data.length; i++) {
          out.write(data[i]);
          Assert.assertEquals(i + 1, out.getPos());
        }
      }
    }

    InputFile inputFile = fileIO().newInputFile(uri.location());
    Assert.assertTrue(inputFile.exists());
    Assert.assertEquals(data.length, inputFile.getLength());

    byte[] actual = new byte[data.length];
    try (SeekableInputStream in = inputFile.newStream()) {
      IOUtils.readFully(in, actual);
    }
    Assert.assertArrayEquals(data, actual);

    // Verify all staging files are cleaned up.
    Assert.assertEquals(0, Files.list(Paths.get(props.ossStagingDirectory())).count());
  }

  private OSSURI randomURI() {
    return new OSSURI(location(String.format("%s.dat", UUID.randomUUID())));
  }

  private byte[] data256() {
    byte[] data = new byte[256];
    for (int i = 0; i < 256; i++) {
      data[i] = (byte) i;
    }
    return data;
  }

  private byte[] randomData(int size) {
    byte[] data = new byte[size];
    random.nextBytes(data);
    return data;
  }
}
