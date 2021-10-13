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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.aliyun.AliyunProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestAliyunOSSOutputStream extends AliyunOSSTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestAliyunOSSOutputStream.class);

  private final OSS oss = oss().get();
  private final OSS ossMock = mock(OSS.class, delegatesTo(oss));

  private final Random random = new Random(1);
  private final Path tmpDir = Files.createTempDirectory("oss-file-io-test-");

  private AliyunProperties props = new AliyunProperties(ImmutableMap.of(
      AliyunProperties.OSS_STAGING_DIRECTORY, tmpDir.toString()
  ));

  public TestAliyunOSSOutputStream() throws IOException {
  }

  @Test
  public void testWrite() throws IOException {
    AliyunOSSURI uri = randomURI();

    for (int i = 0; i < 2; i++) {
      boolean arrayWrite = i % 2 == 0;
      // Write small file.
      writeAndVerify(ossMock, uri, data256(), arrayWrite);
      verify(ossMock, times(1)).putObject(any());
      reset(ossMock);

      // Write large file.
      writeAndVerify(ossMock, uri, randomData(32 * 1024 * 1024), arrayWrite);
      verify(ossMock, times(1)).putObject(any());
      reset(ossMock);
    }
  }

  private void writeAndVerify(OSS mock, AliyunOSSURI uri, byte[] data, boolean arrayWrite)
      throws IOException {
    LOG.info("Write and verify for arguments uri: {}, data length: {}, arrayWrite: {}", uri, data.length,
        arrayWrite);

    try (AliyunOSSOutputStream out = new AliyunOSSOutputStream(mock, uri, props)) {
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

    Assert.assertTrue(oss.doesObjectExist(uri.getBucket(), uri.getKey()));
    Assert.assertEquals(
        oss.getObject(uri.getBucket(), uri.getKey()).getObjectMetadata().getContentLength(),
        data.length);
    byte[] actual = new byte[data.length];
    IOUtils.readFully(oss.getObject(uri.getBucket(), uri.getKey()).getObjectContent(), actual);
    Assert.assertArrayEquals(data, actual);

    // Verify all staging files are cleaned up.
    Assert.assertEquals(0, Files.list(Paths.get(props.ossStagingDirectory())).count());
  }

  private AliyunOSSURI randomURI() {
    return new AliyunOSSURI(location(String.format("%s.dat", UUID.randomUUID())));
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
