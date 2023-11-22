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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.qcloud.cos.COS;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCosOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(TestCosOutputStream.class);

  private final COS cosMock = mock(COS.class, delegatesTo(TestUtility.cos()));
  private final Path tmpDir = Files.createTempDirectory("cos-file-io-test-");
  private static final Random random = ThreadLocalRandom.current();
  private final CosProperties props =
      new CosProperties(ImmutableMap.of(CosProperties.COS_TMP_DIR, tmpDir.toString()));

  public TestCosOutputStream() throws IOException {}

  @Test
  public void testWrite() throws IOException {
    CosURI uri = randomURI();

    for (int i = 0; i < 2; i++) {
      boolean arrayWrite = i % 2 == 0;
      // Write small file.
      writeAndVerify(cosMock, uri, data256(), arrayWrite);
      verify(cosMock, times(1)).putObject(any());
      reset(cosMock);

      // Write large file.
      writeAndVerify(cosMock, uri, randomData(32 * 1024 * 1024), arrayWrite);
      verify(cosMock, times(1)).putObject(any());
      reset(cosMock);
    }
  }

  private void writeAndVerify(COS mock, CosURI uri, byte[] data, boolean arrayWrite)
      throws IOException {
    LOG.info(
        "Write and verify for arguments uri: {}, data length: {}, arrayWrite: {}",
        uri,
        data.length,
        arrayWrite);

    try (CosOutputStream out =
        new CosOutputStream(mock, uri, props, MetricsContext.nullMetrics())) {
      if (arrayWrite) {
        out.write(data);
        assertThat(data.length).as("CosOutputStream position").isEqualTo(out.getPos());
      } else {
        for (int i = 0; i < data.length; i++) {
          out.write(data[i]);
          assertThat(out.getPos()).as("CosOutputStream position").isEqualTo(i + 1);
        }
      }
    }

    assertThat(cosMock.doesObjectExist(uri.bucket(), uri.key()))
        .as("Cos object should exist")
        .isTrue();
    assertThat(cosMock.getObject(uri.bucket(), uri.key()).getObjectMetadata().getContentLength())
        .as("Object length")
        .isEqualTo(data.length);
    assertThat(data).as("Object content").containsExactly(cosDataContent(uri, data.length));

    // Verify all staging files are cleaned up.
    assertThat(Files.list(Paths.get(props.cosTmpDir())).count())
        .as("Staging files should clean up")
        .isEqualTo(0);
  }

  private CosURI randomURI() {
    return new CosURI(TestUtility.location(String.format("%s.dat", UUID.randomUUID())));
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

  private byte[] cosDataContent(CosURI uri, int dataSize) throws IOException {
    try (InputStream is = cosMock.getObject(uri.bucket(), uri.key()).getObjectContent()) {
      byte[] actual = new byte[dataSize];
      ByteStreams.readFully(is, actual);
      return actual;
    }
  }
}
