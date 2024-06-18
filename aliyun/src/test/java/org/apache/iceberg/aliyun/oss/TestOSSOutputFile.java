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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.aliyun.oss.OSS;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.aliyun.AliyunProperties;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.junit.jupiter.api.Test;

public class TestOSSOutputFile extends AliyunOSSTestBase {

  private final OSS ossClient = ossClient().get();
  private final Random random = ThreadLocalRandom.current();
  private final AliyunProperties aliyunProperties = new AliyunProperties();

  @Test
  public void testWriteFile() throws IOException {
    OSSURI uri = randomURI();
    int dataSize = 1024 * 10;
    byte[] data = randomData(dataSize);

    OutputFile out = OSSOutputFile.fromLocation(ossClient, uri.location(), aliyunProperties);
    try (OutputStream os = out.create();
        InputStream is = new ByteArrayInputStream(data)) {
      ByteStreams.copy(is, os);
    }

    assertThat(ossClient.doesObjectExist(uri.bucket(), uri.key()))
        .as("OSS file should exist")
        .isTrue();

    assertThat(ossDataLength(uri)).as("Object length should match").isEqualTo(dataSize);

    byte[] actual = ossDataContent(uri, dataSize);
    assertThat(actual).as("Object content should match").isEqualTo(data);
  }

  @Test
  public void testFromLocation() {
    assertThatThrownBy(() -> OSSOutputFile.fromLocation(ossClient, null, aliyunProperties))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("location cannot be null");
  }

  @Test
  public void testCreate() {
    OSSURI uri = randomURI();
    int dataSize = 8;
    byte[] data = randomData(dataSize);

    writeOSSData(uri, data);

    OutputFile out = OSSOutputFile.fromLocation(ossClient, uri.location(), aliyunProperties);

    assertThatThrownBy(out::create)
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Location already exists");
  }

  @Test
  public void testCreateOrOverwrite() throws IOException {
    OSSURI uri = randomURI();
    int dataSize = 8;
    byte[] data = randomData(dataSize);

    writeOSSData(uri, data);

    int expectSize = 1024;
    byte[] expect = randomData(expectSize);

    OutputFile out = OSSOutputFile.fromLocation(ossClient, uri.location(), aliyunProperties);
    try (OutputStream os = out.createOrOverwrite();
        InputStream is = new ByteArrayInputStream(expect)) {
      ByteStreams.copy(is, os);
    }
    assertThat(ossDataLength(uri))
        .as(String.format("Should overwrite object length from %d to %d", dataSize, expectSize))
        .isEqualTo(expectSize);

    byte[] actual = ossDataContent(uri, expectSize);
    assertThat(actual).as("Should overwrite object content").isEqualTo(expect);
  }

  @Test
  public void testLocation() {
    OSSURI uri = randomURI();
    OutputFile out =
        new OSSOutputFile(ossClient, uri, aliyunProperties, MetricsContext.nullMetrics());
    assertThat(out.location()).as("Location should match").isEqualTo(uri.location());
  }

  @Test
  public void testToInputFile() throws IOException {
    int dataSize = 1024 * 10;
    byte[] data = randomData(dataSize);

    OutputFile out =
        new OSSOutputFile(ossClient, randomURI(), aliyunProperties, MetricsContext.nullMetrics());
    try (OutputStream os = out.create();
        InputStream is = new ByteArrayInputStream(data)) {
      ByteStreams.copy(is, os);
    }

    InputFile in = out.toInputFile();
    assertThat(in).as("Should be an instance of OSSInputFile").isInstanceOf(OSSInputFile.class);
    assertThat(in.exists()).as("OSS file should exist").isTrue();
    assertThat(in.location()).as("Should have expected location").isEqualTo(out.location());
    assertThat(in.getLength()).as("Should have expected length").isEqualTo(dataSize);

    byte[] actual = new byte[dataSize];
    try (InputStream as = in.newStream()) {
      ByteStreams.readFully(as, actual);
    }
    assertThat(actual).as("Should have expected content").isEqualTo(data);
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

  private byte[] ossDataContent(OSSURI uri, int dataSize) throws IOException {
    try (InputStream is = ossClient.getObject(uri.bucket(), uri.key()).getObjectContent()) {
      byte[] actual = new byte[dataSize];
      ByteStreams.readFully(is, actual);
      return actual;
    }
  }
}
