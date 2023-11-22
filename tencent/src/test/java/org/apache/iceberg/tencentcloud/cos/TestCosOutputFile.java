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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.qcloud.cos.model.ObjectMetadata;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.junit.jupiter.api.Test;

public class TestCosOutputFile {
  private final Random random = ThreadLocalRandom.current();

  @Test
  public void testWriteFile() throws IOException {
    CosURI uri = randomURI();
    int dataSize = 1024 * 10;
    byte[] data = randomData(dataSize);

    OutputFile out =
        CosOutputFile.fromLocation(TestUtility.cos(), uri.location(), TestUtility.cosProperties());
    try (OutputStream os = out.create();
        InputStream is = new ByteArrayInputStream(data)) {
      ByteStreams.copy(is, os);
    }

    assertThat(TestUtility.cos().doesObjectExist(uri.bucket(), uri.key()))
        .as("Cos file should exist")
        .isTrue();
    assertThat(cosDataLength(uri)).as("Object length should match").isEqualTo(dataSize);

    assertThat(data)
        .as("Object content should match")
        .containsExactly(cosDataContent(uri, dataSize));
  }

  @Test
  public void testFromLocation() {
    assertThatThrownBy(
            () -> CosOutputFile.fromLocation(TestUtility.cos(), null, TestUtility.cosProperties()))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("location cannot be null");
  }

  @Test
  public void testCreate() {
    CosURI uri = randomURI();
    int dataSize = 8;
    byte[] data = randomData(dataSize);

    writeCosData(uri, data);

    OutputFile out =
        CosOutputFile.fromLocation(TestUtility.cos(), uri.location(), TestUtility.cosProperties());

    assertThatThrownBy(out::create)
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Location already exists");
  }

  @Test
  public void testCreateOrOverwrite() throws IOException {
    CosURI uri = randomURI();
    int dataSize = 8;
    byte[] data = randomData(dataSize);

    writeCosData(uri, data);

    int expectSize = 1024;
    byte[] expect = randomData(expectSize);

    OutputFile out =
        CosOutputFile.fromLocation(TestUtility.cos(), uri.location(), TestUtility.cosProperties());
    try (OutputStream os = out.createOrOverwrite();
        InputStream is = new ByteArrayInputStream(expect)) {
      ByteStreams.copy(is, os);
    }

    assertThat(cosDataLength(uri))
        .as(String.format("Should overwrite object length from %d to %d", dataSize, expectSize))
        .isEqualTo(expectSize);
    assertThat(cosDataContent(uri, expectSize))
        .as("Should overwrite object content")
        .containsExactly(expect);
  }

  @Test
  public void testLocation() {
    CosURI uri = randomURI();
    OutputFile out =
        new CosOutputFile(
            TestUtility.cos(), uri, TestUtility.cosProperties(), MetricsContext.nullMetrics());
    assertThat(uri.location()).as("Location should match").isEqualTo(out.location());
  }

  @Test
  public void testToInputFile() throws IOException {
    int dataSize = 1024 * 10;
    byte[] data = randomData(dataSize);

    OutputFile out =
        new CosOutputFile(
            TestUtility.cos(),
            randomURI(),
            TestUtility.cosProperties(),
            MetricsContext.nullMetrics());
    try (OutputStream os = out.create();
        InputStream is = new ByteArrayInputStream(data)) {
      ByteStreams.copy(is, os);
    }

    InputFile in = out.toInputFile();
    assertThat(in).as("Should be an instance of CosInputFile").isInstanceOf(CosInputFile.class);
    assertThat(in.exists()).as("Cos file should exist").isTrue();
    assertThat(in.location()).as("Should have expected location").isEqualTo(out.location());
    assertThat(in.getLength()).as("Should have expected length").isEqualTo(dataSize);
    byte[] actual = new byte[dataSize];
    try (InputStream as = in.newStream()) {
      ByteStreams.readFully(as, actual);
    }
    assertThat(actual).as("Should have expected content").containsExactly(data);
  }

  private CosURI randomURI() {
    return new CosURI(TestUtility.location(String.format("%s.dat", UUID.randomUUID())));
  }

  private byte[] randomData(int size) {
    byte[] data = new byte[size];
    random.nextBytes(data);
    return data;
  }

  private void writeCosData(CosURI uri, byte[] data) {
    TestUtility.cos()
        .putObject(uri.bucket(), uri.key(), new ByteArrayInputStream(data), new ObjectMetadata());
  }

  private long cosDataLength(CosURI uri) {
    return TestUtility.cos()
        .getObject(uri.bucket(), uri.key())
        .getObjectMetadata()
        .getContentLength();
  }

  private byte[] cosDataContent(CosURI uri, int dataSize) throws IOException {
    try (InputStream is = TestUtility.cos().getObject(uri.bucket(), uri.key()).getObjectContent()) {
      byte[] actual = new byte[dataSize];
      ByteStreams.readFully(is, actual);
      return actual;
    }
  }
}
