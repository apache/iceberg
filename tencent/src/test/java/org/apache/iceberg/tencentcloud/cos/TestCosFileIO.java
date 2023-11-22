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

import com.qcloud.cos.COS;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.apache.iceberg.util.SerializableSupplier;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestCosFileIO {
  private static final String COS_IMPL_CLASS = CosFileIO.class.getName();
  private final Random random = ThreadLocalRandom.current();

  private FileIO fileIO;

  @BeforeEach
  public void beforeFile() {
    fileIO =
        new CosFileIO((SerializableSupplier<COS>) TestUtility::cos, TestUtility.cosProperties());
  }

  @AfterEach
  public void afterFile() {
    if (fileIO != null) {
      fileIO.close();
    }
  }

  @Test
  public void testOutputFile() throws IOException {
    String location = randomLocation();
    int dataSize = 1024 * 10;
    byte[] data = randomData(dataSize);

    OutputFile out = fileIO().newOutputFile(location);
    writeCosData(out, data);

    CosURI uri = new CosURI(location);

    assertThat(TestUtility.cos().doesObjectExist(uri.bucket(), uri.key()))
        .as("Cos file should exist")
        .isTrue();
    assertThat(location).as("Should have expected location").isEqualTo(out.location());
    assertThat(dataSize).as("Should have expected length").isEqualTo(cosDataLength(uri));
    assertThat(data)
        .as("Should have expected content")
        .containsExactly(cosDataContent(uri, dataSize));
  }

  @Test
  public void testInputFile() throws IOException {
    String location = randomLocation();
    InputFile in = fileIO().newInputFile(location);

    assertThat(in.exists()).as("Cos file should not exist").isFalse();

    int dataSize = 1024 * 10;
    byte[] data = randomData(dataSize);
    OutputFile out = fileIO().newOutputFile(location);
    writeCosData(out, data);

    assertThat(in.exists()).as("Cos file should exist").isTrue();
    assertThat(location).as("Should have expected location").isEqualTo(in.location());
    assertThat(dataSize).as("Should have expected length").isEqualTo(in.getLength());
    assertThat(data)
        .as("Should have expected content")
        .containsExactly(inFileContent(in, dataSize));
  }

  @Test
  public void testDeleteFile() throws IOException {
    String location = randomLocation();
    int dataSize = 1024 * 10;
    byte[] data = randomData(dataSize);
    OutputFile out = fileIO().newOutputFile(location);
    writeCosData(out, data);

    InputFile in = fileIO().newInputFile(location);
    assertThat(in.exists()).isTrue();
    fileIO().deleteFile(in);
    assertThat(fileIO().newInputFile(location).exists()).isFalse();
  }

  @Test
  public void testLoadFileIO() {
    FileIO file = CatalogUtil.loadFileIO(COS_IMPL_CLASS, ImmutableMap.of(), new Configuration());
    assertThat(file).isInstanceOf(CosFileIO.class);

    byte[] data = SerializationUtil.serializeToBytes(file);
    FileIO expectedFileIO = SerializationUtil.deserializeFromBytes(data);
    assertThat(expectedFileIO).isInstanceOf(CosFileIO.class);
  }

  private FileIO fileIO() {
    return fileIO;
  }

  private String randomLocation() {
    return TestUtility.location(String.format("%s.dat", UUID.randomUUID()));
  }

  private byte[] randomData(int size) {
    byte[] data = new byte[size];
    random.nextBytes(data);
    return data;
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

  private void writeCosData(OutputFile out, byte[] data) throws IOException {
    try (OutputStream os = out.create();
        InputStream is = new ByteArrayInputStream(data)) {
      ByteStreams.copy(is, os);
    }
  }

  private byte[] inFileContent(InputFile in, int dataSize) throws IOException {
    try (InputStream is = in.newStream()) {
      byte[] actual = new byte[dataSize];
      ByteStreams.readFully(is, actual);
      return actual;
    }
  }
}
