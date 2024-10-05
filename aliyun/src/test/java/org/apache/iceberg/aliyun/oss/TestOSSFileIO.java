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

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSClientBuilder;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
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

public class TestOSSFileIO extends AliyunOSSTestBase {
  private static final String OSS_IMPL_CLASS = OSSFileIO.class.getName();
  private final Configuration conf = new Configuration();
  private final Random random = ThreadLocalRandom.current();

  private FileIO fileIO;

  @BeforeEach
  public void beforeFile() {
    fileIO = new OSSFileIO(ossClient());
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
    writeOSSData(out, data);

    OSSURI uri = new OSSURI(location);
    assertThat(ossClient().get().doesObjectExist(uri.bucket(), uri.key()))
        .as("OSS file should exist")
        .isTrue();
    assertThat(out.location()).as("Should have expected location").isEqualTo(location);
    assertThat(ossDataLength(uri)).as("Should have expected length").isEqualTo(dataSize);
    assertThat(ossDataContent(uri, dataSize)).as("Should have expected content").isEqualTo(data);
  }

  @Test
  public void testInputFile() throws IOException {
    String location = randomLocation();
    InputFile in = fileIO().newInputFile(location);
    assertThat(in.exists()).as("OSS file should not exist").isFalse();

    int dataSize = 1024 * 10;
    byte[] data = randomData(dataSize);
    OutputFile out = fileIO().newOutputFile(location);
    writeOSSData(out, data);

    assertThat(in.exists()).as("OSS file should exist").isTrue();
    assertThat(in.location()).as("Should have expected location").isEqualTo(location);
    assertThat(in.getLength()).as("Should have expected length").isEqualTo(dataSize);
    assertThat(inFileContent(in, dataSize)).as("Should have expected content").isEqualTo(data);
  }

  @Test
  public void testDeleteFile() throws IOException {
    String location = randomLocation();
    int dataSize = 1024 * 10;
    byte[] data = randomData(dataSize);
    OutputFile out = fileIO().newOutputFile(location);
    writeOSSData(out, data);

    InputFile in = fileIO().newInputFile(location);
    assertThat(in.exists()).as("OSS file should exist").isTrue();

    fileIO().deleteFile(in);
    assertThat(fileIO().newInputFile(location).exists()).as("OSS file should not exist").isFalse();
  }

  @Test
  public void testLoadFileIO() {
    FileIO file = CatalogUtil.loadFileIO(OSS_IMPL_CLASS, ImmutableMap.of(), conf);
    assertThat(file).as("Should be OSSFileIO").isInstanceOf(OSSFileIO.class);

    byte[] data = SerializationUtil.serializeToBytes(file);
    FileIO expectedFileIO = SerializationUtil.deserializeFromBytes(data);
    assertThat(expectedFileIO)
        .as("The deserialized FileIO should be OSSFileIO")
        .isInstanceOf(OSSFileIO.class);
  }

  @Test
  public void serializeClient() throws URISyntaxException {
    String endpoint = "iceberg-test-oss.aliyun.com";
    String accessKeyId = UUID.randomUUID().toString();
    String accessSecret = UUID.randomUUID().toString();
    SerializableSupplier<OSS> pre =
        () -> new OSSClientBuilder().build(endpoint, accessKeyId, accessSecret);

    byte[] data = SerializationUtil.serializeToBytes(pre);
    SerializableSupplier<OSS> post = SerializationUtil.deserializeFromBytes(data);

    OSS client = post.get();
    assertThat(client).as("Should be instance of oss client").isInstanceOf(OSSClient.class);

    OSSClient oss = (OSSClient) client;
    assertThat(oss.getEndpoint())
        .as("Should have expected endpoint")
        .isEqualTo(new URI("http://" + endpoint));

    assertThat(oss.getCredentialsProvider().getCredentials().getAccessKeyId())
        .as("Should have expected access key")
        .isEqualTo(accessKeyId);
    assertThat(oss.getCredentialsProvider().getCredentials().getSecretAccessKey())
        .as("Should have expected secret key")
        .isEqualTo(accessSecret);
    assertThat(oss.getCredentialsProvider().getCredentials().getSecurityToken())
        .as("Should have no security token")
        .isNull();
  }

  private FileIO fileIO() {
    return fileIO;
  }

  private String randomLocation() {
    return location(String.format("%s.dat", UUID.randomUUID()));
  }

  private byte[] randomData(int size) {
    byte[] data = new byte[size];
    random.nextBytes(data);
    return data;
  }

  private long ossDataLength(OSSURI uri) {
    return ossClient()
        .get()
        .getObject(uri.bucket(), uri.key())
        .getObjectMetadata()
        .getContentLength();
  }

  private byte[] ossDataContent(OSSURI uri, int dataSize) throws IOException {
    try (InputStream is = ossClient().get().getObject(uri.bucket(), uri.key()).getObjectContent()) {
      byte[] actual = new byte[dataSize];
      ByteStreams.readFully(is, actual);
      return actual;
    }
  }

  private void writeOSSData(OutputFile out, byte[] data) throws IOException {
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
