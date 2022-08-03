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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestOSSFileIO extends AliyunOSSTestBase {
  private static final String OSS_IMPL_CLASS = OSSFileIO.class.getName();
  private final Configuration conf = new Configuration();
  private final Random random = ThreadLocalRandom.current();

  private FileIO fileIO;

  @Before
  public void beforeFile() {
    fileIO = new OSSFileIO(ossClient());
  }

  @After
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
    Assert.assertTrue(
        "OSS file should exist", ossClient().get().doesObjectExist(uri.bucket(), uri.key()));
    Assert.assertEquals("Should have expected location", location, out.location());
    Assert.assertEquals("Should have expected length", dataSize, ossDataLength(uri));
    Assert.assertArrayEquals("Should have expected content", data, ossDataContent(uri, dataSize));
  }

  @Test
  public void testInputFile() throws IOException {
    String location = randomLocation();
    InputFile in = fileIO().newInputFile(location);
    Assert.assertFalse("OSS file should not exist", in.exists());

    int dataSize = 1024 * 10;
    byte[] data = randomData(dataSize);
    OutputFile out = fileIO().newOutputFile(location);
    writeOSSData(out, data);

    Assert.assertTrue("OSS file should exist", in.exists());
    Assert.assertEquals("Should have expected location", location, in.location());
    Assert.assertEquals("Should have expected length", dataSize, in.getLength());
    Assert.assertArrayEquals("Should have expected content", data, inFileContent(in, dataSize));
  }

  @Test
  public void testDeleteFile() throws IOException {
    String location = randomLocation();
    int dataSize = 1024 * 10;
    byte[] data = randomData(dataSize);
    OutputFile out = fileIO().newOutputFile(location);
    writeOSSData(out, data);

    InputFile in = fileIO().newInputFile(location);
    Assert.assertTrue("OSS file should exist", in.exists());
    fileIO().deleteFile(in);
    Assert.assertFalse("OSS file should not exist", fileIO().newInputFile(location).exists());
  }

  @Test
  public void testLoadFileIO() {
    FileIO file = CatalogUtil.loadFileIO(OSS_IMPL_CLASS, ImmutableMap.of(), conf);
    Assert.assertTrue("Should be OSSFileIO", file instanceof OSSFileIO);

    byte[] data = SerializationUtil.serializeToBytes(file);
    FileIO expectedFileIO = SerializationUtil.deserializeFromBytes(data);
    Assert.assertTrue(
        "The deserialized FileIO should be OSSFileIO", expectedFileIO instanceof OSSFileIO);
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
    Assert.assertTrue("Should be instance of oss client", client instanceof OSSClient);

    OSSClient oss = (OSSClient) client;
    Assert.assertEquals(
        "Should have expected endpoint", new URI("http://" + endpoint), oss.getEndpoint());
    Assert.assertEquals(
        "Should have expected access key",
        accessKeyId,
        oss.getCredentialsProvider().getCredentials().getAccessKeyId());
    Assert.assertEquals(
        "Should have expected secret key",
        accessSecret,
        oss.getCredentialsProvider().getCredentials().getSecretAccessKey());
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
