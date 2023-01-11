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
package org.apache.iceberg.huaweicloud.obs;

import com.obs.services.IObsClient;
import com.obs.services.ObsClient;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

public class TestOBSFileIO extends HuaweicloudOBSTestBase {
  private static final String OBS_IMPL_CLASS = OBSFileIO.class.getName();
  private final Configuration conf = new Configuration();
  private final Random random = ThreadLocalRandom.current();

  private FileIO fileIO;

  @Before
  public void beforeFile() {
    fileIO = new OBSFileIO(obsClient());
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
    writeOBSData(out, data);

    OBSURI uri = new OBSURI(location);
    Assert.assertTrue(
        "OBS file should exist", obsClient().get().doesObjectExist(uri.bucket(), uri.key()));
    Assert.assertEquals("Should have expected location", location, out.location());
    Assert.assertEquals("Should have expected length", dataSize, obsDataLength(uri));
    Assert.assertArrayEquals("Should have expected content", data, obsDataContent(uri, dataSize));
  }

  @Test
  public void testInputFile() throws IOException {
    String location = randomLocation();
    InputFile in = fileIO().newInputFile(location);
    Assert.assertFalse("OBS file should not exist", in.exists());

    int dataSize = 1024 * 10;
    byte[] data = randomData(dataSize);
    OutputFile out = fileIO().newOutputFile(location);
    writeOBSData(out, data);

    Assert.assertTrue("OBS file should exist", in.exists());
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
    writeOBSData(out, data);

    InputFile in = fileIO().newInputFile(location);
    Assert.assertTrue("OBS file should exist", in.exists());
    fileIO().deleteFile(in);
    Assert.assertFalse("OBS file should not exist", fileIO().newInputFile(location).exists());
  }

  @Test
  public void testLoadFileIO() {
    FileIO file = CatalogUtil.loadFileIO(OBS_IMPL_CLASS, ImmutableMap.of(), conf);
    Assert.assertTrue("Should be OBSFileIO", file instanceof OBSFileIO);

    byte[] data = SerializationUtil.serializeToBytes(file);
    FileIO expectedFileIO = SerializationUtil.deserializeFromBytes(data);
    Assert.assertTrue(
        "The deserialized FileIO should be OBSFileIO", expectedFileIO instanceof OBSFileIO);
  }

  @Test
  public void serializeClient() throws URISyntaxException {
    String endpoint = "iceberg-test-obs.huaweicloud.com";
    String accessKeyId = UUID.randomUUID().toString();
    String accessSecret = UUID.randomUUID().toString();
    SerializableSupplier<IObsClient> pre = () -> new ObsClient(accessKeyId, accessSecret, endpoint);

    byte[] data = SerializationUtil.serializeToBytes(pre);
    SerializableSupplier<IObsClient> post = SerializationUtil.deserializeFromBytes(data);

    IObsClient client = post.get();
    Assert.assertTrue("Should be instance of obs client", client instanceof ObsClient);
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

  private long obsDataLength(OBSURI uri) {
    return obsClient().get().getObject(uri.bucket(), uri.key()).getMetadata().getContentLength();
  }

  private byte[] obsDataContent(OBSURI uri, int dataSize) throws IOException {
    try (InputStream is = obsClient().get().getObject(uri.bucket(), uri.key()).getObjectContent()) {
      byte[] actual = new byte[dataSize];
      ByteStreams.readFully(is, actual);
      return actual;
    }
  }

  private void writeOBSData(OutputFile out, byte[] data) throws IOException {
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
