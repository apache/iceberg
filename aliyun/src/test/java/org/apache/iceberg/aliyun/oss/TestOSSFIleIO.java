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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.aliyun.oss.mock.OSSMockRule;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.SerializableSupplier;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class TestOSSFIleIO {

  @ClassRule
  public static final OSSMockRule OSS_MOCK_RULE = OSSMockRule.builder().silent().build();
  private final SerializableSupplier<OSS> oss = OSS_MOCK_RULE::createOSSClient;
  private final Random random = new Random(1);

  private OSSFileIO ossFileIO;

  @Before
  public void before() {
    ossFileIO = new OSSFileIO(oss);
    oss.get().createBucket("bucket");
  }

  @After
  public void after() {
    oss.get().deleteBucket("bucket");
  }

  @Test
  public void newInputFile() throws IOException {
    String location = "oss://bucket/path/to/file.txt";
    byte[] expected = new byte[1024 * 1024];
    random.nextBytes(expected);

    InputFile in = ossFileIO.newInputFile(location);
    Assert.assertFalse(in.exists());

    OutputFile out = ossFileIO.newOutputFile(location);
    try (OutputStream os = out.createOrOverwrite()) {
      IOUtils.write(expected, os);
    }

    Assert.assertTrue(in.exists());

    byte[] actual = new byte[1024 * 1024];
    try (InputStream is = in.newStream()) {
      IOUtils.readFully(is, actual);
    }
    Assert.assertArrayEquals(expected, actual);

    ossFileIO.deleteFile(in);
    Assert.assertFalse(ossFileIO.newInputFile(location).exists());
  }

  @Test
  public void serializeClient() throws URISyntaxException {
    String endpoint = "iceberg-test-oss.aliyun.com";
    String accessKeyId = UUID.randomUUID().toString();
    String accessSecret = UUID.randomUUID().toString();
    SerializableSupplier<OSS> pre = () -> new OSSClientBuilder().build(endpoint, accessKeyId, accessSecret);

    byte[] data = SerializationUtil.serializeToBytes(pre);
    SerializableSupplier<OSS> post = SerializationUtil.deserializeFromBytes(data);

    OSS client = post.get();
    Assert.assertTrue(client instanceof OSSClient);

    OSSClient ossClient = (OSSClient) client;
    Assert.assertEquals(new URI("http://" + endpoint), ossClient.getEndpoint());
    Assert.assertEquals(accessKeyId, ossClient.getCredentialsProvider().getCredentials().getAccessKeyId());
    Assert.assertEquals(accessSecret, ossClient.getCredentialsProvider().getCredentials().getSecretAccessKey());
  }
}
