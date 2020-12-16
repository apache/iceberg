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

package org.apache.iceberg.aws.s3;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.SerializableSupplier;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class S3FileIOTest {
  @ClassRule
  public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();
  public SerializableSupplier<S3Client> s3 = S3_MOCK_RULE::createS3ClientV2;
  private final Random random = new Random(1);

  private S3FileIO s3FileIO;

  @Before
  public void before() {
    s3FileIO = new S3FileIO(s3);
    s3.get().createBucket(CreateBucketRequest.builder().bucket("bucket").build());
  }

  @Test
  public void newInputFile() throws IOException {
    String location = "s3://bucket/path/to/file.txt";
    byte [] expected = new byte[1024 * 1024];
    random.nextBytes(expected);

    InputFile in = s3FileIO.newInputFile(location);
    assertFalse(in.exists());

    OutputFile out = s3FileIO.newOutputFile(location);
    try (OutputStream os = out.createOrOverwrite()) {
      IOUtils.write(expected, os);
    }

    assertTrue(in.exists());
    byte [] actual;

    try (InputStream is = in.newStream()) {
      actual = IOUtils.readFully(is, expected.length);
    }

    assertArrayEquals(expected, actual);

    s3FileIO.deleteFile(in);

    assertFalse(s3FileIO.newInputFile(location).exists());
  }

  @Test
  public void serializeClient() {
    SerializableSupplier<S3Client> pre =
        () -> S3Client.builder().httpClient(UrlConnectionHttpClient.builder().build()).region(Region.US_EAST_1).build();

    byte [] data = SerializationUtils.serialize(pre);
    SerializableSupplier<S3Client> post = SerializationUtils.deserialize(data);

    assertEquals("s3", post.get().serviceName());
  }
}
