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
import java.util.Random;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class S3OutputStreamTest {
  @ClassRule
  public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();

  private final S3Client s3 = S3_MOCK_RULE.createS3ClientV2();
  private final Random random = new Random(1);

  @Before
  public void before() {
    s3.createBucket(CreateBucketRequest.builder().bucket("bucket").build());
  }

  @Test
  public void getPos() throws IOException {
    S3URI uri = new S3URI("s3://bucket/path/to/pos.dat");
    int writeSize = 1024;

    try (S3OutputStream stream = new S3OutputStream(s3, uri)) {
      stream.write(new byte[writeSize]);
      assertEquals(writeSize, stream.getPos());
    }
  }

  @Test
  public void testWrite() throws IOException {
    S3URI uri = new S3URI("s3://bucket/path/to/out.dat");
    int size = 5 * 1024 * 1024;
    byte [] expected =  new byte[size];
    random.nextBytes(expected);

    try (S3OutputStream stream = new S3OutputStream(s3, uri)) {
      for (int i = 0; i < size; i++) {
        stream.write(expected[i]);
        assertEquals(i+1, stream.getPos());
      }
    }

    byte [] actual = readS3Data(uri);

    assertArrayEquals(expected, actual);
  }

  @Test
  public void testWriteArray() throws IOException {
    S3URI uri = new S3URI("s3://bucket/path/to/array-out.dat");
    byte [] expected =  new byte[5 * 1024 * 1024];
    random.nextBytes(expected);

    try (S3OutputStream stream = new S3OutputStream(s3, uri)) {
      stream.write(expected);
      assertEquals(expected.length, stream.getPos());
    }

    byte [] actual = readS3Data(uri);

    assertArrayEquals(expected, actual);
  }

  private byte[] readS3Data(S3URI uri) throws IOException {
    ResponseBytes<GetObjectResponse> data =
        s3.getObject(GetObjectRequest.builder().bucket(uri.bucket()).key(uri.key()).build(),
        ResponseTransformer.toBytes());

    return data.asByteArray();
  }
}
