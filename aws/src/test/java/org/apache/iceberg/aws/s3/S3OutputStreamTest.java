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
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.S3Object;
import java.io.IOException;
import java.util.Random;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class S3OutputStreamTest {
  @ClassRule
  public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();

  private final AmazonS3 s3 = S3_MOCK_RULE.createS3Client();
  private final Random random = new Random(1);

  @Before
  public void before() {
    s3.createBucket("bucket");
  }

  @Test
  public void getPos() throws IOException {
    AmazonS3URI uri = new AmazonS3URI("s3://bucket/path/to/pos.dat");
    int writeSize = 1024;

    try (S3OutputStream stream = new S3OutputStream(s3, uri)) {
      stream.write(new byte[writeSize]);
      assertEquals(writeSize, stream.getPos());
    }
  }

  @Test
  public void testWrite() throws IOException {
    AmazonS3URI uri = new AmazonS3URI("s3://bucket/path/to/out.dat");
    byte [] expected =  new byte[5 * 1024 * 1024];
    random.nextBytes(expected);

    try (S3OutputStream stream = new S3OutputStream(s3, uri)) {
      stream.write(expected);
    }

    byte [] actual = readS3Data(uri);

    assertArrayEquals(expected, actual);
  }

  private byte[] readS3Data(AmazonS3URI uri) throws IOException {
    S3Object object = s3.getObject(uri.getBucket(), uri.getKey());
    long length = object.getObjectMetadata().getContentLength();
    return IOUtils.readFully(object.getObjectContent(), (int) length);
  }
}
