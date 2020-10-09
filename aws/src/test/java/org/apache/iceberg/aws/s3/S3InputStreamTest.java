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
import com.amazonaws.services.s3.model.ObjectMetadata;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Random;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;

public class S3InputStreamTest {
  @ClassRule
  public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();

  private final AmazonS3 s3 = S3_MOCK_RULE.createS3Client();
  private final Random random = new Random(1);

  @Before
  public void before() {
    s3.createBucket("bucket");
  }

  @Test
  public void testRead() throws Exception {
    AmazonS3URI uri = new AmazonS3URI("s3://bucket/path/to/read.dat");
    byte [] expected = randomData(1024 * 1024);

    writeS3Data(uri, expected);

    try (InputStream in = new S3InputStream(s3, uri)) {
      byte [] actual = IOUtils.readFully(in, expected.length);
      assertArrayEquals(expected, actual);
    }
  }

  @Test
  public void testClose() throws Exception {
    AmazonS3URI uri = new AmazonS3URI("s3://bucket/path/to/closed.dat");
    SeekableInputStream closed = new S3InputStream(s3, uri);
    closed.close();
    assertThrows(IllegalStateException.class, () -> closed.seek(0));
  }

  @Test
  public void testSeek() throws Exception {
    AmazonS3URI uri = new AmazonS3URI("s3://bucket/path/to/seek.dat");
    byte [] expected = randomData(1024 * 1024);

    writeS3Data(uri, expected);

    try (SeekableInputStream in = new S3InputStream(s3, uri)) {
      in.seek(expected.length / 2);
      byte [] actual = IOUtils.readFully(in, expected.length / 2);
      assertArrayEquals(Arrays.copyOfRange(expected, expected.length / 2, expected.length), actual);
    }
  }

  private byte [] randomData(int size) {
    byte [] data = new byte[size];
    random.nextBytes(data);
    return data;
  }

  private void writeS3Data(AmazonS3URI uri, byte[] data) throws IOException {
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(data.length);

    try (ByteArrayInputStream stream = new ByteArrayInputStream(data)) {
      s3.putObject(uri.getBucket(), uri.getKey(), stream, metadata);
    }
  }
}
