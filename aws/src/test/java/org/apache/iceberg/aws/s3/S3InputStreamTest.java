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
import java.util.Arrays;
import java.util.Random;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class S3InputStreamTest {
  @ClassRule
  public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();

  private final S3Client s3 = S3_MOCK_RULE.createS3ClientV2();
  private final Random random = new Random(1);

  @Before
  public void before() {
    s3.createBucket(CreateBucketRequest.builder().bucket("bucket").build());
  }

  @Test
  public void testRead() throws Exception {
    S3URI uri = new S3URI("s3://bucket/path/to/read.dat");
    int dataSize = 1024 * 1024 * 10;
    byte[] data = randomData(dataSize);

    writeS3Data(uri, data);

    try (SeekableInputStream in = new S3InputStream(s3, uri)) {
      int readSize = 1024;
      byte [] actual = new byte[readSize];

      readAndCheck(in, in.getPos(), readSize, data, false);
      readAndCheck(in, in.getPos(), readSize, data, true);

      // Seek forward in current stream
      int seekSize = 1024;
      readAndCheck(in, in.getPos() + seekSize, readSize, data, false);
      readAndCheck(in, in.getPos() + seekSize, readSize, data, true);

      // Buffered read
      readAndCheck(in, in.getPos(), readSize, data, true);
      readAndCheck(in, in.getPos(), readSize, data, false);

      // Seek with new stream
      long seekNewStreamPosition = 2 * 1024 * 1024;
      readAndCheck(in, in.getPos() + seekNewStreamPosition, readSize, data, true);
      readAndCheck(in, in.getPos() + seekNewStreamPosition, readSize, data, false);

      // Backseek and read
      readAndCheck(in, 0, readSize, data, true);
      readAndCheck(in, 0, readSize, data, false);
    }
  }

  private void readAndCheck(SeekableInputStream in, long rangeStart, int size, byte [] original, boolean buffered)
      throws IOException {
    in.seek(rangeStart);
    assertEquals(rangeStart, in.getPos());

    long rangeEnd = rangeStart + size;
    byte [] actual = new byte[size];

    if (buffered) {
      IOUtils.readFully(in, actual);
    } else {
      int read = 0;
      while (read < size) {
        actual[read++] = (byte) in.read();
      }
    }

    assertEquals(rangeEnd, in.getPos());
    assertArrayEquals(Arrays.copyOfRange(original, (int) rangeStart, (int) rangeEnd), actual);
  }

  @Test
  public void testClose() throws Exception {
    S3URI uri = new S3URI("s3://bucket/path/to/closed.dat");
    SeekableInputStream closed = new S3InputStream(s3, uri);
    closed.close();
    assertThrows(IllegalStateException.class, () -> closed.seek(0));
  }

  @Test
  public void testSeek() throws Exception {
    S3URI uri = new S3URI("s3://bucket/path/to/seek.dat");
    byte[] expected = randomData(1024 * 1024);

    writeS3Data(uri, expected);

    try (SeekableInputStream in = new S3InputStream(s3, uri)) {
      in.seek(expected.length / 2);
      byte[] actual = IOUtils.readFully(in, expected.length / 2);
      assertArrayEquals(Arrays.copyOfRange(expected, expected.length / 2, expected.length), actual);
    }
  }

  private byte[] randomData(int size) {
    byte[] data = new byte[size];
    random.nextBytes(data);
    return data;
  }

  private void writeS3Data(S3URI uri, byte[] data) throws IOException {
    s3.putObject(
        PutObjectRequest.builder()
            .bucket(uri.bucket())
            .key(uri.key())
            .contentLength((long) data.length)
            .build(),
        RequestBody.fromBytes(data));
  }
}
