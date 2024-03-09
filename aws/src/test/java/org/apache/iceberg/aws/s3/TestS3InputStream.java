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

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@ExtendWith(S3MockExtension.class)
public class TestS3InputStream {
  @RegisterExtension
  public static final S3MockExtension S3_MOCK = S3MockExtension.builder().silent().build();

  private final S3Client s3 = S3_MOCK.createS3ClientV2();
  private final Random random = new Random(1);

  @BeforeEach
  public void before() {
    createBucket("bucket");
  }

  @Test
  public void testRead() throws Exception {
    S3URI uri = new S3URI("s3://bucket/path/to/read.dat");
    int dataSize = 1024 * 1024 * 10;
    byte[] data = randomData(dataSize);

    writeS3Data(uri, data);

    try (SeekableInputStream in = new S3InputStream(s3, uri)) {
      int readSize = 1024;
      byte[] actual = new byte[readSize];

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

  private void readAndCheck(
      SeekableInputStream in, long rangeStart, int size, byte[] original, boolean buffered)
      throws IOException {
    in.seek(rangeStart);
    Assertions.assertThat(in.getPos()).isEqualTo(rangeStart);

    long rangeEnd = rangeStart + size;
    byte[] actual = new byte[size];

    if (buffered) {
      IOUtil.readFully(in, actual, 0, size);
    } else {
      int read = 0;
      while (read < size) {
        actual[read++] = (byte) in.read();
      }
    }

    Assertions.assertThat(in.getPos()).isEqualTo(rangeEnd);
    Assertions.assertThat(actual)
        .isEqualTo(Arrays.copyOfRange(original, (int) rangeStart, (int) rangeEnd));
  }

  @Test
  public void testRangeRead() throws Exception {
    S3URI uri = new S3URI("s3://bucket/path/to/range-read.dat");
    int dataSize = 1024 * 1024 * 10;
    byte[] expected = randomData(dataSize);
    byte[] actual = new byte[dataSize];

    long position;
    int offset;
    int length;

    writeS3Data(uri, expected);

    try (RangeReadable in = new S3InputStream(s3, uri)) {
      // first 1k
      position = 0;
      offset = 0;
      length = 1024;
      readAndCheckRanges(in, expected, position, actual, offset, length);

      // last 1k
      position = dataSize - 1024;
      offset = dataSize - 1024;
      readAndCheckRanges(in, expected, position, actual, offset, length);

      // middle 2k
      position = dataSize / 2 - 1024;
      offset = dataSize / 2 - 1024;
      length = 1024 * 2;
      readAndCheckRanges(in, expected, position, actual, offset, length);
    }
  }

  private void readAndCheckRanges(
      RangeReadable in, byte[] original, long position, byte[] buffer, int offset, int length)
      throws IOException {
    in.readFully(position, buffer, offset, length);

    Assertions.assertThat(Arrays.copyOfRange(buffer, offset, offset + length))
        .isEqualTo(Arrays.copyOfRange(original, offset, offset + length));
  }

  @Test
  public void testClose() throws Exception {
    S3URI uri = new S3URI("s3://bucket/path/to/closed.dat");
    SeekableInputStream closed = new S3InputStream(s3, uri);
    closed.close();
    Assertions.assertThatThrownBy(() -> closed.seek(0))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("already closed");
  }

  @Test
  public void testSeek() throws Exception {
    S3URI uri = new S3URI("s3://bucket/path/to/seek.dat");
    byte[] expected = randomData(1024 * 1024);

    writeS3Data(uri, expected);

    try (SeekableInputStream in = new S3InputStream(s3, uri)) {
      in.seek(expected.length / 2);
      byte[] actual = new byte[expected.length / 2];
      IOUtil.readFully(in, actual, 0, expected.length / 2);
      Assertions.assertThat(actual)
          .isEqualTo(Arrays.copyOfRange(expected, expected.length / 2, expected.length));
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

  private void createBucket(String bucketName) {
    try {
      s3.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
    } catch (BucketAlreadyExistsException e) {
      // don't do anything
    }
  }
}
