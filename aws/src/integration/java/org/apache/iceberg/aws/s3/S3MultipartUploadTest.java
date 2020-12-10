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

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.iceberg.aws.AwsClientUtil;
import org.apache.iceberg.aws.AwsIntegTestUtil;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Long-running tests to ensure multipart upload logic is resilient
 */
public class S3MultipartUploadTest {

  private final Random random = new Random(1);
  private static S3Client s3;
  private static String bucketName;
  private static String prefix;
  private String objectUri;

  @BeforeClass
  public static void beforeClass() {
    s3 = AwsClientUtil.defaultS3Client();
    bucketName = AwsIntegTestUtil.testBucketName();
    prefix = UUID.randomUUID().toString();
  }

  @AfterClass
  public static void afterClass() {
    AwsIntegTestUtil.cleanS3Bucket(s3, bucketName, prefix);
  }

  @Before
  public void before() {
    String objectKey = String.format("%s/%s", prefix, UUID.randomUUID().toString());
    objectUri = String.format("s3://%s/%s", bucketName, objectKey);
  }

  @Test
  public void testManyParts_writeWithInt() throws IOException {
    AwsProperties properties = new AwsProperties();
    properties.setS3FileIoMultiPartSize(AwsProperties.S3FILEIO_MULTIPART_SIZE_MIN);
    S3FileIO io = new S3FileIO(() -> s3, properties);
    PositionOutputStream outputStream = io.newOutputFile(objectUri).create();
    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < AwsProperties.S3FILEIO_MULTIPART_SIZE_MIN; j++) {
        outputStream.write(random.nextInt());
      }
    }
    outputStream.close();
    Assert.assertEquals(100 * (long) AwsProperties.S3FILEIO_MULTIPART_SIZE_MIN,
        io.newInputFile(objectUri).getLength());
  }

  @Test
  public void testManyParts_writeWithBytes() throws IOException {
    AwsProperties properties = new AwsProperties();
    properties.setS3FileIoMultiPartSize(AwsProperties.S3FILEIO_MULTIPART_SIZE_MIN);
    S3FileIO io = new S3FileIO(() -> s3, properties);
    PositionOutputStream outputStream = io.newOutputFile(objectUri).create();
    byte[] bytes = new byte[AwsProperties.S3FILEIO_MULTIPART_SIZE_MIN];
    for (int i = 0; i < 100; i++) {
      random.nextBytes(bytes);
      outputStream.write(bytes);
    }
    outputStream.close();
    Assert.assertEquals(100 * (long) AwsProperties.S3FILEIO_MULTIPART_SIZE_MIN,
        io.newInputFile(objectUri).getLength());
  }

  @Test
  public void testContents_writeWithInt() throws IOException {
    AwsProperties properties = new AwsProperties();
    properties.setS3FileIoMultiPartSize(AwsProperties.S3FILEIO_MULTIPART_SIZE_MIN);
    S3FileIO io = new S3FileIO(() -> s3, properties);
    PositionOutputStream outputStream = io.newOutputFile(objectUri).create();
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < AwsProperties.S3FILEIO_MULTIPART_SIZE_MIN; j++) {
        outputStream.write(6);
      }
    }
    outputStream.close();
    SeekableInputStream inputStream = io.newInputFile(objectUri).newStream();
    int cur;
    while ((cur = inputStream.read()) != -1) {
      Assert.assertEquals(6, cur);
    }
    inputStream.close();
  }

  @Test
  public void testContents_writeWithBytes() throws IOException {
    AwsProperties properties = new AwsProperties();
    properties.setS3FileIoMultiPartSize(AwsProperties.S3FILEIO_MULTIPART_SIZE_MIN);
    S3FileIO io = new S3FileIO(() -> s3, properties);
    PositionOutputStream outputStream = io.newOutputFile(objectUri).create();
    byte[] bytes = new byte[AwsProperties.S3FILEIO_MULTIPART_SIZE_MIN];
    for (int i = 0; i < AwsProperties.S3FILEIO_MULTIPART_SIZE_MIN; i++) {
      bytes[i] = 6;
    }
    for (int i = 0; i < 10; i++) {
      outputStream.write(bytes);
    }
    outputStream.close();
    SeekableInputStream inputStream = io.newInputFile(objectUri).newStream();
    int cur;
    while ((cur = inputStream.read()) != -1) {
      Assert.assertEquals(6, cur);
    }
    inputStream.close();
  }

  @Test
  public void testUploadRemainder() throws IOException {
    AwsProperties properties = new AwsProperties();
    properties.setS3FileIoMultiPartSize(AwsProperties.S3FILEIO_MULTIPART_SIZE_MIN);
    properties.setS3FileIoMultipartThresholdFactor(1);
    S3FileIO io = new S3FileIO(() -> s3, properties);
    PositionOutputStream outputStream = io.newOutputFile(objectUri).create();
    long length = 3 * AwsProperties.S3FILEIO_MULTIPART_SIZE_MIN + 2 * 1024 * 1024;
    for (int i = 0; i < length; i++) {
      outputStream.write(random.nextInt());
    }
    outputStream.close();
    Assert.assertEquals(length, io.newInputFile(objectUri).getLength());
  }

  @Test
  public void testParallelUpload() throws IOException {
    AwsProperties properties = new AwsProperties();
    properties.setS3FileIoMultiPartSize(AwsProperties.S3FILEIO_MULTIPART_SIZE_MIN);
    properties.setS3FileIoMultipartUploadThreads(16);
    S3FileIO io = new S3FileIO(() -> s3, properties);
    IntStream.range(0, 16).parallel().forEach(d -> {
      byte[] bytes = new byte[AwsProperties.S3FILEIO_MULTIPART_SIZE_MIN];
      for (int i = 0; i < AwsProperties.S3FILEIO_MULTIPART_SIZE_MIN; i++) {
        bytes[i] = (byte) d;
      }
      PositionOutputStream outputStream = io.newOutputFile(objectUri + "_" + d).create();
      try {
        for (int i = 0; i < 3; i++) {
          outputStream.write(bytes);
        }
        outputStream.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    for (int i = 0; i < 16; i++) {
      String fileUri = objectUri + "_" + i;
      InputFile inputFile = io.newInputFile(fileUri);
      Assert.assertEquals(3 * (long) AwsProperties.S3FILEIO_MULTIPART_SIZE_MIN, inputFile.getLength());
      int cur;
      InputStream stream = inputFile.newStream();
      while ((cur = stream.read()) != -1) {
        Assert.assertEquals(i, cur);
      }
      stream.close();
    }
  }
}
