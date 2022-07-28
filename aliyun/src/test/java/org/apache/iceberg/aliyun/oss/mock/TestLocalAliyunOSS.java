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
package org.apache.iceberg.aliyun.oss.mock;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSErrorCode;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.PutObjectResult;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import org.apache.iceberg.aliyun.TestUtility;
import org.apache.iceberg.aliyun.oss.AliyunOSSTestRule;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class TestLocalAliyunOSS {

  @ClassRule public static final AliyunOSSTestRule OSS_TEST_RULE = TestUtility.initialize();

  private final OSS oss = OSS_TEST_RULE.createOSSClient();
  private final String bucketName = OSS_TEST_RULE.testBucketName();
  private final Random random = new Random(1);

  private static void assertThrows(Runnable runnable, String expectedErrorCode) {
    try {
      runnable.run();
      Assert.fail("No exception was thrown, expected errorCode: " + expectedErrorCode);
    } catch (OSSException e) {
      Assert.assertEquals(expectedErrorCode, e.getErrorCode());
    }
  }

  @Before
  public void before() {
    OSS_TEST_RULE.setUpBucket(bucketName);
  }

  @After
  public void after() {
    OSS_TEST_RULE.tearDownBucket(bucketName);
  }

  @Test
  public void testBuckets() {
    Assume.assumeTrue(
        "Aliyun integration test cannot delete existing bucket from test environment.",
        OSS_TEST_RULE.getClass() == AliyunOSSMockRule.class);

    Assert.assertTrue(doesBucketExist(bucketName));
    assertThrows(() -> oss.createBucket(bucketName), OSSErrorCode.BUCKET_ALREADY_EXISTS);

    oss.deleteBucket(bucketName);
    Assert.assertFalse(doesBucketExist(bucketName));

    oss.createBucket(bucketName);
    Assert.assertTrue(doesBucketExist(bucketName));
  }

  @Test
  public void testDeleteBucket() {
    Assume.assumeTrue(
        "Aliyun integration test cannot delete existing bucket from test environment.",
        OSS_TEST_RULE.getClass() == AliyunOSSMockRule.class);

    String bucketNotExist = String.format("bucket-not-existing-%s", UUID.randomUUID());
    assertThrows(() -> oss.deleteBucket(bucketNotExist), OSSErrorCode.NO_SUCH_BUCKET);

    byte[] bytes = new byte[2000];
    random.nextBytes(bytes);

    oss.putObject(bucketName, "object1", wrap(bytes));

    oss.putObject(bucketName, "object2", wrap(bytes));

    assertThrows(() -> oss.deleteBucket(bucketName), OSSErrorCode.BUCKET_NOT_EMPTY);

    oss.deleteObject(bucketName, "object1");
    assertThrows(() -> oss.deleteBucket(bucketName), OSSErrorCode.BUCKET_NOT_EMPTY);

    oss.deleteObject(bucketName, "object2");
    oss.deleteBucket(bucketName);
    Assert.assertFalse(doesBucketExist(bucketName));

    oss.createBucket(bucketName);
  }

  @Test
  public void testPutObject() throws IOException {
    byte[] bytes = new byte[4 * 1024];
    random.nextBytes(bytes);

    String bucketNotExist = String.format("bucket-not-existing-%s", UUID.randomUUID());
    assertThrows(
        () -> oss.putObject(bucketNotExist, "object", wrap(bytes)), OSSErrorCode.NO_SUCH_BUCKET);

    PutObjectResult result = oss.putObject(bucketName, "object", wrap(bytes));
    Assert.assertEquals(AliyunOSSMockLocalStore.md5sum(wrap(bytes)), result.getETag());
  }

  @Test
  public void testDoesObjectExist() {
    Assert.assertFalse(oss.doesObjectExist(bucketName, "key"));

    byte[] bytes = new byte[4 * 1024];
    random.nextBytes(bytes);
    oss.putObject(bucketName, "key", wrap(bytes));

    Assert.assertTrue(oss.doesObjectExist(bucketName, "key"));
    oss.deleteObject(bucketName, "key");
  }

  @Test
  public void testGetObject() throws IOException {
    String bucketNotExist = String.format("bucket-not-existing-%s", UUID.randomUUID());
    assertThrows(() -> oss.getObject(bucketNotExist, "key"), OSSErrorCode.NO_SUCH_BUCKET);

    assertThrows(() -> oss.getObject(bucketName, "key"), OSSErrorCode.NO_SUCH_KEY);

    byte[] bytes = new byte[2000];
    random.nextBytes(bytes);

    oss.putObject(bucketName, "key", new ByteArrayInputStream(bytes));

    byte[] actual = new byte[2000];
    try (InputStream is = oss.getObject(bucketName, "key").getObjectContent()) {
      ByteStreams.readFully(is, actual);
    }
    Assert.assertArrayEquals(bytes, actual);
    oss.deleteObject(bucketName, "key");
  }

  @Test
  public void testGetObjectWithRange() throws IOException {

    byte[] bytes = new byte[100];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) i;
    }
    oss.putObject(bucketName, "key", new ByteArrayInputStream(bytes));

    int start = 0;
    int end = 0;
    testRange(bytes, start, end);

    start = 0;
    end = 1;
    testRange(bytes, start, end);

    start = 1;
    end = 9;
    testRange(bytes, start, end);

    start = 0;
    end = 99;
    testRange(bytes, start, end);

    start = -1;
    end = 2;
    testRange(bytes, start, end);

    start = 98;
    end = -1;
    testRange(bytes, start, end);

    start = -1;
    end = -1;
    testRange(bytes, start, end);

    oss.deleteObject(bucketName, "key");
  }

  private void testRange(byte[] bytes, int start, int end) throws IOException {
    byte[] testBytes;
    byte[] actual;
    int len;
    if (start == -1 && end == -1) {
      len = bytes.length;
      actual = new byte[len];
      testBytes = new byte[len];
      System.arraycopy(bytes, 0, testBytes, 0, len);
    } else if (start == -1) {
      len = end;
      actual = new byte[len];
      testBytes = new byte[len];
      System.arraycopy(bytes, bytes.length - end, testBytes, 0, len);
    } else if (end == -1) {
      len = bytes.length - start;
      actual = new byte[len];
      testBytes = new byte[len];
      System.arraycopy(bytes, start, testBytes, 0, len);
    } else {
      len = end - start + 1;
      actual = new byte[len];
      testBytes = new byte[len];
      System.arraycopy(bytes, start, testBytes, 0, len);
    }

    GetObjectRequest getObjectRequest;
    getObjectRequest = new GetObjectRequest(bucketName, "key");
    getObjectRequest.setRange(start, end);
    try (InputStream is = oss.getObject(getObjectRequest).getObjectContent()) {
      ByteStreams.readFully(is, actual);
    }
    Assert.assertArrayEquals(testBytes, actual);
  }

  private InputStream wrap(byte[] data) {
    return new ByteArrayInputStream(data);
  }

  private boolean doesBucketExist(String bucket) {
    try {
      oss.createBucket(bucket);
      oss.deleteBucket(bucket);
      return false;
    } catch (OSSException e) {
      if (Objects.equals(e.getErrorCode(), OSSErrorCode.BUCKET_ALREADY_EXISTS)) {
        return true;
      }
      throw e;
    }
  }
}
