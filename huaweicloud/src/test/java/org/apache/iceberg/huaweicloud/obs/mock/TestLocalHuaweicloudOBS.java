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
package org.apache.iceberg.huaweicloud.obs.mock;

import com.obs.services.IObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.GetObjectRequest;
import com.obs.services.model.PutObjectResult;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.iceberg.huaweicloud.TestUtility;
import org.apache.iceberg.huaweicloud.obs.HuaweicloudOBSTestRule;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class TestLocalHuaweicloudOBS {

  @ClassRule public static final HuaweicloudOBSTestRule OBS_TEST_RULE = TestUtility.initialize();

  private final IObsClient obs = OBS_TEST_RULE.createOBSClient();
  private final String bucketName = OBS_TEST_RULE.testBucketName();
  private final Random random = new Random(1);

  private static void assertThrows(Runnable runnable, String expectedErrorCode) {
    try {
      runnable.run();
      Assert.fail("No exception was thrown, expected errorCode: " + expectedErrorCode);
    } catch (ObsException e) {
      Assert.assertEquals(expectedErrorCode, e.getErrorCode());
    }
  }

  @Before
  public void before() {
    OBS_TEST_RULE.setUpBucket(bucketName);
  }

  @After
  public void after() {
    OBS_TEST_RULE.tearDownBucket(bucketName);
  }

  @Test
  public void testBuckets() {
    Assume.assumeTrue(
        "Huaweicloud integration test cannot delete existing bucket from test environment.",
        OBS_TEST_RULE.getClass() == HuaweicloudOBSMockRule.class);

    Assert.assertTrue(doesBucketExist(bucketName));
    assertThrows(() -> obs.createBucket(bucketName), OBSErrorCode.BUCKET_ALREADY_EXISTS);

    obs.deleteBucket(bucketName);
    Assert.assertFalse(doesBucketExist(bucketName));

    obs.createBucket(bucketName);
    Assert.assertTrue(doesBucketExist(bucketName));
  }

  @Test
  public void testDeleteBucket() {
    Assume.assumeTrue(
        "Huaweicloud integration test cannot delete existing bucket from test environment.",
        OBS_TEST_RULE.getClass() == HuaweicloudOBSMockRule.class);

    String bucketNotExist = String.format("bucket-not-existing-%s", UUID.randomUUID());
    assertThrows(() -> obs.deleteBucket(bucketNotExist), OBSErrorCode.NO_SUCH_BUCKET);

    byte[] bytes = new byte[2000];
    random.nextBytes(bytes);

    obs.putObject(bucketName, "object1", wrap(bytes));

    obs.putObject(bucketName, "object2", wrap(bytes));

    assertThrows(() -> obs.deleteBucket(bucketName), OBSErrorCode.BUCKET_NOT_EMPTY);

    obs.deleteObject(bucketName, "object1");
    assertThrows(() -> obs.deleteBucket(bucketName), OBSErrorCode.BUCKET_NOT_EMPTY);

    obs.deleteObject(bucketName, "object2");
    obs.deleteBucket(bucketName);
    Assert.assertFalse(doesBucketExist(bucketName));

    obs.createBucket(bucketName);
  }

  @Test
  public void testPutObject() throws IOException {
    byte[] bytes = new byte[4 * 1024];
    random.nextBytes(bytes);

    String bucketNotExist = String.format("bucket-not-existing-%s", UUID.randomUUID());
    assertThrows(
        () -> obs.putObject(bucketNotExist, "object", wrap(bytes)), OBSErrorCode.NO_SUCH_BUCKET);

    PutObjectResult result = obs.putObject(bucketName, "object", wrap(bytes));
    Assert.assertEquals(
        HuaweicloudOBSMockLocalStore.md5sum(wrap(bytes)), result.getEtag().replace("\"", ""));
  }

  @Test
  public void testDoesObjectExist() {
    Assert.assertFalse(obs.doesObjectExist(bucketName, "key"));

    byte[] bytes = new byte[4 * 1024];
    random.nextBytes(bytes);
    obs.putObject(bucketName, "key", wrap(bytes));

    Assert.assertTrue(obs.doesObjectExist(bucketName, "key"));
    obs.deleteObject(bucketName, "key");
  }

  @Test
  public void testGetObject() throws IOException {
    String bucketNotExist = String.format("bucket-not-existing-%s", UUID.randomUUID());
    assertThrows(() -> obs.getObject(bucketNotExist, "key"), OBSErrorCode.NO_SUCH_BUCKET);

    assertThrows(() -> obs.getObject(bucketName, "key"), OBSErrorCode.NO_SUCH_KEY);

    byte[] bytes = new byte[2000];
    random.nextBytes(bytes);

    obs.putObject(bucketName, "key", new ByteArrayInputStream(bytes));

    byte[] actual = new byte[2000];
    try (InputStream is = obs.getObject(bucketName, "key").getObjectContent()) {
      ByteStreams.readFully(is, actual);
    }
    Assert.assertArrayEquals(bytes, actual);
    obs.deleteObject(bucketName, "key");
  }

  @Test
  public void testGetObjectWithRange() throws IOException {

    byte[] bytes = new byte[100];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) i;
    }
    obs.putObject(bucketName, "key", new ByteArrayInputStream(bytes));

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

    obs.deleteObject(bucketName, "key");
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
    getObjectRequest.setRangeStart((long) start);
    getObjectRequest.setRangeEnd((long) end);
    try (InputStream is = obs.getObject(getObjectRequest).getObjectContent()) {
      ByteStreams.readFully(is, actual);
    }
    Assert.assertArrayEquals(testBytes, actual);
  }

  private InputStream wrap(byte[] data) {
    return new ByteArrayInputStream(data);
  }

  private boolean doesBucketExist(String bucket) {
    try {
      obs.createBucket(bucket);
      obs.deleteBucket(bucket);
      return false;
    } catch (ObsException obsException) {
      if (Objects.equals(obsException.getResponseCode(), HttpStatus.SC_CONFLICT)) {
        return true;
      }
      throw obsException;
    }
  }
}
