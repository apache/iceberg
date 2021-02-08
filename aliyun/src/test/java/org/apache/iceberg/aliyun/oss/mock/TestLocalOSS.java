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
import com.aliyun.oss.model.PutObjectResult;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Random;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

public class TestLocalOSS {

  @ClassRule
  public static final OSSMockRule OSS_MOCK_RULE = OSSMockRule.builder().silent().build();

  private final OSS oss = OSS_MOCK_RULE.createOSSClient();
  private final Random random = new Random(1);

  @Test
  public void testBuckets() {
    oss.createBucket("bucket");
    Assert.assertTrue(doesBucketExist("bucket"));
    assertThrows(() -> oss.createBucket("bucket"), OSSErrorCode.BUCKET_ALREADY_EXISTS);

    oss.deleteBucket("bucket");
    Assert.assertFalse(doesBucketExist("bucket"));

    oss.createBucket("bucket");
    Assert.assertTrue(doesBucketExist("bucket"));

    oss.deleteBucket("bucket");
    Assert.assertFalse(doesBucketExist("bucket"));
  }

  @Test
  public void testDeleteBucket() {
    oss.createBucket("bucket");

    assertThrows(() -> oss.deleteBucket("non-existing"), OSSErrorCode.NO_SUCH_BUCKET);

    byte[] bytes = new byte[2000];
    random.nextBytes(bytes);
    ByteArrayInputStream input = new ByteArrayInputStream(bytes);

    oss.putObject("bucket", "object1", input);

    input.reset();
    oss.putObject("bucket", "object2", input);

    assertThrows(() -> oss.deleteBucket("bucket"), OSSErrorCode.BUCKET_NOT_EMPTY);

    oss.deleteObject("bucket", "object1");
    assertThrows(() -> oss.deleteBucket("bucket"), OSSErrorCode.BUCKET_NOT_EMPTY);

    oss.deleteObject("bucket", "object2");
    oss.deleteBucket("bucket");
    Assert.assertFalse(doesBucketExist("bucket"));
  }

  @Test
  public void testPutObject() throws IOException {
    byte[] bytes = new byte[4 * 1024];
    random.nextBytes(bytes);

    assertThrows(() -> oss.putObject("bucket", "object", new ByteArrayInputStream(bytes)), OSSErrorCode.NO_SUCH_BUCKET);

    oss.createBucket("bucket");
    try {
      PutObjectResult result = oss.putObject("bucket", "object", new ByteArrayInputStream(bytes));
      Assert.assertEquals(LocalStore.md5sum(new ByteArrayInputStream(bytes)), result.getETag());
    } finally {
      oss.deleteObject("bucket", "object");
      oss.deleteBucket("bucket");
    }
  }

  @Test
  public void testDoesObjectExist() {
    Assert.assertFalse(oss.doesObjectExist("bucket", "key"));

    oss.createBucket("bucket");
    try {
      Assert.assertFalse(oss.doesObjectExist("bucket", "key"));

      byte[] bytes = new byte[4 * 1024];
      random.nextBytes(bytes);
      ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
      oss.putObject("bucket", "key", inputStream);

      Assert.assertTrue(oss.doesObjectExist("bucket", "key"));
      oss.deleteObject("bucket", "key");
    } finally {
      oss.deleteBucket("bucket");
    }
  }

  @Test
  public void testGetObject() throws IOException {
    assertThrows(() -> oss.getObject("bucket", "key"), OSSErrorCode.NO_SUCH_BUCKET);

    oss.createBucket("bucket");
    try {
      assertThrows(() -> oss.getObject("bucket", "key"), OSSErrorCode.NO_SUCH_KEY);

      byte[] bytes = new byte[2000];
      random.nextBytes(bytes);

      oss.putObject("bucket", "key", new ByteArrayInputStream(bytes));

      byte[] actual = new byte[2000];
      IOUtils.readFully(oss.getObject("bucket", "key").getObjectContent(), actual);

      Assert.assertArrayEquals(bytes, actual);
      oss.deleteObject("bucket", "key");
    } finally {
      oss.deleteBucket("bucket");
    }
  }

  private boolean doesBucketExist(String bucketName) {
    try {
      oss.createBucket(bucketName);
      oss.deleteBucket(bucketName);
      return false;
    } catch (OSSException e) {
      if (Objects.equals(e.getErrorCode(), OSSErrorCode.BUCKET_ALREADY_EXISTS)) {
        return true;
      }
      throw e;
    }
  }

  private static void assertThrows(Runnable runnable, String errorCode) {
    try {
      runnable.run();
      Assert.fail("No exception was thrown, expected errorCode: " + errorCode);
    } catch (OSSException e) {
      Assert.assertEquals(e.getErrorCode(), errorCode);
    }
  }
}
