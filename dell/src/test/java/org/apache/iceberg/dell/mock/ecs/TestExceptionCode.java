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
package org.apache.iceberg.dell.mock.ecs;

import com.emc.object.Range;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.request.PutObjectRequest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

/** Verify the error codes between real client and mock client. */
public class TestExceptionCode {

  @Rule public EcsS3MockRule rule = EcsS3MockRule.create();

  @Test
  public void testExceptionCode() {
    String object = "test";
    assertS3Exception(
        "Append absent object",
        404,
        "NoSuchKey",
        () -> rule.client().appendObject(rule.bucket(), object, "abc".getBytes()));
    assertS3Exception(
        "Get object",
        404,
        "NoSuchKey",
        () -> rule.client().readObjectStream(rule.bucket(), object, Range.fromOffset(0)));

    rule.client().putObject(new PutObjectRequest(rule.bucket(), object, "abc".getBytes()));
    assertS3Exception(
        "Put object with unexpect E-Tag",
        412,
        "PreconditionFailed",
        () -> {
          PutObjectRequest request = new PutObjectRequest(rule.bucket(), object, "def".getBytes());
          request.setIfMatch("abc");
          rule.client().putObject(request);
        });
    assertS3Exception(
        "Put object if absent",
        412,
        "PreconditionFailed",
        () -> {
          PutObjectRequest request = new PutObjectRequest(rule.bucket(), object, "def".getBytes());
          request.setIfNoneMatch("*");
          rule.client().putObject(request);
        });
  }

  public void assertS3Exception(String message, int httpCode, String errorCode, Runnable task) {
    try {
      task.run();
      Assert.fail("Expect s3 exception for " + message);
    } catch (S3Exception e) {
      Assert.assertEquals(message + ", http code", httpCode, e.getHttpCode());
      Assert.assertEquals(message + ", error code", errorCode, e.getErrorCode());
    }
  }
}
