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

import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.s3.model.S3Request;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

public class TestS3RequestUtil {

  private ServerSideEncryption serverSideEncryption = null;
  private String kmsKeyId = null;
  private String customAlgorithm = null;
  private String customKey = null;
  private String customMd5 = null;

  @Test
  public void testConfigureServerSideCustomEncryption() {
    S3FileIOProperties s3FileIOProperties = new S3FileIOProperties();
    s3FileIOProperties.setSseType(S3FileIOProperties.SSE_TYPE_CUSTOM);
    s3FileIOProperties.setSseKey("key");
    s3FileIOProperties.setSseMd5("md5");
    S3RequestUtil.configureEncryption(
        s3FileIOProperties,
        this::setServerSideEncryption,
        this::setKmsKeyId,
        this::setCustomAlgorithm,
        this::setCustomKey,
        this::setCustomMd5);
    Assert.assertNull(serverSideEncryption);
    Assert.assertNull(kmsKeyId);
    Assert.assertEquals(ServerSideEncryption.AES256.name(), customAlgorithm);
    Assert.assertEquals("key", customKey);
    Assert.assertEquals("md5", customMd5);
  }

  @Test
  public void testConfigureServerSideS3Encryption() {
    S3FileIOProperties s3FileIOProperties = new S3FileIOProperties();
    s3FileIOProperties.setSseType(S3FileIOProperties.SSE_TYPE_S3);
    S3RequestUtil.configureEncryption(
        s3FileIOProperties,
        this::setServerSideEncryption,
        this::setKmsKeyId,
        this::setCustomAlgorithm,
        this::setCustomKey,
        this::setCustomMd5);
    Assert.assertEquals(ServerSideEncryption.AES256, serverSideEncryption);
    Assert.assertNull(kmsKeyId);
    Assert.assertNull(customAlgorithm);
    Assert.assertNull(customKey);
    Assert.assertNull(customMd5);
  }

  @Test
  public void testConfigureServerSideKmsEncryption() {
    S3FileIOProperties s3FileIOProperties = new S3FileIOProperties();
    s3FileIOProperties.setSseType(S3FileIOProperties.SSE_TYPE_KMS);
    s3FileIOProperties.setSseKey("key");
    S3RequestUtil.configureEncryption(
        s3FileIOProperties,
        this::setServerSideEncryption,
        this::setKmsKeyId,
        this::setCustomAlgorithm,
        this::setCustomKey,
        this::setCustomMd5);
    Assert.assertEquals(ServerSideEncryption.AWS_KMS, serverSideEncryption);
    Assert.assertEquals("key", kmsKeyId);
    Assert.assertNull(customAlgorithm);
    Assert.assertNull(customKey);
    Assert.assertNull(customMd5);
  }

  @Test
  public void testConfigureEncryptionSkipNullSetters() {
    S3FileIOProperties s3FileIOProperties = new S3FileIOProperties();
    s3FileIOProperties.setSseType(S3FileIOProperties.SSE_TYPE_KMS);
    s3FileIOProperties.setSseKey("key");
    S3RequestUtil.configureEncryption(
        s3FileIOProperties,
        v -> null,
        v -> null,
        this::setCustomAlgorithm,
        this::setCustomKey,
        this::setCustomMd5);
    Assert.assertNull(serverSideEncryption);
    Assert.assertNull(kmsKeyId);
    Assert.assertNull(customAlgorithm);
    Assert.assertNull(customKey);
    Assert.assertNull(customMd5);
  }

  public S3Request.Builder setCustomAlgorithm(String algorithm) {
    this.customAlgorithm = algorithm;
    return null;
  }

  public S3Request.Builder setCustomKey(String key) {
    this.customKey = key;
    return null;
  }

  public S3Request.Builder setCustomMd5(String md5) {
    this.customMd5 = md5;
    return null;
  }

  public S3Request.Builder setKmsKeyId(String keyId) {
    this.kmsKeyId = keyId;
    return null;
  }

  public S3Request.Builder setServerSideEncryption(ServerSideEncryption sse) {
    this.serverSideEncryption = sse;
    return null;
  }
}
