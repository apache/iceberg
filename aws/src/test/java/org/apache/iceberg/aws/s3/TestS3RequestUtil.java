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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
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
    Assertions.assertThat(serverSideEncryption).isNull();
    Assertions.assertThat(kmsKeyId).isNull();
    Assertions.assertThat(customAlgorithm).isEqualTo(ServerSideEncryption.AES256.name());
    Assertions.assertThat(customKey).isEqualTo("key");
    Assertions.assertThat(customMd5).isEqualTo("md5");
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
    Assertions.assertThat(serverSideEncryption).isEqualTo(ServerSideEncryption.AES256);
    Assertions.assertThat(kmsKeyId).isNull();
    Assertions.assertThat(customAlgorithm).isNull();
    Assertions.assertThat(customKey).isNull();
    Assertions.assertThat(customMd5).isNull();
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
    Assertions.assertThat(serverSideEncryption).isEqualTo(ServerSideEncryption.AWS_KMS);
    Assertions.assertThat(kmsKeyId).isEqualTo("key");
    Assertions.assertThat(customAlgorithm).isNull();
    Assertions.assertThat(customKey).isNull();
    Assertions.assertThat(customMd5).isNull();
  }

  @Test
  public void testConfigureDualLayerServerSideKmsEncryption() {
    S3FileIOProperties s3FileIOProperties = new S3FileIOProperties();
    s3FileIOProperties.setSseType(S3FileIOProperties.DSSE_TYPE_KMS);
    s3FileIOProperties.setSseKey("key");
    S3RequestUtil.configureEncryption(
        s3FileIOProperties,
        this::setServerSideEncryption,
        this::setKmsKeyId,
        this::setCustomAlgorithm,
        this::setCustomKey,
        this::setCustomMd5);
    Assertions.assertThat(serverSideEncryption).isEqualTo(ServerSideEncryption.AWS_KMS_DSSE);
    Assertions.assertThat(kmsKeyId).isEqualTo("key");
    Assertions.assertThat(customAlgorithm).isNull();
    Assertions.assertThat(customKey).isNull();
    Assertions.assertThat(customMd5).isNull();
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
    Assertions.assertThat(serverSideEncryption).isNull();
    Assertions.assertThat(kmsKeyId).isNull();
    Assertions.assertThat(customAlgorithm).isNull();
    Assertions.assertThat(customKey).isNull();
    Assertions.assertThat(customMd5).isNull();
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
