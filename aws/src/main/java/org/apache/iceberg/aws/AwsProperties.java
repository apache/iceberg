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

package org.apache.iceberg.aws;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class AwsProperties {

  /**
   * Type of S3 Server side encryption used, default to {@link AwsProperties#S3FILEIO_SSE_TYPE_NONE}.
   */
  public static final String S3FILEIO_SSE_TYPE = "s3fileio.sse.type";

  /**
   * No server side encryption.
   */
  public static final String S3FILEIO_SSE_TYPE_NONE = "none";

  /**
   * S3 SSE-KMS encryption.
   * For more details: https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingKMSEncryption.html
   */
  public static final String S3FILEIO_SSE_TYPE_KMS = "kms";

  /**
   * S3 SSE-S3 encryption.
   * For more details: https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html
   */
  public static final String S3FILEIO_SSE_TYPE_S3 = "s3";

  /**
   * S3 SSE-C encryption.
   * For more details: https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html
   */
  public static final String S3FILEIO_SSE_TYPE_CUSTOM = "custom";

  /**
   * If S3 encryption type is SSE-KMS, input is a KMS Key ID or ARN.
   *   In case this property is not set, default key "aws/s3" is used.
   * If encryption type is SSE-C, input is a custom base-64 AES256 symmetric key.
   */
  public static final String S3FILEIO_SSE_KEY = "s3fileio.sse.key";

  /**
   * If S3 encryption type is SSE-C, input is the base-64 MD5 digest of the secret key.
   * This MD5 must be explicitly passed in by the caller to ensure key integrity.
   */
  public static final String S3FILEIO_SSE_MD5 = "s3fileio.sse.md5";

  private String s3FileIoSseType;
  private String s3FileIoSseKey;
  private String s3FileIoSseMd5;

  public AwsProperties() {
    this.s3FileIoSseType = S3FILEIO_SSE_TYPE_NONE;
    this.s3FileIoSseKey = null;
    this.s3FileIoSseMd5 = null;
  }

  public AwsProperties(Map<String, String> properties) {
    this.s3FileIoSseType = properties.getOrDefault(
        AwsProperties.S3FILEIO_SSE_TYPE, AwsProperties.S3FILEIO_SSE_TYPE_NONE);
    this.s3FileIoSseKey = properties.get(AwsProperties.S3FILEIO_SSE_KEY);
    this.s3FileIoSseMd5 = properties.get(AwsProperties.S3FILEIO_SSE_MD5);
    if (AwsProperties.S3FILEIO_SSE_TYPE_CUSTOM.equals(s3FileIoSseType)) {
      Preconditions.checkNotNull(s3FileIoSseKey, "Cannot initialize SSE-C S3FileIO with null encryption key");
      Preconditions.checkNotNull(s3FileIoSseMd5, "Cannot initialize SSE-C S3FileIO with null encryption key MD5");
    }
  }

  public String s3FileIoSseType() {
    return s3FileIoSseType;
  }

  public void setS3FileIoSseType(String sseType) {
    this.s3FileIoSseType = sseType;
  }

  public String s3FileIoSseKey() {
    return s3FileIoSseKey;
  }

  public void setS3FileIoSseKey(String sseKey) {
    this.s3FileIoSseKey = sseKey;
  }

  public String s3FileIoSseMd5() {
    return s3FileIoSseMd5;
  }

  public void setS3FileIoSseMd5(String sseMd5) {
    this.s3FileIoSseMd5 = sseMd5;
  }
}
