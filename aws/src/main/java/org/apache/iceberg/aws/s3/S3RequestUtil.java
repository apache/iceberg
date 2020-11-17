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

import java.util.Locale;
import org.apache.iceberg.aws.AwsProperties;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

public class S3RequestUtil {

  private S3RequestUtil() {
  }

  static void configureEncryption(AwsProperties awsProperties, PutObjectRequest.Builder requestBuilder) {
    switch (awsProperties.s3FileIoSseType().toLowerCase(Locale.ENGLISH)) {
      case AwsProperties.S3FILEIO_SSE_TYPE_NONE:
        break;

      case AwsProperties.S3FILEIO_SSE_TYPE_KMS:
        requestBuilder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
        requestBuilder.ssekmsKeyId(awsProperties.s3FileIoSseKey());
        break;

      case AwsProperties.S3FILEIO_SSE_TYPE_S3:
        requestBuilder.serverSideEncryption(ServerSideEncryption.AES256);
        break;

      case AwsProperties.S3FILEIO_SSE_TYPE_CUSTOM:
        requestBuilder.sseCustomerAlgorithm(ServerSideEncryption.AES256.name());
        requestBuilder.sseCustomerKey(awsProperties.s3FileIoSseKey());
        requestBuilder.sseCustomerKeyMD5(awsProperties.s3FileIoSseMd5());
        break;

      default:
        throw new IllegalArgumentException(
            "Cannot support given S3 encryption type: " + awsProperties.s3FileIoSseType());
    }
  }

  static void configureEncryption(AwsProperties awsProperties, CreateMultipartUploadRequest.Builder requestBuilder) {
    switch (awsProperties.s3FileIoSseType().toLowerCase(Locale.ENGLISH)) {
      case AwsProperties.S3FILEIO_SSE_TYPE_NONE:
        break;

      case AwsProperties.S3FILEIO_SSE_TYPE_KMS:
        requestBuilder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
        requestBuilder.ssekmsKeyId(awsProperties.s3FileIoSseKey());
        break;

      case AwsProperties.S3FILEIO_SSE_TYPE_S3:
        requestBuilder.serverSideEncryption(ServerSideEncryption.AES256);
        break;

      case AwsProperties.S3FILEIO_SSE_TYPE_CUSTOM:
        requestBuilder.sseCustomerAlgorithm(ServerSideEncryption.AES256.name());
        requestBuilder.sseCustomerKey(awsProperties.s3FileIoSseKey());
        requestBuilder.sseCustomerKeyMD5(awsProperties.s3FileIoSseMd5());
        break;

      default:
        throw new IllegalArgumentException(
            "Cannot support given S3 encryption type: " + awsProperties.s3FileIoSseType());
    }
  }

  static void configureEncryption(AwsProperties awsProperties, UploadPartRequest.Builder requestBuilder) {
    switch (awsProperties.s3FileIoSseType().toLowerCase(Locale.ENGLISH)) {
      case AwsProperties.S3FILEIO_SSE_TYPE_NONE:
      case AwsProperties.S3FILEIO_SSE_TYPE_KMS:
      case AwsProperties.S3FILEIO_SSE_TYPE_S3:
        break;

      case AwsProperties.S3FILEIO_SSE_TYPE_CUSTOM:
        requestBuilder.sseCustomerAlgorithm(ServerSideEncryption.AES256.name());
        requestBuilder.sseCustomerKey(awsProperties.s3FileIoSseKey());
        requestBuilder.sseCustomerKeyMD5(awsProperties.s3FileIoSseMd5());
        break;

      default:
        throw new IllegalArgumentException(
            "Cannot support given S3 encryption type: " + awsProperties.s3FileIoSseType());
    }
  }

  static void configureEncryption(AwsProperties awsProperties, GetObjectRequest.Builder requestBuilder) {
    switch (awsProperties.s3FileIoSseType().toLowerCase(Locale.ENGLISH)) {
      case AwsProperties.S3FILEIO_SSE_TYPE_NONE:
      case AwsProperties.S3FILEIO_SSE_TYPE_KMS:
      case AwsProperties.S3FILEIO_SSE_TYPE_S3:
        break;

      case AwsProperties.S3FILEIO_SSE_TYPE_CUSTOM:
        requestBuilder.sseCustomerAlgorithm(ServerSideEncryption.AES256.name());
        requestBuilder.sseCustomerKey(awsProperties.s3FileIoSseKey());
        requestBuilder.sseCustomerKeyMD5(awsProperties.s3FileIoSseMd5());
        break;

      default:
        throw new IllegalArgumentException(
            "Cannot support given S3 encryption type: " + awsProperties.s3FileIoSseType());
    }
  }
}
