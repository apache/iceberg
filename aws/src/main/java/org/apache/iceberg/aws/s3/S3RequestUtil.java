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
import java.util.function.Function;
import org.apache.iceberg.aws.AwsProperties;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Request;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

@SuppressWarnings("UnnecessaryLambda")
public class S3RequestUtil {

  private static final Function<ServerSideEncryption, S3Request.Builder> NULL_SSE_SETTER =
      sse -> null;
  private static final Function<String, S3Request.Builder> NULL_STRING_SETTER = s -> null;

  private S3RequestUtil() {}

  static void configureEncryption(
      AwsProperties awsProperties, PutObjectRequest.Builder requestBuilder) {
    configureEncryption(
        awsProperties,
        requestBuilder::serverSideEncryption,
        requestBuilder::ssekmsKeyId,
        requestBuilder::sseCustomerAlgorithm,
        requestBuilder::sseCustomerKey,
        requestBuilder::sseCustomerKeyMD5);
  }

  static void configureEncryption(
      AwsProperties awsProperties, CreateMultipartUploadRequest.Builder requestBuilder) {
    configureEncryption(
        awsProperties,
        requestBuilder::serverSideEncryption,
        requestBuilder::ssekmsKeyId,
        requestBuilder::sseCustomerAlgorithm,
        requestBuilder::sseCustomerKey,
        requestBuilder::sseCustomerKeyMD5);
  }

  static void configureEncryption(
      AwsProperties awsProperties, UploadPartRequest.Builder requestBuilder) {
    configureEncryption(
        awsProperties,
        NULL_SSE_SETTER,
        NULL_STRING_SETTER,
        requestBuilder::sseCustomerAlgorithm,
        requestBuilder::sseCustomerKey,
        requestBuilder::sseCustomerKeyMD5);
  }

  static void configureEncryption(
      AwsProperties awsProperties, GetObjectRequest.Builder requestBuilder) {
    configureEncryption(
        awsProperties,
        NULL_SSE_SETTER,
        NULL_STRING_SETTER,
        requestBuilder::sseCustomerAlgorithm,
        requestBuilder::sseCustomerKey,
        requestBuilder::sseCustomerKeyMD5);
  }

  static void configureEncryption(
      AwsProperties awsProperties, HeadObjectRequest.Builder requestBuilder) {
    configureEncryption(
        awsProperties,
        NULL_SSE_SETTER,
        NULL_STRING_SETTER,
        requestBuilder::sseCustomerAlgorithm,
        requestBuilder::sseCustomerKey,
        requestBuilder::sseCustomerKeyMD5);
  }

  @SuppressWarnings("ReturnValueIgnored")
  static void configureEncryption(
      AwsProperties awsProperties,
      Function<ServerSideEncryption, S3Request.Builder> encryptionSetter,
      Function<String, S3Request.Builder> kmsKeySetter,
      Function<String, S3Request.Builder> customAlgorithmSetter,
      Function<String, S3Request.Builder> customKeySetter,
      Function<String, S3Request.Builder> customMd5Setter) {

    switch (awsProperties.s3FileIoSseType().toLowerCase(Locale.ENGLISH)) {
      case AwsProperties.S3FILEIO_SSE_TYPE_NONE:
        break;

      case AwsProperties.S3FILEIO_SSE_TYPE_KMS:
        encryptionSetter.apply(ServerSideEncryption.AWS_KMS);
        kmsKeySetter.apply(awsProperties.s3FileIoSseKey());
        break;

      case AwsProperties.S3FILEIO_SSE_TYPE_S3:
        encryptionSetter.apply(ServerSideEncryption.AES256);
        break;

      case AwsProperties.S3FILEIO_SSE_TYPE_CUSTOM:
        // setters for SSE-C exist for all request builders, no need to check null
        customAlgorithmSetter.apply(ServerSideEncryption.AES256.name());
        customKeySetter.apply(awsProperties.s3FileIoSseKey());
        customMd5Setter.apply(awsProperties.s3FileIoSseMd5());
        break;

      default:
        throw new IllegalArgumentException(
            "Cannot support given S3 encryption type: " + awsProperties.s3FileIoSseType());
    }
  }

  static void configurePermission(
      AwsProperties awsProperties, PutObjectRequest.Builder requestBuilder) {
    configurePermission(awsProperties, requestBuilder::acl);
  }

  static void configurePermission(
      AwsProperties awsProperties, CreateMultipartUploadRequest.Builder requestBuilder) {
    configurePermission(awsProperties, requestBuilder::acl);
  }

  @SuppressWarnings("ReturnValueIgnored")
  static void configurePermission(
      AwsProperties awsProperties, Function<ObjectCannedACL, S3Request.Builder> aclSetter) {
    aclSetter.apply(awsProperties.s3FileIoAcl());
  }
}
