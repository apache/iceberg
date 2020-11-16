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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.UUID;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import org.apache.iceberg.aws.AwsClientUtil;
import org.apache.iceberg.aws.AwsIntegTestUtil;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.ListAliasesRequest;
import software.amazon.awssdk.services.kms.model.ListAliasesResponse;
import software.amazon.awssdk.services.kms.model.ScheduleKeyDeletionRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.utils.IoUtils;

public class S3FileIOTest {

  private static S3Client s3;
  private static KmsClient kms;
  private static String bucketName;
  private static String prefix;
  private static byte[] contentBytes;
  private static String content;
  private static String kmsKeyArn;
  private String objectKey;
  private String objectUri;

  @BeforeClass
  public static void beforeClass() {
    s3 = AwsClientUtil.defaultS3Client();
    kms = AwsClientUtil.defaultKmsClient();
    bucketName = AwsIntegTestUtil.testBucketName();
    prefix = UUID.randomUUID().toString();
    contentBytes = new byte[1024 * 1024 * 10];
    content = new String(contentBytes, StandardCharsets.UTF_8);
    kmsKeyArn = kms.createKey().keyMetadata().arn();
  }

  @AfterClass
  public static void afterClass() {
    AwsIntegTestUtil.cleanS3Bucket(s3, bucketName, prefix);
    kms.scheduleKeyDeletion(ScheduleKeyDeletionRequest.builder().keyId(kmsKeyArn).pendingWindowInDays(7).build());
  }

  @Before
  public void before() {
    objectKey = String.format("%s/%s", prefix, UUID.randomUUID().toString());
    objectUri = String.format("s3://%s/%s", bucketName, objectKey);
  }

  @Test
  public void testNewInputStream() throws Exception {
    s3.putObject(PutObjectRequest.builder().bucket(bucketName).key(objectKey).build(),
        RequestBody.fromBytes(contentBytes));
    S3FileIO s3FileIO = new S3FileIO();
    validateRead(s3FileIO);
  }

  @Test
  public void testNewOutputStream() throws Exception {
    S3FileIO s3FileIO = new S3FileIO();
    write(s3FileIO);
    InputStream stream = s3.getObject(GetObjectRequest.builder().bucket(bucketName).key(objectKey).build());
    String result = IoUtils.toUtf8String(stream);
    stream.close();
    Assert.assertEquals(content, result);
  }

  @Test
  public void testSSE_S3() throws Exception {
    AwsProperties properties = new AwsProperties();
    properties.setS3FileIoSseType(AwsProperties.S3FILEIO_SSE_TYPE_S3);
    S3FileIO s3FileIO = new S3FileIO(AwsClientUtil::defaultS3Client, properties);
    write(s3FileIO);
    validateRead(s3FileIO);
    GetObjectResponse response = s3.getObject(
        GetObjectRequest.builder().bucket(bucketName).key(objectKey).build()).response();
    Assert.assertEquals(ServerSideEncryption.AES256, response.serverSideEncryption());
  }

  @Test
  public void testSSE_KMS() throws Exception {
    AwsProperties properties = new AwsProperties();
    properties.setS3FileIoSseType(AwsProperties.S3FILEIO_SSE_TYPE_KMS);
    properties.setS3FileIoSseKey(kmsKeyArn);
    S3FileIO s3FileIO = new S3FileIO(AwsClientUtil::defaultS3Client, properties);
    write(s3FileIO);
    validateRead(s3FileIO);
    GetObjectResponse response = s3.getObject(
        GetObjectRequest.builder().bucket(bucketName).key(objectKey).build()).response();
    Assert.assertEquals(ServerSideEncryption.AWS_KMS, response.serverSideEncryption());
    Assert.assertEquals(response.ssekmsKeyId(), kmsKeyArn);
  }

  @Test
  public void testSSE_KMS_default() throws Exception {
    AwsProperties properties = new AwsProperties();
    properties.setS3FileIoSseType(AwsProperties.S3FILEIO_SSE_TYPE_KMS);
    S3FileIO s3FileIO = new S3FileIO(AwsClientUtil::defaultS3Client, properties);
    write(s3FileIO);
    validateRead(s3FileIO);
    GetObjectResponse response = s3.getObject(
        GetObjectRequest.builder().bucket(bucketName).key(objectKey).build()).response();
    Assert.assertEquals(ServerSideEncryption.AWS_KMS, response.serverSideEncryption());
    ListAliasesResponse listAliasesResponse = kms.listAliases(
        ListAliasesRequest.builder().keyId(response.ssekmsKeyId()).build());
    Assert.assertTrue(listAliasesResponse.hasAliases());
    Assert.assertEquals(1, listAliasesResponse.aliases().size());
    Assert.assertEquals("alias/aws/s3", listAliasesResponse.aliases().get(0).aliasName());
  }

  @Test
  public void testSSE_Custom() throws Exception {
    // generate key
    KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
    keyGenerator.init(256, new SecureRandom());
    SecretKey secretKey = keyGenerator.generateKey();
    Base64.Encoder encoder = Base64.getEncoder();
    String encodedKey = new String(encoder.encode(secretKey.getEncoded()), StandardCharsets.UTF_8);
    // generate md5
    MessageDigest digest = MessageDigest.getInstance("MD5");
    String md5 = new String(encoder.encode(digest.digest(secretKey.getEncoded())), StandardCharsets.UTF_8);

    AwsProperties properties = new AwsProperties();
    properties.setS3FileIoSseType(AwsProperties.S3FILEIO_SSE_TYPE_CUSTOM);
    properties.setS3FileIoSseKey(encodedKey);
    properties.setS3FileIoSseMd5(md5);
    S3FileIO s3FileIO = new S3FileIO(AwsClientUtil::defaultS3Client, properties);
    write(s3FileIO);
    validateRead(s3FileIO);
    GetObjectResponse response = s3.getObject(
        GetObjectRequest.builder().bucket(bucketName).key(objectKey)
            .sseCustomerAlgorithm(ServerSideEncryption.AES256.name())
            .sseCustomerKey(encodedKey)
            .sseCustomerKeyMD5(md5)
            .build()).response();
    Assert.assertNull(response.serverSideEncryption());
    Assert.assertEquals(ServerSideEncryption.AES256.name(), response.sseCustomerAlgorithm());
    Assert.assertEquals(md5, response.sseCustomerKeyMD5());
  }

  private void write(S3FileIO s3FileIO) throws Exception {
    OutputFile outputFile = s3FileIO.newOutputFile(objectUri);
    OutputStream outputStream = outputFile.create();
    IoUtils.copy(new ByteArrayInputStream(contentBytes), outputStream);
    outputStream.close();
  }

  private void validateRead(S3FileIO s3FileIO) throws Exception {
    InputFile file = s3FileIO.newInputFile(objectUri);
    Assert.assertEquals(contentBytes.length, file.getLength());
    InputStream stream = file.newStream();
    String result = IoUtils.toUtf8String(stream);
    stream.close();
    Assert.assertEquals(content, result);
  }
}
