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
import java.util.UUID;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.model.CreateRoleRequest;
import software.amazon.awssdk.services.iam.model.CreateRoleResponse;
import software.amazon.awssdk.services.iam.model.DeleteRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.DeleteRoleRequest;
import software.amazon.awssdk.services.iam.model.PutRolePolicyRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class TestAssumeRoleAwsClientFactory {

  private static final Logger LOG = LoggerFactory.getLogger(TestAssumeRoleAwsClientFactory.class);

  private IamClient iam;
  private String roleName;
  private Map<String, String> assumeRoleProperties;
  private String policyName;

  @Before
  public void before() {
    roleName = UUID.randomUUID().toString();
    iam =
        IamClient.builder()
            .region(Region.AWS_GLOBAL)
            .httpClientBuilder(UrlConnectionHttpClient.builder())
            .build();
    CreateRoleResponse response =
        iam.createRole(
            CreateRoleRequest.builder()
                .roleName(roleName)
                .assumeRolePolicyDocument(
                    "{"
                        + "\"Version\":\"2012-10-17\","
                        + "\"Statement\":[{"
                        + "\"Effect\":\"Allow\","
                        + "\"Principal\":{"
                        + "\"AWS\":\"arn:aws:iam::"
                        + AwsIntegTestUtil.testAccountId()
                        + ":root\"},"
                        + "\"Action\": [\"sts:AssumeRole\","
                        + "\"sts:TagSession\"]}]}")
                .maxSessionDuration(3600)
                .build());
    assumeRoleProperties = Maps.newHashMap();
    assumeRoleProperties.put(
        AwsProperties.CLIENT_FACTORY, AssumeRoleAwsClientFactory.class.getName());
    assumeRoleProperties.put(AwsProperties.HTTP_CLIENT_TYPE, AwsProperties.HTTP_CLIENT_TYPE_APACHE);
    assumeRoleProperties.put(
        AwsProperties.CLIENT_ASSUME_ROLE_REGION, AwsIntegTestUtil.testRegion());
    assumeRoleProperties.put(AwsProperties.CLIENT_ASSUME_ROLE_ARN, response.role().arn());
    assumeRoleProperties.put(AwsProperties.CLIENT_ASSUME_ROLE_TAGS_PREFIX + "key1", "value1");
    assumeRoleProperties.put(AwsProperties.CLIENT_ASSUME_ROLE_TAGS_PREFIX + "key2", "value2");
    policyName = UUID.randomUUID().toString();
  }

  @After
  public void after() {
    iam.deleteRolePolicy(
        DeleteRolePolicyRequest.builder().roleName(roleName).policyName(policyName).build());
    iam.deleteRole(DeleteRoleRequest.builder().roleName(roleName).build());
  }

  @Test
  public void testAssumeRoleGlueCatalog() throws Exception {
    String glueArnPrefix = "arn:aws:glue:*:" + AwsIntegTestUtil.testAccountId();
    iam.putRolePolicy(
        PutRolePolicyRequest.builder()
            .roleName(roleName)
            .policyName(policyName)
            .policyDocument(
                "{"
                    + "\"Version\":\"2012-10-17\","
                    + "\"Statement\":[{"
                    + "\"Sid\":\"policy1\","
                    + "\"Effect\":\"Allow\","
                    + "\"Action\":[\"glue:CreateDatabase\",\"glue:DeleteDatabase\",\"glue:GetDatabase\",\"glue:GetTables\"],"
                    + "\"Resource\":[\""
                    + glueArnPrefix
                    + ":catalog\","
                    + "\""
                    + glueArnPrefix
                    + ":database/allowed_*\","
                    + "\""
                    + glueArnPrefix
                    + ":table/allowed_*/*\","
                    + "\""
                    + glueArnPrefix
                    + ":userDefinedFunction/allowed_*/*\"]}]}")
            .build());
    waitForIamConsistency();

    GlueCatalog glueCatalog = new GlueCatalog();
    assumeRoleProperties.put("warehouse", "s3://path");
    glueCatalog.initialize("test", assumeRoleProperties);
    Assertions.assertThatThrownBy(
            () ->
                glueCatalog.createNamespace(
                    Namespace.of("denied_" + UUID.randomUUID().toString().replace("-", ""))))
        .isInstanceOf(AccessDeniedException.class);

    Namespace namespace = Namespace.of("allowed_" + UUID.randomUUID().toString().replace("-", ""));
    try {
      glueCatalog.createNamespace(namespace);
    } finally {
      glueCatalog.dropNamespace(namespace);
    }
  }

  @Test
  public void testAssumeRoleS3FileIO() throws Exception {
    String bucketArn = "arn:aws:s3:::" + AwsIntegTestUtil.testBucketName();
    iam.putRolePolicy(
        PutRolePolicyRequest.builder()
            .roleName(roleName)
            .policyName(policyName)
            .policyDocument(
                "{"
                    + "\"Version\":\"2012-10-17\","
                    + "\"Statement\":[{"
                    + "\"Sid\":\"policy1\","
                    + "\"Effect\":\"Allow\","
                    + "\"Action\":\"s3:ListBucket\","
                    + "\"Resource\":[\""
                    + bucketArn
                    + "\"],"
                    + "\"Condition\":{\"StringLike\":{\"s3:prefix\":[\"allowed/*\"]}}} ,{"
                    + "\"Sid\":\"policy2\","
                    + "\"Effect\":\"Allow\","
                    + "\"Action\":\"s3:GetObject\","
                    + "\"Resource\":[\""
                    + bucketArn
                    + "/allowed/*\"]}]}")
            .build());
    waitForIamConsistency();

    S3FileIO s3FileIO = new S3FileIO();
    s3FileIO.initialize(assumeRoleProperties);
    Assertions.assertThatThrownBy(
            () ->
                s3FileIO
                    .newInputFile("s3://" + AwsIntegTestUtil.testBucketName() + "/denied/file")
                    .exists())
        .isInstanceOf(S3Exception.class)
        .asInstanceOf(InstanceOfAssertFactories.type(S3Exception.class))
        .extracting(SdkServiceException::statusCode)
        .isEqualTo(403);

    InputFile inputFile =
        s3FileIO.newInputFile("s3://" + AwsIntegTestUtil.testBucketName() + "/allowed/file");
    Assert.assertFalse("should be able to access file", inputFile.exists());
  }

  private void waitForIamConsistency() throws Exception {
    Thread.sleep(10000); // sleep to make sure IAM up to date
  }
}
