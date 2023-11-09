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
package org.apache.iceberg.aws.lakeformation;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.aws.AwsIntegTestUtil;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.HttpClientProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.model.CreateRoleRequest;
import software.amazon.awssdk.services.iam.model.CreateRoleResponse;
import software.amazon.awssdk.services.iam.model.DeleteRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.DeleteRoleRequest;
import software.amazon.awssdk.services.iam.model.GetRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.PutRolePolicyRequest;

public class TestLakeFormationAwsClientFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestLakeFormationAwsClientFactory.class);
  private static final int IAM_PROPAGATION_DELAY = 10000;
  private static final int ASSUME_ROLE_SESSION_DURATION = 3600;

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
                .maxSessionDuration(ASSUME_ROLE_SESSION_DURATION)
                .build());
    assumeRoleProperties = Maps.newHashMap();
    assumeRoleProperties.put(AwsProperties.CLIENT_ASSUME_ROLE_REGION, "us-east-1");
    assumeRoleProperties.put(AwsProperties.GLUE_LAKEFORMATION_ENABLED, "true");
    assumeRoleProperties.put(
        HttpClientProperties.CLIENT_TYPE, HttpClientProperties.CLIENT_TYPE_APACHE);
    assumeRoleProperties.put(AwsProperties.CLIENT_ASSUME_ROLE_ARN, response.role().arn());
    assumeRoleProperties.put(
        AwsProperties.CLIENT_ASSUME_ROLE_TAGS_PREFIX
            + LakeFormationAwsClientFactory.LF_AUTHORIZED_CALLER,
        "emr");
    policyName = UUID.randomUUID().toString();
  }

  @After
  public void after() {
    iam.deleteRolePolicy(
        DeleteRolePolicyRequest.builder().roleName(roleName).policyName(policyName).build());
    iam.deleteRole(DeleteRoleRequest.builder().roleName(roleName).build());
  }

  @Test
  public void testLakeFormationEnabledGlueCatalog() throws Exception {
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
                    + "\"Action\":[\"glue:CreateDatabase\",\"glue:DeleteDatabase\","
                    + "\"glue:Get*\",\"lakeformation:GetDataAccess\"],"
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
    Awaitility.await()
        .pollDelay(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () ->
                Assertions.assertThat(
                        iam.getRolePolicy(
                            GetRolePolicyRequest.builder()
                                .roleName(roleName)
                                .policyName(policyName)
                                .build()))
                    .isNotNull());
    GlueCatalog glueCatalog = new GlueCatalog();
    assumeRoleProperties.put("warehouse", "s3://path");
    glueCatalog.initialize("test", assumeRoleProperties);
    Namespace deniedNamespace =
        Namespace.of("denied_" + UUID.randomUUID().toString().replace("-", ""));
    try {
      glueCatalog.createNamespace(deniedNamespace);
      Assert.fail("Access to Glue should be denied");
    } catch (GlueException e) {
      Assert.assertEquals(AccessDeniedException.class, e.getClass());
    } catch (AssertionError e) {
      glueCatalog.dropNamespace(deniedNamespace);
      throw e;
    }

    Namespace allowedNamespace =
        Namespace.of("allowed_" + UUID.randomUUID().toString().replace("-", ""));
    try {
      glueCatalog.createNamespace(allowedNamespace);
    } catch (GlueException e) {
      LOG.error("fail to create Glue database", e);
      Assert.fail("create namespace should succeed");
    } finally {
      glueCatalog.dropNamespace(allowedNamespace);
      try {
        glueCatalog.close();
      } catch (Exception e) {
        // swallow exception during closing
        LOG.error("Error closing GlueCatalog", e);
      }
    }
  }
}
