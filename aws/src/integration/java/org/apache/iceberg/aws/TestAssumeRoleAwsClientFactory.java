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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetBucketAccelerateConfigurationRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class TestAssumeRoleAwsClientFactory {

  private static final Logger LOG = LoggerFactory.getLogger(TestAssumeRoleAwsClientFactory.class);
  private Map<String, String> assumeRoleProperties;

  @Before
  public void before() {
    assumeRoleProperties = Maps.newHashMap();
    assumeRoleProperties.put(AwsProperties.CLIENT_FACTORY, AssumeRoleAwsClientFactory.class.getName());
    assumeRoleProperties.put(AwsProperties.HTTP_CLIENT_TYPE, AwsProperties.HTTP_CLIENT_TYPE_APACHE);
    assumeRoleProperties.put(AwsProperties.CLIENT_ASSUME_ROLE_REGION, AwsIntegTestUtil.testRegion());
    assumeRoleProperties.put(AwsProperties.CLIENT_ASSUME_ROLE_ARN, AwsIntegTestUtil.testAssumeRoleArn());
    assumeRoleProperties.put(AwsProperties.CLIENT_ASSUME_ROLE_TAGS_PREFIX + "key1", "value1");
    assumeRoleProperties.put(AwsProperties.CLIENT_ASSUME_ROLE_TAGS_PREFIX + "key2", "value2");
  }

  @Test
  public void testAssumeRoleGlueCatalog() {
    GlueCatalog glueCatalog = new GlueCatalog();
    assumeRoleProperties.put("warehouse", "s3://path");
    glueCatalog.initialize("test", assumeRoleProperties);
    try {
      glueCatalog.createNamespace(Namespace.of("denied_" + UUID.randomUUID().toString().replace("-", "")));
      Assert.fail("Access to Glue should be denied");
    } catch (GlueException e) {
      Assert.assertEquals(AccessDeniedException.class, e.getClass());
    }

    Namespace namespace = Namespace.of("iceberg_aws_ci_" + UUID.randomUUID().toString().replace("-", ""));
    try {
      glueCatalog.createNamespace(namespace);
    } catch (GlueException e) {
      LOG.error("fail to create or delete Glue database", e);
      Assert.fail("create namespace should succeed");
    } finally {
      glueCatalog.dropNamespace(namespace);
    }
  }

  @Test
  public void testAssumeRoleS3FileIO() {
    S3Client s3Client = AwsClientFactories.from(assumeRoleProperties).s3();
    try {
      s3Client.getBucketAccelerateConfiguration(GetBucketAccelerateConfigurationRequest.builder()
          .bucket(AwsIntegTestUtil.testBucketName())
          .build());
      Assert.fail("Access to s3 should be denied");
    } catch (S3Exception e) {
      Assert.assertEquals("Should see 403 error code", 403, e.statusCode());
    }

    S3FileIO s3FileIO = new S3FileIO();
    s3FileIO.initialize(assumeRoleProperties);
    InputFile inputFile = s3FileIO.newInputFile("s3://" + AwsIntegTestUtil.testBucketName() + "/allowed/file");
    Assert.assertFalse("should be able to access file", inputFile.exists());
  }
}
