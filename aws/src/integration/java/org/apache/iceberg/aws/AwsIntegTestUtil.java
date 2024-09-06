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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.ObjectVersion;
import software.amazon.awssdk.services.s3.paginators.ListObjectVersionsIterable;
import software.amazon.awssdk.services.s3control.S3ControlClient;
import software.amazon.awssdk.services.s3control.model.CreateAccessPointRequest;
import software.amazon.awssdk.services.s3control.model.DeleteAccessPointRequest;

public class AwsIntegTestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(AwsIntegTestUtil.class);

  private AwsIntegTestUtil() {}

  /**
   * Get the environment variable AWS_REGION to use for testing
   *
   * @return region
   */
  public static String testRegion() {
    return System.getenv("AWS_REGION");
  }

  /**
   * Get the environment variable AWS_CROSS_REGION to use for testing
   *
   * @return region
   */
  public static String testCrossRegion() {
    String crossRegion = System.getenv("AWS_CROSS_REGION");
    Preconditions.checkArgument(
        !testRegion().equals(crossRegion),
        "AWS_REGION should not be equal to " + "AWS_CROSS_REGION");
    return crossRegion;
  }

  /**
   * Set the environment variable AWS_TEST_BUCKET for a default bucket to use for testing
   *
   * @return bucket name
   */
  public static String testBucketName() {
    return System.getenv("AWS_TEST_BUCKET");
  }

  /**
   * Set the environment variable AWS_TEST_CROSS_REGION_BUCKET for a default bucket to use for
   * testing
   *
   * @return bucket name
   */
  public static String testCrossRegionBucketName() {
    return System.getenv("AWS_TEST_CROSS_REGION_BUCKET");
  }

  /**
   * Set the environment variable AWS_TEST_ACCOUNT_ID for a default account to use for testing
   *
   * @return account id
   */
  public static String testAccountId() {
    return System.getenv("AWS_TEST_ACCOUNT_ID");
  }

  /**
   * Set the environment variable AWS_TEST_MULTI_REGION_ACCESS_POINT_ALIAS for a default account to
   * use for testing. Developers need to create a S3 multi region access point before running
   * integration tests because creating it takes a few minutes
   *
   * @return The alias of S3 multi region access point route to the default S3 bucket
   */
  public static String testMultiRegionAccessPointAlias() {
    return System.getenv("AWS_TEST_MULTI_REGION_ACCESS_POINT_ALIAS");
  }

  public static void cleanS3Bucket(S3Client s3, String bucketName, String prefix) {
    ListObjectVersionsIterable response =
        s3.listObjectVersionsPaginator(
            ListObjectVersionsRequest.builder().bucket(bucketName).prefix(prefix).build());
    List<ObjectVersion> versionsToDelete = Lists.newArrayList();
    int batchDeletionSize = 1000;
    response.versions().stream()
        .forEach(
            version -> {
              versionsToDelete.add(version);
              if (versionsToDelete.size() == batchDeletionSize) {
                deleteObjectVersions(s3, bucketName, versionsToDelete);
                versionsToDelete.clear();
              }
            });

    if (!versionsToDelete.isEmpty()) {
      deleteObjectVersions(s3, bucketName, versionsToDelete);
    }
  }

  private static void deleteObjectVersions(
      S3Client s3, String bucket, List<ObjectVersion> objectVersions) {
    s3.deleteObjects(
        DeleteObjectsRequest.builder()
            .bucket(bucket)
            .delete(
                Delete.builder()
                    .objects(
                        objectVersions.stream()
                            .map(
                                obj ->
                                    ObjectIdentifier.builder()
                                        .key(obj.key())
                                        .versionId(obj.versionId())
                                        .build())
                            .collect(Collectors.toList()))
                    .build())
            .build());
  }

  public static void cleanGlueCatalog(GlueClient glue, List<String> namespaces) {
    for (String namespace : namespaces) {
      try {
        // delete db also delete tables
        glue.deleteDatabase(DeleteDatabaseRequest.builder().name(namespace).build());
      } catch (Exception e) {
        LOG.error("Cannot delete namespace {}", namespace, e);
      }
    }
  }

  public static S3ControlClient createS3ControlClient(String region) {
    return S3ControlClient.builder()
        .httpClientBuilder(UrlConnectionHttpClient.builder())
        .region(Region.of(region))
        .build();
  }

  public static void createAccessPoint(
      S3ControlClient s3ControlClient, String accessPointName, String bucketName) {
    try {
      s3ControlClient.createAccessPoint(
          CreateAccessPointRequest.builder()
              .name(accessPointName)
              .bucket(bucketName)
              .accountId(testAccountId())
              .build());
    } catch (Exception e) {
      LOG.error("Cannot create access point {}", accessPointName, e);
    }
  }

  public static void deleteAccessPoint(S3ControlClient s3ControlClient, String accessPointName) {
    try {
      s3ControlClient.deleteAccessPoint(
          DeleteAccessPointRequest.builder()
              .name(accessPointName)
              .accountId(testAccountId())
              .build());
    } catch (Exception e) {
      LOG.error("Cannot delete access point {}", accessPointName, e);
    }
  }
}
