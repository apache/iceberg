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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

public class AwsIntegTestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(AwsIntegTestUtil.class);

  private AwsIntegTestUtil() {
  }

  /**
   * Set the environment variable AWS_TEST_BUCKET for a default bucket to use for testing
   * @return bucket name
   */
  public static String testBucketName() {
    return System.getenv("AWS_TEST_BUCKET");
  }

  /**
   * Set the environment variable AWS_TEST_ACCOUNT_ID for a default account to use for testing
   * @return account id
   */
  public static String testAccountId() {
    return System.getenv("AWS_TEST_ACCOUNT_ID");
  }

  public static void cleanS3Bucket(S3Client s3, String bucketName, String prefix) {
    boolean hasContent = true;
    while (hasContent) {
      ListObjectsV2Response response = s3.listObjectsV2(ListObjectsV2Request.builder()
          .bucket(bucketName).prefix(prefix).build());
      hasContent = response.hasContents();
      if (hasContent) {
        s3.deleteObjects(DeleteObjectsRequest.builder().bucket(bucketName).delete(Delete.builder().objects(
            response.contents().stream()
                .map(obj -> ObjectIdentifier.builder().key(obj.key()).build())
                .collect(Collectors.toList())
        ).build()).build());
      }
    }
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
}
