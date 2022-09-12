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

import com.github.benmanes.caffeine.cache.CacheLoader;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryGlueTableCredentialsRequest;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryGlueTableCredentialsResponse;
import software.amazon.awssdk.services.lakeformation.model.PermissionType;

class LakeFormationCredentialsCacheLoader
    implements CacheLoader<String, LakeFormationTemporaryCredentials> {
  private LakeFormationClient client;

  LakeFormationCredentialsCacheLoader(LakeFormationClient client) {
    this.client = client;
  }

  @Override
  public LakeFormationTemporaryCredentials load(String key) {
    GetTemporaryGlueTableCredentialsRequest getTemporaryGlueTableCredentialsRequest =
        GetTemporaryGlueTableCredentialsRequest.builder()
            .tableArn(key)
            // Now only two permission types (COLUMN_PERMISSION and CELL_FILTER_PERMISSION) are
            // supported
            // and Iceberg only supports COLUMN_PERMISSION at this time
            .supportedPermissionTypes(PermissionType.COLUMN_PERMISSION)
            .build();
    GetTemporaryGlueTableCredentialsResponse response =
        client.getTemporaryGlueTableCredentials(getTemporaryGlueTableCredentialsRequest);
    AwsSessionCredentials credentials =
        AwsSessionCredentials.create(
            response.accessKeyId(), response.secretAccessKey(), response.sessionToken());
    return new LakeFormationTemporaryCredentials(credentials, response.expiration());
  }
}
