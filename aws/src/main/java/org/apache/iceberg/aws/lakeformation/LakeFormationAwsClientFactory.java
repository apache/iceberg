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

import java.util.Map;
import org.apache.iceberg.aws.AssumeRoleAwsClientFactory;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.regions.PartitionMetadata;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryGlueTableCredentialsRequest;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryGlueTableCredentialsResponse;
import software.amazon.awssdk.services.lakeformation.model.PermissionType;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * This implementation of AwsClientFactory is used by default if {@link
 * org.apache.iceberg.aws.AwsProperties#GLUE_LAKEFORMATION_ENABLED} is set to true. It uses the
 * default credential chain to assume role. Third-party engines can further extend this class to any
 * custom credential setup.
 *
 * <p>It extends AssumeRoleAwsClientFactory to reuse the assuming-role approach for all clients
 * except S3 and KMS. If a table is registered with LakeFormation, the S3/KMS client will use
 * LakeFormation vended credentials, otherwise it uses AssumingRole credentials. For using
 * LakeFormation credential vending for a third-party query engine, see:
 * https://docs.aws.amazon.com/lake-formation/latest/dg/register-query-engine.html
 */
public class LakeFormationAwsClientFactory extends AssumeRoleAwsClientFactory {

  public static final String LF_AUTHORIZED_CALLER = "LakeFormationAuthorizedCaller";

  private String dbName;
  private String tableName;
  private String glueCatalogId;
  private String glueAccountId;

  public LakeFormationAwsClientFactory() {}

  @Override
  public void initialize(Map<String, String> catalogProperties) {
    super.initialize(catalogProperties);
    Preconditions.checkArgument(
        awsProperties().stsClientAssumeRoleTags().stream()
            .anyMatch(t -> LF_AUTHORIZED_CALLER.equals(t.key())),
        "STS assume role session tag %s must be set using %s to use LakeFormation client factory",
        LF_AUTHORIZED_CALLER,
        AwsProperties.CLIENT_ASSUME_ROLE_TAGS_PREFIX);
    this.dbName = catalogProperties.get(AwsProperties.LAKE_FORMATION_DB_NAME);
    this.tableName = catalogProperties.get(AwsProperties.LAKE_FORMATION_TABLE_NAME);
    this.glueCatalogId = catalogProperties.get(AwsProperties.GLUE_CATALOG_ID);
    this.glueAccountId = catalogProperties.get(AwsProperties.GLUE_ACCOUNT_ID);
  }

  @Override
  public S3Client s3() {
    if (isTableRegisteredWithLakeFormation()) {
      return S3Client.builder()
          .applyMutation(httpClientProperties()::applyHttpClientConfigurations)
          .applyMutation(s3FileIOProperties()::applyEndpointConfigurations)
          .applyMutation(s3FileIOProperties()::applyServiceConfigurations)
          .applyMutation(s3FileIOProperties()::applyRetryConfigurations)
          .credentialsProvider(
              new LakeFormationCredentialsProvider(lakeFormation(), buildTableArn()))
          .region(Region.of(region()))
          .build();
    } else {
      return super.s3();
    }
  }

  @Override
  public KmsClient kms() {
    if (isTableRegisteredWithLakeFormation()) {
      return KmsClient.builder()
          .applyMutation(httpClientProperties()::applyHttpClientConfigurations)
          .credentialsProvider(
              new LakeFormationCredentialsProvider(lakeFormation(), buildTableArn()))
          .region(Region.of(region()))
          .build();
    } else {
      return super.kms();
    }
  }

  private boolean isTableRegisteredWithLakeFormation() {
    Preconditions.checkArgument(
        dbName != null && !dbName.isEmpty(), "Database name can not be empty");
    Preconditions.checkArgument(
        tableName != null && !tableName.isEmpty(), "Table name can not be empty");

    GetTableResponse response =
        glue()
            .getTable(
                GetTableRequest.builder()
                    .catalogId(glueCatalogId)
                    .databaseName(dbName)
                    .name(tableName)
                    .build());
    return response.table().isRegisteredWithLakeFormation();
  }

  private String buildTableArn() {
    Preconditions.checkArgument(
        glueAccountId != null && !glueAccountId.isEmpty(),
        "%s can not be empty",
        AwsProperties.GLUE_ACCOUNT_ID);
    String partitionName = PartitionMetadata.of(Region.of(region())).id();
    return String.format(
        "arn:%s:glue:%s:%s:table/%s/%s", partitionName, region(), glueAccountId, dbName, tableName);
  }

  private LakeFormationClient lakeFormation() {
    return LakeFormationClient.builder()
        .applyMutation(this::applyAssumeRoleConfigurations)
        .applyMutation(httpClientProperties()::applyHttpClientConfigurations)
        .build();
  }

  static class LakeFormationCredentialsProvider implements AwsCredentialsProvider {
    private final LakeFormationClient client;
    private final String tableArn;

    LakeFormationCredentialsProvider(LakeFormationClient lakeFormationClient, String tableArn) {
      this.client = lakeFormationClient;
      this.tableArn = tableArn;
    }

    @Override
    public AwsCredentials resolveCredentials() {
      GetTemporaryGlueTableCredentialsRequest getTemporaryGlueTableCredentialsRequest =
          GetTemporaryGlueTableCredentialsRequest.builder()
              .tableArn(tableArn)
              // Now only two permission types (COLUMN_PERMISSION and CELL_FILTER_PERMISSION) are
              // supported
              // and Iceberg only supports COLUMN_PERMISSION at this time
              .supportedPermissionTypes(PermissionType.COLUMN_PERMISSION)
              .build();
      GetTemporaryGlueTableCredentialsResponse response =
          client.getTemporaryGlueTableCredentials(getTemporaryGlueTableCredentialsRequest);
      return AwsSessionCredentials.create(
          response.accessKeyId(), response.secretAccessKey(), response.sessionToken());
    }
  }
}
