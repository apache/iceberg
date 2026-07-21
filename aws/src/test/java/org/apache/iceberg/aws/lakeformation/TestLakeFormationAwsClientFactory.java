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

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.Map;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.kms.KmsClient;

public class TestLakeFormationAwsClientFactory {

  @Test
  public void testKmsClientAppliesEndpointOverrideForRegisteredTable() {
    GlueClient mockGlueClient = Mockito.mock(GlueClient.class);
    Mockito.when(mockGlueClient.getTable(Mockito.any(GetTableRequest.class)))
        .thenReturn(
            GetTableResponse.builder()
                .table(Table.builder().isRegisteredWithLakeFormation(true).build())
                .build());

    LakeFormationAwsClientFactory factory =
        new LakeFormationAwsClientFactory() {
          @Override
          public GlueClient glue() {
            return mockGlueClient;
          }
        };
    factory.initialize(getLakeFormationClientFactoryProperties());

    KmsClient kmsClient = factory.kms();
    assertThat(kmsClient.serviceClientConfiguration().endpointOverride())
        .hasValue(URI.create("https://kms.custom.endpoint"));
  }

  private Map<String, String> getLakeFormationClientFactoryProperties() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.CLIENT_ASSUME_ROLE_ARN, "arn::test");
    properties.put(AwsProperties.CLIENT_ASSUME_ROLE_REGION, "us-east-1");
    properties.put(
        AwsProperties.CLIENT_ASSUME_ROLE_TAGS_PREFIX
            + LakeFormationAwsClientFactory.LF_AUTHORIZED_CALLER,
        "emr");
    properties.put(AwsProperties.LAKE_FORMATION_DB_NAME, "test-db");
    properties.put(AwsProperties.LAKE_FORMATION_TABLE_NAME, "test-table");
    properties.put(AwsProperties.GLUE_ACCOUNT_ID, "123456789012");
    properties.put(AwsProperties.KMS_ENDPOINT, "https://kms.custom.endpoint");
    return properties;
  }
}
