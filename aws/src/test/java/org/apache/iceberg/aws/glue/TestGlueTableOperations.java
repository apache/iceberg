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
package org.apache.iceberg.aws.glue;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.BaseMetastoreOperations.CommitStatus;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateTableResponse;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableResponse;

public class TestGlueTableOperations {

  private static final String CATALOG_NAME = "glue";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of("db", "table");
  private static final String CUSTOM_CATALOG_ID = "myCatalogId";

  private GlueClient glue;

  private GlueTableOperations newTableOperations(Map<String, String> properties) {
    glue = Mockito.mock(GlueClient.class);
    Mockito.doReturn(CreateTableResponse.builder().build())
        .when(glue)
        .createTable(Mockito.any(CreateTableRequest.class));
    Mockito.doReturn(DeleteTableResponse.builder().build())
        .when(glue)
        .deleteTable(Mockito.any(DeleteTableRequest.class));
    AwsProperties awsProperties = new AwsProperties(properties);
    return new GlueTableOperations(
        glue, null, CATALOG_NAME, awsProperties, ImmutableMap.of(), null, TABLE_IDENTIFIER);
  }

  @Test
  public void createTempTableUsesConfiguredCatalogId() {
    GlueTableOperations ops =
        newTableOperations(
            ImmutableMap.of(
                AwsProperties.GLUE_LAKEFORMATION_ENABLED,
                "true",
                AwsProperties.GLUE_CATALOG_ID,
                CUSTOM_CATALOG_ID));

    boolean created = ops.createGlueTempTableIfNecessary(null, "s3://bucket/metadata.json");

    assertThat(created).isTrue();
    ArgumentCaptor<CreateTableRequest> captor = ArgumentCaptor.forClass(CreateTableRequest.class);
    Mockito.verify(glue).createTable(captor.capture());
    assertThat(captor.getValue().catalogId()).isEqualTo(CUSTOM_CATALOG_ID);
  }

  @Test
  public void deleteTempTableUsesConfiguredCatalogId() {
    GlueTableOperations ops =
        newTableOperations(
            ImmutableMap.of(
                AwsProperties.GLUE_LAKEFORMATION_ENABLED,
                "true",
                AwsProperties.GLUE_CATALOG_ID,
                CUSTOM_CATALOG_ID));

    ops.cleanupGlueTempTableIfNecessary(true, CommitStatus.FAILURE);

    ArgumentCaptor<DeleteTableRequest> captor = ArgumentCaptor.forClass(DeleteTableRequest.class);
    Mockito.verify(glue).deleteTable(captor.capture());
    assertThat(captor.getValue().catalogId()).isEqualTo(CUSTOM_CATALOG_ID);
  }

  @Test
  public void tempTableCatalogIdIsNullWhenNotConfigured() {
    GlueTableOperations ops =
        newTableOperations(ImmutableMap.of(AwsProperties.GLUE_LAKEFORMATION_ENABLED, "true"));

    ops.createGlueTempTableIfNecessary(null, "s3://bucket/metadata.json");
    ops.cleanupGlueTempTableIfNecessary(true, CommitStatus.FAILURE);

    ArgumentCaptor<CreateTableRequest> createCaptor =
        ArgumentCaptor.forClass(CreateTableRequest.class);
    Mockito.verify(glue).createTable(createCaptor.capture());
    assertThat(createCaptor.getValue().catalogId()).isNull();

    ArgumentCaptor<DeleteTableRequest> deleteCaptor =
        ArgumentCaptor.forClass(DeleteTableRequest.class);
    Mockito.verify(glue).deleteTable(deleteCaptor.capture());
    assertThat(deleteCaptor.getValue().catalogId()).isNull();
  }
}
