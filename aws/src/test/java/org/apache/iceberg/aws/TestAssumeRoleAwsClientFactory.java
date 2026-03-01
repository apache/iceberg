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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public class TestAssumeRoleAwsClientFactory {
  @Test
  public void testApplyAssumeRoleConfigurationsSetsRegion() {
    Map<String, String> properties =
        ImmutableMap.of(
            AwsProperties.CLIENT_ASSUME_ROLE_ARN, "arn:aws:iam::123456789012:role/test-role",
            AwsProperties.CLIENT_ASSUME_ROLE_REGION, "ap-southeeast-1");

    AssumeRoleAwsClientFactory factory = new AssumeRoleAwsClientFactory();
    factory.initialize(properties);

    S3ClientBuilder mockBuilder = Mockito.mock(S3ClientBuilder.class);

    Mockito.when(mockBuilder.region(Mockito.any())).thenReturn(mockBuilder);
    Mockito.when(mockBuilder.credentialsProvider(Mockito.any())).thenReturn(mockBuilder);

    ArgumentCaptor<Region> regionCaptor = ArgumentCaptor.forClass(Region.class);

    factory.applyAssumeRoleConfigurations(mockBuilder);

    Mockito.verify(mockBuilder).region(regionCaptor.capture());

    Region captured = regionCaptor.getValue();
    assertThat(captured.id())
        .as("Region should match CLIENT_ASSUME_ROLE_REGION for cases outside AWS machines")
        .isEqualTo("ap-southeeast-1");
  }
}
