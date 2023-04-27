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
package org.apache.iceberg.aws.metrics;

import java.util.Map;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.HttpClientProperties;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

public class DefaultSqsMetricsReporterAwsClientFactory
    implements SqsMetricsReporterAwsClientFactory {

  private SqsMetricsReporterProperties sqsMetricsReporterProperties;
  private HttpClientProperties httpClientProperties;
  private AwsClientProperties awsClientProperties;

  DefaultSqsMetricsReporterAwsClientFactory() {
    this.sqsMetricsReporterProperties = new SqsMetricsReporterProperties();
    this.httpClientProperties = new HttpClientProperties();
    this.awsClientProperties = new AwsClientProperties();
  }

  @Override
  public void initialize(Map<String, String> properties) {
    Map<String, String> catalogProperties = Maps.newHashMap(properties);
    catalogProperties.put(
        HttpClientProperties.CLIENT_TYPE, HttpClientProperties.HTTP_CLIENT_TYPE_NETTYNIO);
    this.sqsMetricsReporterProperties = new SqsMetricsReporterProperties(catalogProperties);
    this.awsClientProperties = new AwsClientProperties(catalogProperties);
    this.httpClientProperties = new HttpClientProperties(catalogProperties);
  }

  @Override
  public SqsAsyncClient sqs() {
    return SqsAsyncClient.builder()
        .applyMutation(awsClientProperties::applyClientRegionConfiguration)
        .applyMutation(httpClientProperties::applyAsyncHttpClientConfigurations)
        .build();
  }
}
