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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Class to store the properties used by {@link SqsMetricsReporter} */
public class SqsMetricsReporterProperties {

  /**
   * This property is used to pass in the aws client factory implementation class for SQS Metrics
   * Reporter. The class should implement {@link SqsMetricsReporterAwsClientFactory}. For example,
   * {@link DefaultSqsMetricsReporterAwsClientFactory} implements {@link
   * SqsMetricsReporterAwsClientFactory}. If this property wasn't set, will load one of {@link
   * org.apache.iceberg.aws.AwsClientFactory} factory classes to provide backward compatibility.
   */
  public static final String METRICS_REPORTER_SQS_CLIENT_FACTORY_IMPL =
      "metrics-reporter.sqs.client-factory-impl";

  public static final String METRICS_REPORTER_SQS_QUEUE_URL = "metrics-reporter.sqs.queue-url";

  private String sqsQueueUrl;

  public SqsMetricsReporterProperties() {
    this.sqsQueueUrl = null;
  }

  public SqsMetricsReporterProperties(Map<String, String> properties) {
    this.sqsQueueUrl = properties.get(METRICS_REPORTER_SQS_QUEUE_URL);
    Preconditions.checkArgument(
        null != sqsQueueUrl, "%s should be be set", METRICS_REPORTER_SQS_QUEUE_URL);
  }

  public String sqsQueueUrl() {
    return sqsQueueUrl;
  }

  public void setSqsQueueUrl(String sqsQueueUrl) {
    this.sqsQueueUrl = sqsQueueUrl;
  }
}
