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
import java.util.concurrent.CompletableFuture;
import org.apache.iceberg.aws.SqsMetricsReporterAwsClientFactories;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.CommitReportParser;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.metrics.ScanReportParser;
import org.apache.iceberg.util.SerializableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

/** An implementation of {@link MetricsReporter} which reports {@link MetricsReport} to SQS */
public class SqsMetricsReporter implements MetricsReporter {

  private static final Logger LOG = LoggerFactory.getLogger(SqsMetricsReporter.class);

  private SerializableSupplier<SqsAsyncClient> sqs;

  private transient volatile SqsAsyncClient client;

  private String sqsQueueUrl;

  public SqsMetricsReporter() {}

  public SqsMetricsReporter(SerializableSupplier<SqsAsyncClient> sqs, String sqsQueueUrl) {
    this.sqs = sqs;
    this.client = sqs.get();
    this.sqsQueueUrl = sqsQueueUrl;
  }

  @Override
  public void initialize(Map<String, String> properties) {
    SqsMetricsReporterProperties sqsMetricsReporterProperties =
        new SqsMetricsReporterProperties(properties);
    Object clientFactory = SqsMetricsReporterAwsClientFactories.initialize(properties);
    if (clientFactory instanceof SqsMetricsReporterAwsClientFactory) {
      this.sqs = ((SqsMetricsReporterAwsClientFactory) clientFactory)::sqs;
    }
    this.client = sqs.get();
    this.sqsQueueUrl = sqsMetricsReporterProperties.sqsQueueUrl();
  }

  @Override
  public void report(MetricsReport report) {
    if (null == report) {
      LOG.warn("Received invalid metrics report: null");
      return;
    }

    try {
      String message = null;
      if (report instanceof CommitReport) {
        message = CommitReportParser.toJson((CommitReport) report);
      } else if (report instanceof ScanReport) {
        message = ScanReportParser.toJson((ScanReport) report);
      }

      if (null == message) {
        LOG.warn("Received unknown MetricsReport type");
        return;
      }

      CompletableFuture<SendMessageResponse> future =
          client.sendMessage(
              SendMessageRequest.builder().messageBody(message).queueUrl(sqsQueueUrl).build());
      future.whenComplete(
          (response, error) -> {
            if (response != null) {
              LOG.info("Metrics {} reported to: {}", response, sqsQueueUrl);
            } else {
              if (error != null) {
                LOG.error("Failed to report metrics to SQS queue: {}", error.getMessage());
              }
            }
          });
    } catch (Exception e) {
      LOG.warn("Failed to report metrics to SQS queue {}", sqsQueueUrl, e);
    }
  }
}
