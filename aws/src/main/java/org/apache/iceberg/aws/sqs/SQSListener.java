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

package org.apache.iceberg.aws.sqs;

import java.util.Map;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.events.Listener;
import org.apache.iceberg.util.EventParser;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

public class SQSListener<T> implements Listener<T> {
  private static final Logger LOG = LoggerFactory.getLogger(SQSListener.class);

  private String queueUrl;
  private SqsClient sqs;
  private int retry;
  private long retryIntervalMs;

  public SQSListener() {
  }

  public SQSListener(String queueUrl, SqsClient sqs, int retry, long retryIntervalMs) {
    this.sqs = sqs;
    this.queueUrl = queueUrl;
    this.retry = retry;
    this.retryIntervalMs = retryIntervalMs;
  }

  @Override
  public void notify(Object event) {
    try {
      String msg = EventParser.toJson(event);
      SendMessageRequest request = SendMessageRequest.builder()
              .queueUrl(queueUrl)
              .messageBody(msg)
              .build();
      Tasks.foreach(request)
          .exponentialBackoff(retryIntervalMs, retryIntervalMs, retryIntervalMs, 1 /* scale factor */)
          .retry(retry)
          .onlyRetryOn(QueueDoesNotExistException.class)
          .run(sqs::sendMessage);
    } catch (SqsException e) {
      LOG.error("Failed to send notification event to SQS", e);
    } catch (RuntimeException e) {
      LOG.error("Failed to add to queue", e);
    }
  }

  @Override
  public void initialize(String listenerName, Map<String, String> properties) {
    AwsClientFactory factory = AwsClientFactories.from(properties);
    this.sqs = factory.sqs();

    if (listenerName == null) {
      throw new NullPointerException("Listener Name cannot be null");
    }

    if (properties.get(AwsProperties.SQS_QUEUE_URL) == null) {
      throw new NullPointerException("SNS queue url cannot be null");
    }

    this.queueUrl = properties.get(AwsProperties.SQS_QUEUE_URL);

    this.retry = PropertyUtil.propertyAsInt(
            properties, AwsProperties.SQS_RETRY, AwsProperties.SQS_RETRY_DEFAULT);

    this.retryIntervalMs = PropertyUtil.propertyAsInt(
            properties, AwsProperties.SQS_RETRY_INTERVAL_MS, AwsProperties.SQS_RETRY_INTERVAL_MS_DEFAULT);
  }
}

