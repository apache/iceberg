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

package org.apache.iceberg.aws.sns;

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
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.NotFoundException;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.SnsException;

public class SNSListener<T> implements Listener<T> {
  private static final Logger LOG = LoggerFactory.getLogger(SNSListener.class);

  private String topicArn;
  private SnsClient sns;
  private int retry;
  private long retryIntervalMs;

  public SNSListener() {
  }

  public SNSListener(String topicArn, SnsClient sns, int retry, long retryIntervalMs) {
    this.sns = sns;
    this.topicArn = topicArn;
    this.retry = retry;
    this.retryIntervalMs = retryIntervalMs;
  }

  @Override
  public void notify(Object event) {
    try {
      String msg = EventParser.toJson(event);
      PublishRequest request = PublishRequest.builder()
              .message(msg)
              .topicArn(topicArn)
              .build();
      Tasks.foreach(request)
          .exponentialBackoff(retryIntervalMs, retryIntervalMs, retryIntervalMs, 1 /* scale factor */)
          .retry(retry)
          .onlyRetryOn(NotFoundException.class)
          .run(sns::publish);
    } catch (SnsException e) {
      LOG.error("Failed to send notification event to SNS topic", e);
    } catch (RuntimeException e) {
      LOG.error("Failed to notify subscriber", e);
    }
  }

  @Override
  public void initialize(String listenerName, Map<String, String> properties) {
    AwsClientFactory factory = AwsClientFactories.from(properties);
    this.sns = factory.sns();

    if (listenerName == null) {
      throw new NullPointerException("Listener Name cannot be null");
    }

    if (properties.get(AwsProperties.SNS_TOPIC_ARN) == null) {
      throw new NullPointerException("SNS queue url cannot be null");
    }

    this.topicArn = properties.get(AwsProperties.SNS_TOPIC_ARN);

    this.retry = PropertyUtil.propertyAsInt(
            properties, AwsProperties.SNS_RETRY, AwsProperties.SNS_RETRY_DEFAULT);

    this.retryIntervalMs = PropertyUtil.propertyAsInt(
            properties, AwsProperties.SNS_RETRY_INTERVAL_MS, AwsProperties.SNS_RETRY_INTERVAL_MS_DEFAULT);
  }
}

