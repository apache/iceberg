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

import org.apache.iceberg.events.Listener;
import org.apache.iceberg.util.EventParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.SnsException;

public class SNSListener implements Listener {
  private static final Logger LOG = LoggerFactory.getLogger(SNSListener.class);

  private String topicArn;
  // private AwsClientFactory awsClientFactory; // to be used later
  private SnsClient sns;

  public SNSListener(String topicArn, SnsClient sns) {
    this.sns = sns;
    this.topicArn = topicArn;
  }

  @Override
  public void notify(Object event) {
    try {
      String msg = EventParser.toJson(event);
      PublishRequest request = PublishRequest.builder()
              .message(msg)
              .topicArn(topicArn)
              .build();
      sns.publish(request);
    } catch (SnsException e) {
      LOG.error("Failed to send notification event to SNS topic", e);
    } catch (RuntimeException e) {
      LOG.error("Failed to notify subscriber", e);
    }
  }
}

