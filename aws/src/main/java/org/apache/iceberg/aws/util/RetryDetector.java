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
package org.apache.iceberg.aws.util;

import java.util.concurrent.atomic.AtomicBoolean;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;

/**
 * Metrics are the only reliable way provided by the AWS SDK to determine if an API call was
 * retried. This class can be attached to an AWS API call and checked after to determine if retries
 * occurred.
 */
public class RetryDetector implements MetricPublisher {
  private final AtomicBoolean retried = new AtomicBoolean(false);

  @Override
  public void publish(MetricCollection metricCollection) {
    if (!retried.get()) {
      if (metricCollection.metricValues(CoreMetric.RETRY_COUNT).stream().anyMatch(i -> i > 0)) {
        retried.set(true);
      } else {
        metricCollection.children().forEach(this::publish);
      }
    }
  }

  @Override
  public void close() {}

  public boolean retried() {
    return retried.get();
  }
}
