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

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricCollector;

public class TestRetryDetector {
  private static final String METRICS_NAME = "name";

  @Test
  public void testNoMetrics() {
    RetryDetector detector = new RetryDetector();
    Assert.assertFalse("Should default to false", detector.retried());
  }

  @Test
  public void testRetryCountMissing() {
    MetricCollector metrics = MetricCollector.create(METRICS_NAME);

    RetryDetector detector = new RetryDetector();
    detector.publish(metrics.collect());
    Assert.assertFalse(
        "Should not detect retries if RETRY_COUNT metric is not reported", detector.retried());
  }

  @Test
  public void testRetryCountZero() {
    MetricCollector metrics = MetricCollector.create(METRICS_NAME);
    metrics.reportMetric(CoreMetric.RETRY_COUNT, 0);

    RetryDetector detector = new RetryDetector();
    detector.publish(metrics.collect());
    Assert.assertFalse("Should not detect retries if RETRY_COUNT is zero", detector.retried());
  }

  @Test
  public void testRetryCountNonZero() {
    MetricCollector metrics = MetricCollector.create(METRICS_NAME);
    metrics.reportMetric(CoreMetric.RETRY_COUNT, 1);

    RetryDetector detector = new RetryDetector();
    detector.publish(metrics.collect());
    Assert.assertTrue("Should detect retries if RETRY_COUNT is non-zero", detector.retried());
  }

  @Test
  public void testMultipleRetryCounts() {
    MetricCollector metrics = MetricCollector.create(METRICS_NAME);
    metrics.reportMetric(CoreMetric.RETRY_COUNT, 0);
    metrics.reportMetric(CoreMetric.RETRY_COUNT, 1);

    RetryDetector detector = new RetryDetector();
    detector.publish(metrics.collect());
    Assert.assertTrue(
        "Should detect retries if even one RETRY_COUNT is non-zero", detector.retried());
  }

  @Test
  public void testNestedRetryCountZero() {
    MetricCollector metrics = MetricCollector.create(METRICS_NAME);
    metrics.reportMetric(CoreMetric.RETRY_COUNT, 0);
    MetricCollector childMetrics = metrics.createChild("child1").createChild("child2");
    childMetrics.reportMetric(CoreMetric.RETRY_COUNT, 0);

    RetryDetector detector = new RetryDetector();
    detector.publish(metrics.collect());
    Assert.assertFalse(
        "Should not detect retries if nested RETRY_COUNT is zero", detector.retried());
  }

  @Test
  public void testNestedRetryCountNonZero() {
    MetricCollector metrics = MetricCollector.create(METRICS_NAME);
    metrics.reportMetric(CoreMetric.RETRY_COUNT, 0);
    MetricCollector childMetrics = metrics.createChild("child1").createChild("child2");
    childMetrics.reportMetric(CoreMetric.RETRY_COUNT, 1);

    RetryDetector detector = new RetryDetector();
    detector.publish(metrics.collect());
    Assert.assertTrue(
        "Should detect retries if nested RETRY_COUNT is non-zero", detector.retried());
  }

  @Test
  public void testNestedRetryCountMultipleChildren() {
    MetricCollector metrics = MetricCollector.create(METRICS_NAME);
    metrics.reportMetric(CoreMetric.RETRY_COUNT, 0);
    for (int i = 0; i < 5; i++) {
      MetricCollector childMetrics = metrics.createChild("child" + i);
      childMetrics.reportMetric(CoreMetric.RETRY_COUNT, 0);
    }

    MetricCollector childMetrics = metrics.createChild("child10");
    childMetrics.reportMetric(CoreMetric.RETRY_COUNT, 1);

    RetryDetector detector = new RetryDetector();
    detector.publish(metrics.collect());
    Assert.assertTrue(
        "Should detect retries if even one nested RETRY_COUNT is non-zero", detector.retried());
  }

  @Test
  public void testMultipleCollectionsReported() {
    MetricCollector metrics1 = MetricCollector.create(METRICS_NAME);
    metrics1.reportMetric(CoreMetric.RETRY_COUNT, 0);
    MetricCollector metrics2 = MetricCollector.create(METRICS_NAME);
    metrics2.reportMetric(CoreMetric.RETRY_COUNT, 1);

    RetryDetector detector = new RetryDetector();
    detector.publish(metrics1.collect());
    Assert.assertFalse("Should not detect retries if RETRY_COUNT is zero", detector.retried());
    detector.publish(metrics2.collect());
    Assert.assertTrue(
        "Should continue detecting retries in additional metrics", detector.retried());
  }

  @Test
  public void testNoOpAfterDetection() {
    MetricCollector metrics1 = MetricCollector.create(METRICS_NAME);
    metrics1.reportMetric(CoreMetric.RETRY_COUNT, 1);
    MetricCollection metrics1Spy = Mockito.spy(metrics1.collect());
    MetricCollector metrics2 = MetricCollector.create(METRICS_NAME);
    metrics2.reportMetric(CoreMetric.RETRY_COUNT, 0);
    MetricCollection metrics2Spy = Mockito.spy(metrics2.collect());

    RetryDetector detector = new RetryDetector();
    detector.publish(metrics1Spy);
    Assert.assertTrue("Should detect retries if RETRY_COUNT is zero", detector.retried());
    detector.publish(metrics2Spy);
    Assert.assertTrue("Should remain true once a retry is detected", detector.retried());

    Mockito.verify(metrics1Spy).metricValues(Mockito.eq(CoreMetric.RETRY_COUNT));
    Mockito.verifyNoMoreInteractions(metrics1Spy, metrics2Spy);
  }
}
