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

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.aws.HttpClientProperties;
import org.apache.iceberg.metrics.CommitMetrics;
import org.apache.iceberg.metrics.CommitMetricsResult;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.DefaultMetricsContext;
import org.apache.iceberg.metrics.ImmutableCommitReport;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.SerializableSupplier;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SqsMetricsReporterTest {

  public SerializableSupplier<SqsAsyncClient> sqs;

  private SqsMetricsReporter sqsMetricsReporter;

  private CommitReport commitReport;

  @BeforeAll
  public void before() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("metrics-reporter.sqs.queue-url", "test-sqs-queue");
    properties.put(
        HttpClientProperties.CLIENT_TYPE, HttpClientProperties.HTTP_CLIENT_TYPE_NETTYNIO);
    sqs = () -> mock(SqsAsyncClient.class);
    sqsMetricsReporter = new SqsMetricsReporter(sqs, "test-queue-url");
    commitReport =
        ImmutableCommitReport.builder()
            .tableName("tableName")
            .snapshotId(123L)
            .operation(DataOperations.APPEND)
            .sequenceNumber(123L)
            .commitMetrics(
                CommitMetricsResult.from(
                    CommitMetrics.of(new DefaultMetricsContext()), Maps.newHashMap()))
            .build();
  }

  @Test
  public void report() {
    sqsMetricsReporter.report(commitReport);
    when(sqs.get().sendMessage(isA(SendMessageRequest.class)))
        .thenReturn(
            CompletableFuture.completedFuture(
                SendMessageResponse.builder().messageId("test-id").build()));
  }
}
