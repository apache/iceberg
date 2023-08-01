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
package org.apache.iceberg.dell.ecs;

import static org.assertj.core.api.Assertions.assertThat;

import com.emc.object.s3.request.PutObjectRequest;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.iceberg.dell.mock.ecs.EcsS3MockRule;
import org.apache.iceberg.metrics.MetricsContext;
import org.junit.ClassRule;
import org.junit.Test;

public class TestEcsSeekableInputStream {

  @ClassRule public static EcsS3MockRule rule = EcsS3MockRule.create();

  @Test
  public void testSeekPosRead() throws IOException {
    String objectName = rule.randomObjectName();
    rule.client()
        .putObject(new PutObjectRequest(rule.bucket(), objectName, "0123456789".getBytes()));

    try (EcsSeekableInputStream input =
        new EcsSeekableInputStream(
            rule.client(), new EcsURI(rule.bucket(), objectName), MetricsContext.nullMetrics())) {
      input.seek(2);
      assertThat(input.read()).as("Expect 2 when seek to 2").isEqualTo('2');
    }
  }

  @Test
  public void testMultipleSeekPosRead() throws IOException {
    String objectName = rule.randomObjectName();
    rule.client()
        .putObject(new PutObjectRequest(rule.bucket(), objectName, "0123456789".getBytes()));

    try (EcsSeekableInputStream input =
        new EcsSeekableInputStream(
            rule.client(), new EcsURI(rule.bucket(), objectName), MetricsContext.nullMetrics())) {
      input.seek(999);
      input.seek(3);
      assertThat(input.read()).as("Expect 3 when seek to 3 finally").isEqualTo('3');
    }
  }

  @Test
  public void testReadOneByte() throws IOException {
    String objectName = rule.randomObjectName();
    rule.client()
        .putObject(new PutObjectRequest(rule.bucket(), objectName, "0123456789".getBytes()));

    try (EcsSeekableInputStream input =
        new EcsSeekableInputStream(
            rule.client(), new EcsURI(rule.bucket(), objectName), MetricsContext.nullMetrics())) {
      assertThat(input.read()).as("The first byte should be 0 ").isEqualTo('0');
    }
  }

  @Test
  public void testReadBytes() throws IOException {
    String objectName = rule.randomObjectName();
    rule.client()
        .putObject(new PutObjectRequest(rule.bucket(), objectName, "0123456789".getBytes()));

    try (EcsSeekableInputStream input =
        new EcsSeekableInputStream(
            rule.client(), new EcsURI(rule.bucket(), objectName), MetricsContext.nullMetrics())) {
      byte[] buffer = new byte[3];
      assertThat(input.read(buffer)).as("The first read should be 3 bytes").isEqualTo(3);
      assertThat(new String(buffer, StandardCharsets.UTF_8))
          .as("The first 3 bytes should be 012")
          .isEqualTo("012");
    }
  }
}
