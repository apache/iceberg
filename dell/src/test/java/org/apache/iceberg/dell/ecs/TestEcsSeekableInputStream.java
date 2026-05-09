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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class TestEcsSeekableInputStream {

  @RegisterExtension public static EcsS3MockRule rule = EcsS3MockRule.create();

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

  @Test
  public void testReadSingleByteAtEof() throws IOException {
    // Regression test for #16062 — single-byte read() at EOF must return -1
    // and must not advance pos. Without the fix, EcsSeekableInputStream
    // forwarded the underlying -1 to the caller while still incrementing
    // pos (and the readBytes / readOperations metrics) by 1.
    String objectName = rule.randomObjectName();
    byte[] data = "0123".getBytes(StandardCharsets.UTF_8);
    rule.client().putObject(new PutObjectRequest(rule.bucket(), objectName, data));

    try (EcsSeekableInputStream input =
        new EcsSeekableInputStream(
            rule.client(), new EcsURI(rule.bucket(), objectName), MetricsContext.nullMetrics())) {
      // Drain the stream byte-by-byte so the next read sees EOF.
      for (int i = 0; i < data.length; i++) {
        assertThat(input.read()).isEqualTo(data[i] & 0xFF);
      }
      assertThat(input.getPos()).isEqualTo(data.length);

      // First EOF read returns -1 and pos stays at the file size.
      assertThat(input.read()).as("read() at EOF must return -1").isEqualTo(-1);
      assertThat(input.getPos())
          .as("pos must not advance past EOF on a -1 read")
          .isEqualTo(data.length);

      // Repeated EOF reads stay idempotent (no drift).
      assertThat(input.read()).isEqualTo(-1);
      assertThat(input.getPos()).isEqualTo(data.length);
    }
  }

  @Test
  public void testReadBufferAtEof() throws IOException {
    // Companion to testReadSingleByteAtEof: read(byte[], off, len) at EOF
    // must return -1 without advancing pos. Without the fix,
    // EcsSeekableInputStream still added -1 to pos and to the metric
    // counters on every EOF call.
    String objectName = rule.randomObjectName();
    byte[] data = "0123".getBytes(StandardCharsets.UTF_8);
    rule.client().putObject(new PutObjectRequest(rule.bucket(), objectName, data));

    try (EcsSeekableInputStream input =
        new EcsSeekableInputStream(
            rule.client(), new EcsURI(rule.bucket(), objectName), MetricsContext.nullMetrics())) {
      byte[] buffer = new byte[data.length];
      assertThat(input.read(buffer, 0, buffer.length)).isEqualTo(data.length);
      assertThat(input.getPos()).isEqualTo(data.length);

      assertThat(input.read(buffer, 0, buffer.length))
          .as("read(byte[], off, len) at EOF must return -1")
          .isEqualTo(-1);
      assertThat(input.getPos())
          .as("pos must not advance past EOF on a -1 read")
          .isEqualTo(data.length);
    }
  }
}
