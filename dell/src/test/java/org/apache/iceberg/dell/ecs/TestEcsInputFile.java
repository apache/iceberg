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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.emc.object.s3.request.PutObjectRequest;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.iceberg.dell.DellProperties;
import org.apache.iceberg.dell.mock.ecs.EcsS3MockRule;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class TestEcsInputFile {

  @RegisterExtension public static EcsS3MockRule rule = EcsS3MockRule.create();

  @Test
  public void testKnownLengthAvoidsHeadRequest() {
    String objectName = rule.randomObjectName();
    EcsInputFile inputFile =
        EcsInputFile.fromLocation(
            new EcsURI(rule.bucket(), objectName).toString(),
            10L,
            rule.client(),
            new DellProperties(),
            MetricsContext.nullMetrics());

    // The object is never created, so any getObjectMetadata() call would fail. Getting the
    // length back without error proves it came from the cached value, not a HEAD request.
    assertThat(inputFile.getLength()).isEqualTo(10L);
  }

  @Test
  public void testNegativeLengthIsRejected() {
    assertThatThrownBy(
            () ->
                EcsInputFile.fromLocation(
                    new EcsURI(rule.bucket(), rule.randomObjectName()).toString(),
                    -1L,
                    rule.client(),
                    new DellProperties(),
                    MetricsContext.nullMetrics()))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid file length");
  }

  @Test
  public void testAbsentFile() {
    String objectName = rule.randomObjectName();
    EcsInputFile inputFile =
        EcsInputFile.fromLocation(new EcsURI(rule.bucket(), objectName).toString(), rule.client());
    assertThat(inputFile.exists()).as("File is absent").isFalse();
  }

  @Test
  public void testFileRead() throws IOException {
    String objectName = rule.randomObjectName();
    EcsInputFile inputFile =
        EcsInputFile.fromLocation(new EcsURI(rule.bucket(), objectName).toString(), rule.client());

    rule.client()
        .putObject(new PutObjectRequest(rule.bucket(), objectName, "0123456789".getBytes()));

    assertThat(inputFile.exists()).as("File should exists").isTrue();
    assertThat(inputFile.getLength()).as("File length should be 10").isEqualTo(10);
    try (InputStream inputStream = inputFile.newStream()) {
      assertThat(new String(ByteStreams.toByteArray(inputStream), StandardCharsets.UTF_8))
          .as("The file content should be 0123456789")
          .isEqualTo("0123456789");
    }
  }
}
