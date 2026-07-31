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
package org.apache.iceberg.aws.s3;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link S3FileIO#newInputFile} dispatches HTTP(S) locations to the direct HTTP(S)
 * read path and everything else to the native S3 client, without performing any I/O.
 */
public class TestS3FileIOHttpUrlDispatch {

  private S3FileIO fileIO;

  @AfterEach
  void after() {
    if (fileIO != null) {
      fileIO.close();
    }
  }

  @Test
  void httpsLocationUsesHttpShortCircuit() {
    fileIO =
        new S3FileIO(
            () -> {
              throw new AssertionError("Native S3 client should not be used for an https:// path");
            });
    fileIO.initialize(ImmutableMap.of());

    String url = "https://bucket.s3.amazonaws.com/key?X-Amz-Signature=abc";
    InputFile inputFile = fileIO.newInputFile(url);

    assertThat(inputFile.location()).isEqualTo(url);
    assertThat(inputFile.getClass().getName())
        .isEqualTo("org.apache.iceberg.io.http.HTTPInputFile");

    InputFile withLength = fileIO.newInputFile(url, 100L);
    assertThat(withLength.getClass().getName())
        .isEqualTo("org.apache.iceberg.io.http.HTTPInputFile");
  }

  @Test
  void httpLocationUsesHttpShortCircuit() {
    fileIO =
        new S3FileIO(
            () -> {
              throw new AssertionError("Native S3 client should not be used for an http:// path");
            });
    fileIO.initialize(ImmutableMap.of());

    InputFile inputFile = fileIO.newInputFile("http://example.com/data.parquet");
    assertThat(inputFile.getClass().getName())
        .isEqualTo("org.apache.iceberg.io.http.HTTPInputFile");
  }

  @Test
  void s3LocationUsesNativeClient() {
    fileIO = new S3FileIO(() -> null);
    fileIO.initialize(ImmutableMap.of());

    InputFile inputFile = fileIO.newInputFile("s3://bucket/key");
    assertThat(inputFile).isInstanceOf(S3InputFile.class);

    InputFile withLength = fileIO.newInputFile("s3://bucket/key", 100L);
    assertThat(withLength).isInstanceOf(S3InputFile.class);
  }
}
