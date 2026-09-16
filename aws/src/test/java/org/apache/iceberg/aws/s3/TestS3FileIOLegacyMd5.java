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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import java.security.MessageDigest;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;

class TestS3FileIOLegacyMd5 {
  private static final HttpRequest DELETE_REQUEST =
      request().withMethod("POST").withPath("/bucket").withQueryStringParameter("delete");

  private ClientAndServer server;

  @BeforeEach
  void startServer() {
    server = startClientAndServer(0);
    // Legacy S3 implementations require Content-MD5 for multi-object deletes.
    server
        .when(DELETE_REQUEST)
        .respond(
            request -> {
              String checksum =
                  Base64.getEncoder()
                      .encodeToString(
                          MessageDigest.getInstance("MD5").digest(request.getBodyAsRawBytes()));
              if (!checksum.equals(request.getFirstHeader("Content-MD5"))) {
                return response()
                    .withStatusCode(400)
                    .withBody(
                        "<Error><Code>InvalidRequest</Code>"
                            + "<Message>Missing or invalid Content-MD5</Message></Error>");
              }

              return response()
                  .withStatusCode(200)
                  .withBody(
                      "<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                          + "<Deleted><Key>file</Key></Deleted></DeleteResult>");
            });
  }

  @AfterEach
  void stopServer() {
    server.stop();
  }

  @Test
  void batchDeleteWithLegacyMd5() {
    try (S3FileIO fileIO = fileIO(true)) {
      fileIO.deleteFiles(List.of("s3://bucket/file"));
    }

    assertThat(server.retrieveRecordedRequests(DELETE_REQUEST)).hasSize(1);
  }

  @Test
  void batchDeleteWithoutLegacyMd5IsRejected() {
    try (S3FileIO fileIO = fileIO(false)) {
      assertThatThrownBy(() -> fileIO.deleteFiles(List.of("s3://bucket/file")))
          .isInstanceOf(BulkDeletionFailureException.class)
          .hasMessage("Failed to delete 1 files");
    }

    assertThat(server.retrieveRecordedRequests(DELETE_REQUEST))
        .singleElement()
        .satisfies(request -> assertThat(request.getFirstHeader("Content-MD5")).isEmpty());
  }

  private S3FileIO fileIO(boolean legacyMd5Enabled) {
    S3FileIO fileIO = new S3FileIO();
    fileIO.initialize(
        Map.of(
            AwsClientProperties.CLIENT_REGION, "us-east-1",
            AwsClientProperties.LEGACY_MD5_PLUGIN_ENABLED, Boolean.toString(legacyMd5Enabled),
            S3FileIOProperties.ENDPOINT, "http://localhost:" + server.getLocalPort(),
            S3FileIOProperties.PATH_STYLE_ACCESS, "true",
            S3FileIOProperties.ACCESS_KEY_ID, "accessKey",
            S3FileIOProperties.SECRET_ACCESS_KEY, "secretKey"));
    return fileIO;
  }
}
