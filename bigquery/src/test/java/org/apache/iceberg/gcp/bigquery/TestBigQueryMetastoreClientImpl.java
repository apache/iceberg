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
package org.apache.iceberg.gcp.bigquery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.ExternalCatalogTableOptions;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.http.HttpTransportOptions;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class TestBigQueryMetastoreClientImpl {

  private static final String DATASET_OK_JSON =
      "{\"kind\":\"bigquery#dataset\","
          + "\"datasetReference\":{\"projectId\":\"test-project\",\"datasetId\":\"test-dataset\"}}";

  private static final String TABLE_OK_JSON =
      "{\"kind\":\"bigquery#table\","
          + "\"tableReference\":{\"projectId\":\"test-project\","
          + "\"datasetId\":\"test-dataset\",\"tableId\":\"test-table\"}}";

  private static final String CONFLICT_JSON =
      "{\"error\":{\"code\":409,\"message\":\"Already Exists\","
          + "\"status\":\"ALREADY_EXISTS\","
          + "\"errors\":[{\"message\":\"Already Exists\",\"domain\":\"global\","
          + "\"reason\":\"duplicate\"}]}}";

  // The HttpRequest class defaults both connect and read timeout to 20 seconds.
  private static final int HTTP_REQUEST_DEFAULT_MS = 20_000;

  private static GoogleCredentials credentials(String token) {
    return GoogleCredentials.create(
        new AccessToken(token, Date.from(Instant.now().plusSeconds(3600))));
  }

  private static BigQueryOptions optionsWithDefaults() {
    return BigQueryOptions.newBuilder()
        .setProjectId("test-project")
        .setCredentials(credentials("default-token"))
        .build();
  }

  private static BigQueryOptions optionsWithTransport(HttpTransportOptions transportOptions) {
    return BigQueryOptions.newBuilder()
        .setProjectId("test-project")
        .setCredentials(credentials("default-token"))
        .setTransportOptions(transportOptions)
        .build();
  }

  private static Dataset testDataset() {
    return new Dataset()
        .setDatasetReference(
            new DatasetReference().setProjectId("test-project").setDatasetId("test-dataset"));
  }

  private static Table testTable() {
    return new Table()
        .setTableReference(
            new TableReference()
                .setProjectId("test-project")
                .setDatasetId("test-dataset")
                .setTableId("test-table"))
        .setExternalCatalogTableOptions(
            new ExternalCatalogTableOptions()
                .setParameters(
                    ImmutableMap.of(
                        BaseMetastoreTableOperations.METADATA_LOCATION_PROP,
                        "gs://bucket/path/meta",
                        BaseMetastoreTableOperations.TABLE_TYPE_PROP,
                        BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE)));
  }

  /**
   * Fake transport that captures the connect/read timeouts and Authorization header from the first
   * HTTP request. LowLevelHttpRequest.setTimeout(connect, read) is the single entry point through
   * which HttpRequest propagates both timeout values to the low-level transport.
   */
  private static class CapturingTransport extends HttpTransport {
    // Integer.MIN_VALUE means setTimeout was never called.
    volatile int capturedConnectTimeout = Integer.MIN_VALUE;
    volatile int capturedReadTimeout = Integer.MIN_VALUE;
    volatile String capturedAuthHeader;

    private final int responseStatus;
    private final String responseBody;

    CapturingTransport(int responseStatus, String responseBody) {
      this.responseStatus = responseStatus;
      this.responseBody = responseBody;
    }

    @Override
    protected LowLevelHttpRequest buildRequest(String method, String url) {
      return new LowLevelHttpRequest() {
        private int connectMs = Integer.MIN_VALUE;
        private int readMs = Integer.MIN_VALUE;
        private String authHeader;

        @Override
        public void setTimeout(int connectTimeout, int readTimeout) {
          connectMs = connectTimeout;
          readMs = readTimeout;
        }

        @Override
        public void addHeader(String name, String value) {
          if ("Authorization".equalsIgnoreCase(name)) {
            authHeader = value;
          }
        }

        @Override
        public LowLevelHttpResponse execute() throws IOException {
          capturedConnectTimeout = connectMs;
          capturedReadTimeout = readMs;
          capturedAuthHeader = authHeader;
          return new FakeHttpResponse(responseStatus, responseBody);
        }
      };
    }
  }

  private static class FakeHttpResponse extends LowLevelHttpResponse {
    private final int statusCode;
    private final byte[] body;

    FakeHttpResponse(int statusCode, String body) {
      this.statusCode = statusCode;
      this.body = body.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public InputStream getContent() throws IOException {
      return new ByteArrayInputStream(body);
    }

    @Override
    public String getContentEncoding() throws IOException {
      return null;
    }

    @Override
    public long getContentLength() throws IOException {
      return body.length;
    }

    @Override
    public String getContentType() throws IOException {
      return "application/json";
    }

    @Override
    public String getStatusLine() throws IOException {
      return "HTTP/1.1 " + statusCode;
    }

    @Override
    public int getStatusCode() throws IOException {
      return statusCode;
    }

    @Override
    public String getReasonPhrase() throws IOException {
      return statusCode == 200 ? "OK" : statusCode == 409 ? "Conflict" : "Unknown";
    }

    @Override
    public int getHeaderCount() throws IOException {
      return 0;
    }

    @Override
    public String getHeaderName(int index) throws IOException {
      return null;
    }

    @Override
    public String getHeaderValue(int index) throws IOException {
      return null;
    }
  }

  @Test
  void configuredTimeoutsReachTableRequest() throws Exception {
    CapturingTransport transport = new CapturingTransport(200, TABLE_OK_JSON);
    HttpTransportOptions transportOptions =
        HttpTransportOptions.newBuilder().setConnectTimeout(60_000).setReadTimeout(180_000).build();

    BigQueryMetastoreClientImpl client =
        new BigQueryMetastoreClientImpl(optionsWithTransport(transportOptions), transport);
    client.create(testTable());

    assertThat(transport.capturedConnectTimeout).isEqualTo(60_000);
    assertThat(transport.capturedReadTimeout).isEqualTo(180_000);
  }

  @Test
  void configuredTimeoutsReachDatasetRequest() throws Exception {
    CapturingTransport transport = new CapturingTransport(200, DATASET_OK_JSON);
    HttpTransportOptions transportOptions =
        HttpTransportOptions.newBuilder().setConnectTimeout(60_000).setReadTimeout(180_000).build();

    BigQueryMetastoreClientImpl client =
        new BigQueryMetastoreClientImpl(optionsWithTransport(transportOptions), transport);
    client.create(testDataset());

    assertThat(transport.capturedConnectTimeout).isEqualTo(60_000);
    assertThat(transport.capturedReadTimeout).isEqualTo(180_000);
  }

  @Test
  void sdkDefaultReadTimeoutHonored() throws Exception {
    // Plain BigQueryOptions supplies a 60-second read timeout; the fix must honor it.
    CapturingTransport transport = new CapturingTransport(200, DATASET_OK_JSON);

    BigQueryMetastoreClientImpl client =
        new BigQueryMetastoreClientImpl(optionsWithDefaults(), transport);
    client.create(testDataset());

    assertThat(transport.capturedReadTimeout).isEqualTo(60_000);
  }

  @Test
  void explicitlyUnsetTransportOptionsPreserveHttpDefaults() throws Exception {
    // HttpTransportOptions.newBuilder().build() leaves both timeouts at -1.
    // The fix must leave HttpRequest defaults intact rather than overriding them.
    HttpTransportOptions emptyOptions = HttpTransportOptions.newBuilder().build();
    CapturingTransport transport = new CapturingTransport(200, DATASET_OK_JSON);

    BigQueryMetastoreClientImpl client =
        new BigQueryMetastoreClientImpl(optionsWithTransport(emptyOptions), transport);
    client.create(testDataset());

    assertThat(transport.capturedConnectTimeout).isEqualTo(HTTP_REQUEST_DEFAULT_MS);
    assertThat(transport.capturedReadTimeout).isEqualTo(HTTP_REQUEST_DEFAULT_MS);
  }

  @Test
  void connectOnlyOverrideApplied() throws Exception {
    HttpTransportOptions transportOptions =
        HttpTransportOptions.newBuilder().setConnectTimeout(45_000).build();
    CapturingTransport transport = new CapturingTransport(200, DATASET_OK_JSON);

    BigQueryMetastoreClientImpl client =
        new BigQueryMetastoreClientImpl(optionsWithTransport(transportOptions), transport);
    client.create(testDataset());

    assertThat(transport.capturedConnectTimeout).isEqualTo(45_000);
    assertThat(transport.capturedReadTimeout).isEqualTo(HTTP_REQUEST_DEFAULT_MS);
  }

  @Test
  void readOnlyOverrideApplied() throws Exception {
    HttpTransportOptions transportOptions =
        HttpTransportOptions.newBuilder().setReadTimeout(120_000).build();
    CapturingTransport transport = new CapturingTransport(200, DATASET_OK_JSON);

    BigQueryMetastoreClientImpl client =
        new BigQueryMetastoreClientImpl(optionsWithTransport(transportOptions), transport);
    client.create(testDataset());

    assertThat(transport.capturedConnectTimeout).isEqualTo(HTTP_REQUEST_DEFAULT_MS);
    assertThat(transport.capturedReadTimeout).isEqualTo(120_000);
  }

  @Test
  void zeroTimeoutsForwarded() throws Exception {
    // Zero means "no timeout" per the SDK contract and must be forwarded, not filtered out.
    HttpTransportOptions transportOptions =
        HttpTransportOptions.newBuilder().setConnectTimeout(0).setReadTimeout(0).build();
    CapturingTransport transport = new CapturingTransport(200, DATASET_OK_JSON);

    BigQueryMetastoreClientImpl client =
        new BigQueryMetastoreClientImpl(optionsWithTransport(transportOptions), transport);
    client.create(testDataset());

    assertThat(transport.capturedConnectTimeout).isEqualTo(0);
    assertThat(transport.capturedReadTimeout).isEqualTo(0);
  }

  @Test
  void authorizationHeaderPreserved() throws Exception {
    // The credential initializer must still add the Authorization header after the fix.
    BigQueryOptions options =
        BigQueryOptions.newBuilder()
            .setProjectId("test-project")
            .setCredentials(credentials("my-auth-token"))
            .build();
    CapturingTransport transport = new CapturingTransport(200, DATASET_OK_JSON);

    BigQueryMetastoreClientImpl client = new BigQueryMetastoreClientImpl(options, transport);
    client.create(testDataset());

    assertThat(transport.capturedAuthHeader).isEqualTo("Bearer my-auth-token");
  }

  @Test
  void conflictResponseMappedToAlreadyExistsException() throws Exception {
    CapturingTransport transport = new CapturingTransport(409, CONFLICT_JSON);

    BigQueryMetastoreClientImpl client =
        new BigQueryMetastoreClientImpl(optionsWithDefaults(), transport);

    assertThatThrownBy(() -> client.create(testDataset()))
        .isInstanceOf(AlreadyExistsException.class);
  }

  @Test
  void noCrossClientTimeoutLeakage() throws Exception {
    HttpTransportOptions opts1 =
        HttpTransportOptions.newBuilder().setConnectTimeout(30_000).setReadTimeout(90_000).build();
    HttpTransportOptions opts2 =
        HttpTransportOptions.newBuilder().setConnectTimeout(60_000).setReadTimeout(180_000).build();

    CapturingTransport transport1 = new CapturingTransport(200, DATASET_OK_JSON);
    CapturingTransport transport2 = new CapturingTransport(200, DATASET_OK_JSON);

    BigQueryMetastoreClientImpl client1 =
        new BigQueryMetastoreClientImpl(optionsWithTransport(opts1), transport1);
    BigQueryMetastoreClientImpl client2 =
        new BigQueryMetastoreClientImpl(optionsWithTransport(opts2), transport2);

    client1.create(testDataset());
    client2.create(testDataset());

    assertThat(transport1.capturedConnectTimeout).isEqualTo(30_000);
    assertThat(transport1.capturedReadTimeout).isEqualTo(90_000);
    assertThat(transport2.capturedConnectTimeout).isEqualTo(60_000);
    assertThat(transport2.capturedReadTimeout).isEqualTo(180_000);
  }
}
