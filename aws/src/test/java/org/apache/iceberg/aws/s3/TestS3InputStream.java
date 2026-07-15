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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.ConnectionOptions;
import org.mockserver.model.HttpResponse;
import org.mockserver.verify.VerificationTimes;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

@ExtendWith(MockitoExtension.class)
public final class TestS3InputStream {

  @Mock private S3Client s3Client;
  @Mock private InputStream inputStream;

  private S3InputStream s3InputStream;

  @BeforeEach
  void before() {
    lenient()
        .when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
        .thenReturn(inputStream);
    s3InputStream = new S3InputStream(s3Client, mock());
  }

  @Test
  void testReadFullyClosesTheStream() throws IOException {
    s3InputStream.readFully(0, new byte[0]);

    verify(inputStream).close();
  }

  @Test
  void testReadTailClosesTheStream() throws IOException {
    s3InputStream.readTail(new byte[0], 0, 0);

    verify(inputStream).close();
  }

  @Test
  void testReadRetriesOnPrematureConnectionCloseWithApacheHttpClient() throws IOException {
    testReadRetriesOnPrematureConnectionClose(ApacheHttpClient.create());
  }

  @Test
  void testReadRetriesOnPrematureConnectionCloseWithUrlConnectionHttpClient() throws IOException {
    testReadRetriesOnPrematureConnectionClose(UrlConnectionHttpClient.create());
  }

  void testReadRetriesOnPrematureConnectionClose(SdkHttpClient httpClient) throws IOException {
    int fileSize = 1024;
    int blockSize = fileSize / 2;
    byte[] block1 = new byte[blockSize];
    Arrays.fill(block1, (byte) 1);
    byte[] block2 = new byte[blockSize];
    Arrays.fill(block2, (byte) 2);
    byte[] response1 = Arrays.copyOf(block1, blockSize + blockSize / 2);
    System.arraycopy(block2, 0, response1, blockSize, blockSize / 2);
    byte[] response2 = Arrays.copyOfRange(block2, blockSize / 2, blockSize);

    ClientAndServer mockServer = startClientAndServer();
    try {
      // First unbounded request: send block1 and the first half of block2,
      // then close the socket to simulate stalled connection
      mockServer
          .when(request().withHeader("Range", "bytes=0-"))
          .respond(
              HttpResponse.response()
                  .withStatusCode(206)
                  .withHeader("Content-Range", "bytes 0-%s/%s".formatted(fileSize - 1, fileSize))
                  .withBody(response1)
                  .withConnectionOptions(
                      ConnectionOptions.connectionOptions()
                          .withContentLengthHeaderOverride(fileSize)
                          .withCloseSocket(true)));
      // Retry from the second half of block2: complete response
      mockServer
          .when(request().withHeader("Range", "bytes=%s-".formatted(response1.length)))
          .respond(
              HttpResponse.response()
                  .withStatusCode(206)
                  .withHeader(
                      "Content-Range",
                      "bytes %s-%s/%s".formatted(response1.length, fileSize - 1, fileSize))
                  .withBody(response2));

      try (S3Client s3Client =
              S3Client.builder()
                  .endpointOverride(URI.create("http://127.0.0.1:" + mockServer.getLocalPort()))
                  .serviceConfiguration(
                      S3Configuration.builder().pathStyleAccessEnabled(true).build())
                  .credentialsProvider(AnonymousCredentialsProvider.create())
                  .region(Region.US_EAST_1)
                  .httpClient(httpClient)
                  .build();
          S3InputStream stream =
              new S3InputStream(s3Client, new S3URI("s3://test-bucket/test-key"))) {
        // Read block1 from response1
        byte[] buf1 = new byte[blockSize];
        assertThat(stream.readNBytes(buf1, 0, blockSize)).isEqualTo(blockSize);
        assertThat(buf1).isEqualTo(block1);
        // Read the first half of block2 from response1,
        // retry on "Premature end ..." (apache) or -1 (urlconnection),
        // send a new range request from the current position
        // and read the second half of block2 from response2
        byte[] buf2 = new byte[blockSize];
        assertThat(stream.readNBytes(buf2, 0, blockSize)).isEqualTo(blockSize);
        assertThat(buf2).isEqualTo(block2);
      }
      // Confirm the retry opened a second request to the server.
      mockServer.verify(request(), VerificationTimes.exactly(2));
    } finally {
      mockServer.stop();
    }
  }
}
