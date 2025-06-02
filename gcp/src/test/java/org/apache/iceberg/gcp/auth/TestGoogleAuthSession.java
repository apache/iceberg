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
package org.apache.iceberg.gcp.auth;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;
import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.apache.iceberg.rest.auth.AuthSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TestGoogleAuthSession {

  @Mock private GoogleCredentials credentials;
  @Mock private AccessToken accessToken;

  private AuthSession session;
  private static final String TEST_TOKEN_VALUE = "test-token-12345";
  private URI testBaseUri;
  private static final String TEST_RELATIVE_PATH = "v1/some/resource";

  @BeforeEach
  public void beforeEach() throws URISyntaxException {
    this.session = new GoogleAuthSession(credentials);
    this.testBaseUri = new URI("http://localhost:8080");
  }

  @Test
  public void addsAuthHeaderOnSuccessfulTokenFetch() throws IOException {
    when(credentials.getAccessToken()).thenReturn(accessToken);
    when(accessToken.getTokenValue()).thenReturn(TEST_TOKEN_VALUE);

    HTTPRequest originalRequest =
        ImmutableHTTPRequest.builder()
            .baseUri(testBaseUri)
            .path(TEST_RELATIVE_PATH)
            .method(HTTPRequest.HTTPMethod.GET)
            .build();
    HTTPRequest authenticatedRequest = session.authenticate(originalRequest);

    verify(credentials).refreshIfExpired();
    verify(credentials).getAccessToken();

    Assertions.assertNotSame(
        originalRequest, authenticatedRequest, "A new request object should be returned");

    HTTPHeaders headers = authenticatedRequest.headers();
    Assertions.assertEquals(1, headers.entries().size());
    Assertions.assertTrue(headers.contains("Authorization"), "Should contain Authorization header");

    Set<HTTPHeaders.HTTPHeader> authHeaderEntries = headers.entries("Authorization");
    Assertions.assertEquals(1, authHeaderEntries.size(), "Should have one Authorization header");
    Assertions.assertEquals(
        "Bearer " + TEST_TOKEN_VALUE,
        authHeaderEntries.iterator().next().value(),
        "Authorization header should be set correctly");
  }

  @Test
  public void preservesExistingAuthHeader() throws IOException {
    String existingAuthHeaderValue = "Bearer existing-bearer-token";
    HTTPHeaders initialHeaders =
        HTTPHeaders.of(HTTPHeaders.HTTPHeader.of("Authorization", existingAuthHeaderValue));
    HTTPRequest originalRequest =
        ImmutableHTTPRequest.builder()
            .baseUri(testBaseUri)
            .path(TEST_RELATIVE_PATH)
            .method(HTTPRequest.HTTPMethod.GET)
            .headers(initialHeaders)
            .build();

    when(credentials.getAccessToken()).thenReturn(accessToken);
    when(accessToken.getTokenValue()).thenReturn(TEST_TOKEN_VALUE);

    HTTPRequest authenticatedRequest = session.authenticate(originalRequest);

    verify(credentials).refreshIfExpired();

    Assertions.assertSame(
        originalRequest,
        authenticatedRequest,
        "Original request object should be returned if header exists");

    HTTPHeaders resultingHeaders = authenticatedRequest.headers();
    Assertions.assertEquals(1, resultingHeaders.entries().size());
    Assertions.assertEquals(
        existingAuthHeaderValue,
        resultingHeaders.entries("Authorization").iterator().next().value(),
        "Existing Authorization header should be preserved");
  }

  @Test
  public void propagatesIOExceptionAsUncheckedOnTokenRefreshFailure() throws IOException {
    doThrow(new IOException("Failed to refresh token")).when(credentials).refreshIfExpired();

    HTTPRequest originalRequest =
        ImmutableHTTPRequest.builder()
            .baseUri(testBaseUri)
            .path(TEST_RELATIVE_PATH)
            .method(HTTPRequest.HTTPMethod.GET)
            .build();

    UncheckedIOException thrown =
        Assertions.assertThrows(
            UncheckedIOException.class,
            () -> session.authenticate(originalRequest),
            "Should throw UncheckedIOException on refresh failure");

    Assertions.assertEquals("Failed to refresh Google access token", thrown.getMessage());
    Assertions.assertInstanceOf(IOException.class, thrown.getCause());
    verify(credentials).refreshIfExpired();
  }

  @Test
  public void returnsOriginalRequestWhenAccessTokenIsNull() throws IOException {
    when(credentials.getAccessToken()).thenReturn(null);

    HTTPRequest originalRequest =
        ImmutableHTTPRequest.builder()
            .baseUri(testBaseUri)
            .path(TEST_RELATIVE_PATH)
            .method(HTTPRequest.HTTPMethod.GET)
            .build();
    HTTPRequest authenticatedRequest = session.authenticate(originalRequest);

    Assertions.assertSame(
        originalRequest, authenticatedRequest, "Request should be unchanged when token is null");
    Assertions.assertTrue(
        authenticatedRequest.headers().entries().isEmpty(), "Headers should be empty");
  }

  @Test
  public void returnsOriginalRequestWhenTokenValueIsNull() throws IOException {
    when(credentials.getAccessToken()).thenReturn(accessToken);
    when(accessToken.getTokenValue()).thenReturn(null);

    HTTPRequest originalRequest =
        ImmutableHTTPRequest.builder()
            .baseUri(testBaseUri)
            .path(TEST_RELATIVE_PATH)
            .method(HTTPRequest.HTTPMethod.GET)
            .build();
    HTTPRequest authenticatedRequest = session.authenticate(originalRequest);

    Assertions.assertSame(
        originalRequest,
        authenticatedRequest,
        "Request should be unchanged when token value is null");
    Assertions.assertTrue(
        authenticatedRequest.headers().entries().isEmpty(), "Headers should be empty");
  }

  @Test
  public void sessionCloseBehavesAsNoOp() {
    Assertions.assertDoesNotThrow(session::close);
  }
}
