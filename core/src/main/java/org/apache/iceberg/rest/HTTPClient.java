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
package org.apache.iceberg.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.hc.client5.http.auth.CredentialsProvider;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpRequestInterceptor;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.impl.EnglishReasonPhraseCatalog;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.io.CloseMode;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An HttpClient for usage with the REST catalog. */
public class HTTPClient extends BaseHTTPClient {

  private static final Logger LOG = LoggerFactory.getLogger(HTTPClient.class);
  private static final String SIGV4_ENABLED = "rest.sigv4-enabled";
  private static final String SIGV4_REQUEST_INTERCEPTOR_IMPL =
      "org.apache.iceberg.aws.RESTSigV4Signer";
  @VisibleForTesting static final String CLIENT_VERSION_HEADER = "X-Client-Version";

  @VisibleForTesting
  static final String CLIENT_GIT_COMMIT_SHORT_HEADER = "X-Client-Git-Commit-Short";

  private static final String REST_MAX_RETRIES = "rest.client.max-retries";
  static final String REST_MAX_CONNECTIONS = "rest.client.max-connections";
  static final int REST_MAX_CONNECTIONS_DEFAULT = 100;
  static final String REST_MAX_CONNECTIONS_PER_ROUTE = "rest.client.connections-per-route";
  static final int REST_MAX_CONNECTIONS_PER_ROUTE_DEFAULT = 100;

  @VisibleForTesting
  static final String REST_CONNECTION_TIMEOUT_MS = "rest.client.connection-timeout-ms";

  @VisibleForTesting static final String REST_SOCKET_TIMEOUT_MS = "rest.client.socket-timeout-ms";

  private final URI baseUri;
  private final CloseableHttpClient httpClient;
  private final Map<String, String> baseHeaders;
  private final ObjectMapper mapper;
  private final AuthSession authSession;

  private HTTPClient(
      URI baseUri,
      HttpHost proxy,
      CredentialsProvider proxyCredsProvider,
      Map<String, String> baseHeaders,
      ObjectMapper objectMapper,
      HttpRequestInterceptor requestInterceptor,
      Map<String, String> properties,
      HttpClientConnectionManager connectionManager,
      AuthSession session) {
    this.baseUri = baseUri;
    this.baseHeaders = baseHeaders;
    this.mapper = objectMapper;
    this.authSession = session;

    HttpClientBuilder clientBuilder = HttpClients.custom();

    clientBuilder.setConnectionManager(connectionManager);

    if (requestInterceptor != null) {
      clientBuilder.addRequestInterceptorLast(requestInterceptor);
    }

    int maxRetries = PropertyUtil.propertyAsInt(properties, REST_MAX_RETRIES, 5);
    clientBuilder.setRetryStrategy(new ExponentialHttpRequestRetryStrategy(maxRetries));

    if (proxy != null) {
      if (proxyCredsProvider != null) {
        clientBuilder.setDefaultCredentialsProvider(proxyCredsProvider);
      }

      clientBuilder.setProxy(proxy);
    }

    this.httpClient = clientBuilder.build();
  }

  /**
   * Constructor for creating a child HTTPClient associated with an AuthSession. The returned child
   * shares the same base uri, mapper, and HTTP client as the parent, thus not requiring any
   * additional resource allocation.
   */
  private HTTPClient(HTTPClient parent, AuthSession authSession) {
    this.baseUri = parent.baseUri;
    this.httpClient = parent.httpClient;
    this.mapper = parent.mapper;
    this.baseHeaders = parent.baseHeaders;
    this.authSession = authSession;
  }

  @Override
  public HTTPClient withAuthSession(AuthSession session) {
    Preconditions.checkNotNull(session, "Invalid auth session: null");
    return new HTTPClient(this, session);
  }

  private static String extractResponseBodyAsString(CloseableHttpResponse response) {
    try {
      if (response.getEntity() == null) {
        return null;
      }

      // EntityUtils.toString returns null when HttpEntity.getContent returns null.
      return EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
    } catch (IOException | ParseException e) {
      throw new RESTException(e, "Failed to convert HTTP response body to string");
    }
  }

  // Per the spec, the only currently defined / used "success" responses are 200 and 202.
  private static boolean isSuccessful(CloseableHttpResponse response) {
    int code = response.getCode();
    return code == HttpStatus.SC_OK
        || code == HttpStatus.SC_ACCEPTED
        || code == HttpStatus.SC_NO_CONTENT;
  }

  private static ErrorResponse buildDefaultErrorResponse(CloseableHttpResponse response) {
    String responseReason = response.getReasonPhrase();
    String message =
        responseReason != null && !responseReason.isEmpty()
            ? responseReason
            : EnglishReasonPhraseCatalog.INSTANCE.getReason(response.getCode(), null /* ignored */);
    String type = "RESTException";
    return ErrorResponse.builder()
        .responseCode(response.getCode())
        .withMessage(message)
        .withType(type)
        .build();
  }

  // Process a failed response through the provided errorHandler, and throw a RESTException if the
  // provided error handler doesn't already throw.
  private static void throwFailure(
      CloseableHttpResponse response, String responseBody, Consumer<ErrorResponse> errorHandler) {
    ErrorResponse errorResponse = null;

    if (responseBody != null) {
      try {
        if (errorHandler instanceof ErrorHandler) {
          errorResponse =
              ((ErrorHandler) errorHandler).parseResponse(response.getCode(), responseBody);
        } else {
          LOG.warn(
              "Unknown error handler {}, response body won't be parsed",
              errorHandler.getClass().getName());
          errorResponse =
              ErrorResponse.builder()
                  .responseCode(response.getCode())
                  .withMessage(responseBody)
                  .build();
        }

      } catch (UncheckedIOException | IllegalArgumentException e) {
        // It's possible to receive a non-successful response that isn't a properly defined
        // ErrorResponse
        // without any bugs in the server implementation. So we ignore this exception and build an
        // error
        // response for the user.
        //
        // For example, the connection could time out before every reaching the server, in which
        // case we'll
        // likely get a 5xx with the load balancers default 5xx response.
        LOG.error("Failed to parse an error response. Will create one instead.", e);
      }
    }

    if (errorResponse == null) {
      errorResponse = buildDefaultErrorResponse(response);
    }

    errorHandler.accept(errorResponse);

    // Throw an exception in case the provided error handler does not throw.
    throw new RESTException("Unhandled error: %s", errorResponse);
  }

  @Override
  protected HTTPRequest buildRequest(
      HTTPMethod method,
      String path,
      Map<String, String> queryParams,
      Map<String, String> headers,
      Object body) {

    ImmutableHTTPRequest.Builder builder =
        ImmutableHTTPRequest.builder()
            .baseUri(baseUri)
            .mapper(mapper)
            .method(method)
            .path(path)
            .body(body)
            .queryParameters(queryParams == null ? Map.of() : queryParams);

    Map<String, String> allHeaders = Maps.newLinkedHashMap();
    if (headers != null) {
      allHeaders.putAll(headers);
    }

    allHeaders.putIfAbsent(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType());

    // Many systems require that content type is set regardless and will fail,
    // even on an empty bodied request.
    // Encode maps as form data (application/x-www-form-urlencoded),
    // and other requests are assumed to contain JSON bodies (application/json).
    ContentType mimeType =
        body instanceof Map
            ? ContentType.APPLICATION_FORM_URLENCODED
            : ContentType.APPLICATION_JSON;
    allHeaders.putIfAbsent(HttpHeaders.CONTENT_TYPE, mimeType.getMimeType());

    // Apply base headers now to mimic the behavior of
    // org.apache.hc.client5.http.protocol.RequestDefaultHeaders
    // We want these headers applied *before* the AuthSession authenticates the request.
    if (baseHeaders != null) {
      baseHeaders.forEach(allHeaders::putIfAbsent);
    }

    Preconditions.checkState(authSession != null, "Invalid auth session: null");
    return authSession.authenticate(builder.headers(HTTPHeaders.of(allHeaders)).build());
  }

  @Override
  protected <T extends RESTResponse> T execute(
      HTTPRequest req,
      Class<T> responseType,
      Consumer<ErrorResponse> errorHandler,
      Consumer<Map<String, String>> responseHeaders) {
    HttpUriRequestBase request = new HttpUriRequestBase(req.method().name(), req.requestUri());

    req.headers().entries().forEach(e -> request.addHeader(e.name(), e.value()));

    String encodedBody = req.encodedBody();
    if (encodedBody != null) {
      request.setEntity(new StringEntity(encodedBody));
    }

    try (CloseableHttpResponse response = httpClient.execute(request)) {
      Map<String, String> respHeaders = Maps.newHashMap();
      for (Header header : response.getHeaders()) {
        respHeaders.put(header.getName(), header.getValue());
      }

      responseHeaders.accept(respHeaders);

      // Skip parsing the response stream for any successful request not expecting a response body
      if (response.getCode() == HttpStatus.SC_NO_CONTENT
          || (responseType == null && isSuccessful(response))) {
        return null;
      }

      String responseBody = extractResponseBodyAsString(response);

      if (!isSuccessful(response)) {
        // The provided error handler is expected to throw, but a RESTException is thrown if not.
        throwFailure(response, responseBody, errorHandler);
      }

      if (responseBody == null) {
        throw new RESTException(
            "Invalid (null) response body for request (expected %s): method=%s, path=%s, status=%d",
            responseType.getSimpleName(), req.method(), req.path(), response.getCode());
      }

      try {
        return mapper.readValue(responseBody, responseType);
      } catch (JsonProcessingException e) {
        throw new RESTException(
            e,
            "Received a success response code of %d, but failed to parse response body into %s",
            response.getCode(),
            responseType.getSimpleName());
      }
    } catch (IOException e) {
      throw new RESTException(e, "Error occurred while processing %s request", req.method());
    }
  }

  @Override
  public void close() throws IOException {
    try {
      if (authSession != null) {
        authSession.close();
      }
    } finally {
      httpClient.close(CloseMode.GRACEFUL);
    }
  }

  @VisibleForTesting
  static HttpRequestInterceptor loadInterceptorDynamically(
      String impl, Map<String, String> properties) {
    HttpRequestInterceptor instance;

    DynConstructors.Ctor<HttpRequestInterceptor> ctor;
    try {
      ctor =
          DynConstructors.builder(HttpRequestInterceptor.class)
              .loader(HTTPClient.class.getClassLoader())
              .impl(impl)
              .buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize RequestInterceptor, missing no-arg constructor: %s", impl),
          e);
    }

    try {
      instance = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize, %s does not implement RequestInterceptor", impl), e);
    }

    DynMethods.builder("initialize")
        .hiddenImpl(impl, Map.class)
        .orNoop()
        .build(instance)
        .invoke(properties);

    return instance;
  }

  static HttpClientConnectionManager configureConnectionManager(Map<String, String> properties) {
    PoolingHttpClientConnectionManagerBuilder connectionManagerBuilder =
        PoolingHttpClientConnectionManagerBuilder.create();
    ConnectionConfig connectionConfig = configureConnectionConfig(properties);
    if (connectionConfig != null) {
      connectionManagerBuilder.setDefaultConnectionConfig(connectionConfig);
    }

    return connectionManagerBuilder
        .useSystemProperties()
        .setMaxConnTotal(
            Integer.getInteger(
                REST_MAX_CONNECTIONS,
                PropertyUtil.propertyAsInt(
                    properties, REST_MAX_CONNECTIONS, REST_MAX_CONNECTIONS_DEFAULT)))
        .setMaxConnPerRoute(
            PropertyUtil.propertyAsInt(
                properties, REST_MAX_CONNECTIONS_PER_ROUTE, REST_MAX_CONNECTIONS_PER_ROUTE_DEFAULT))
        .build();
  }

  @VisibleForTesting
  static ConnectionConfig configureConnectionConfig(Map<String, String> properties) {
    Long connectionTimeoutMillis =
        PropertyUtil.propertyAsNullableLong(properties, REST_CONNECTION_TIMEOUT_MS);
    Integer socketTimeoutMillis =
        PropertyUtil.propertyAsNullableInt(properties, REST_SOCKET_TIMEOUT_MS);

    if (connectionTimeoutMillis == null && socketTimeoutMillis == null) {
      return null;
    }

    ConnectionConfig.Builder connConfigBuilder = ConnectionConfig.custom();

    if (connectionTimeoutMillis != null) {
      connConfigBuilder.setConnectTimeout(connectionTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    if (socketTimeoutMillis != null) {
      connConfigBuilder.setSocketTimeout(socketTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    return connConfigBuilder.build();
  }

  public static Builder builder(Map<String, String> properties) {
    return new Builder(properties);
  }

  public static class Builder {
    private final Map<String, String> properties;
    private final Map<String, String> baseHeaders = Maps.newHashMap();
    private URI uri;
    private ObjectMapper mapper = RESTObjectMapper.mapper();
    private HttpHost proxy;
    private CredentialsProvider proxyCredentialsProvider;
    private AuthSession authSession;

    private Builder(Map<String, String> properties) {
      this.properties = properties;
    }

    public Builder uri(String baseUri) {
      Preconditions.checkNotNull(baseUri, "Invalid uri for http client: null");
      try {
        this.uri = URI.create(RESTUtil.stripTrailingSlash(baseUri));
      } catch (IllegalArgumentException e) {
        throw new RESTException(e, "Failed to create request URI from base %s", baseUri);
      }
      return this;
    }

    public Builder uri(URI baseUri) {
      Preconditions.checkNotNull(baseUri, "Invalid uri for http client: null");
      this.uri = baseUri;
      return this;
    }

    public Builder withProxy(String hostname, int port) {
      Preconditions.checkNotNull(hostname, "Invalid hostname for http client proxy: null");
      this.proxy = new HttpHost(hostname, port);
      return this;
    }

    public Builder withProxyCredentialsProvider(CredentialsProvider credentialsProvider) {
      Preconditions.checkNotNull(
          credentialsProvider, "Invalid credentials provider for http client proxy: null");
      this.proxyCredentialsProvider = credentialsProvider;
      return this;
    }

    public Builder withHeader(String key, String value) {
      baseHeaders.put(key, value);
      return this;
    }

    public Builder withHeaders(Map<String, String> headers) {
      baseHeaders.putAll(headers);
      return this;
    }

    public Builder withObjectMapper(ObjectMapper objectMapper) {
      this.mapper = objectMapper;
      return this;
    }

    public Builder withAuthSession(AuthSession session) {
      this.authSession = session;
      return this;
    }

    public HTTPClient build() {
      withHeader(CLIENT_VERSION_HEADER, IcebergBuild.fullVersion());
      withHeader(CLIENT_GIT_COMMIT_SHORT_HEADER, IcebergBuild.gitCommitShortId());

      HttpRequestInterceptor interceptor = null;

      if (PropertyUtil.propertyAsBoolean(properties, SIGV4_ENABLED, false)) {
        interceptor = loadInterceptorDynamically(SIGV4_REQUEST_INTERCEPTOR_IMPL, properties);
      }

      if (this.proxyCredentialsProvider != null) {
        Preconditions.checkNotNull(
            proxy, "Invalid http client proxy for proxy credentials provider: null");
      }

      return new HTTPClient(
          uri,
          proxy,
          proxyCredentialsProvider,
          baseHeaders,
          mapper,
          interceptor,
          properties,
          configureConnectionManager(properties),
          authSession);
    }
  }
}
