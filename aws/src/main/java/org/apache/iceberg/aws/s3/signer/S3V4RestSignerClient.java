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
package org.apache.iceberg.aws.s3.signer;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.auth.OAuth2Util.AuthSession;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.ThreadPools;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.signer.internal.AbstractAws4Signer;
import software.amazon.awssdk.auth.signer.internal.Aws4SignerRequestParams;
import software.amazon.awssdk.auth.signer.params.Aws4PresignerParams;
import software.amazon.awssdk.auth.signer.params.AwsS3V4SignerParams;
import software.amazon.awssdk.core.checksums.SdkChecksum;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.utils.IoUtils;

@Value.Immutable
public abstract class S3V4RestSignerClient
    extends AbstractAws4Signer<AwsS3V4SignerParams, Aws4PresignerParams> {

  private static final Logger LOG = LoggerFactory.getLogger(S3V4RestSignerClient.class);
  public static final String S3_SIGNER_URI = "s3.signer.uri";
  public static final String S3_SIGNER_ENDPOINT = "s3.signer.endpoint";
  static final String S3_SIGNER_DEFAULT_ENDPOINT = "v1/aws/s3/sign";
  static final String UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";
  static final String CACHE_CONTROL = "Cache-Control";
  static final String CACHE_CONTROL_PRIVATE = "private";
  static final String CACHE_CONTROL_NO_CACHE = "no-cache";

  private static final Cache<Key, SignedComponent> SIGNED_COMPONENT_CACHE =
      Caffeine.newBuilder().expireAfterWrite(30, TimeUnit.SECONDS).maximumSize(100).build();

  private static final String SCOPE = "sign";

  @SuppressWarnings("immutables:incompat")
  private static volatile ScheduledExecutorService tokenRefreshExecutor;

  @SuppressWarnings("immutables:incompat")
  private static volatile RESTClient httpClient;

  @SuppressWarnings("immutables:incompat")
  private static volatile Cache<String, AuthSession> authSessionCache;

  public abstract Map<String, String> properties();

  @Value.Default
  public Supplier<Map<String, String>> requestPropertiesSupplier() {
    return Collections::emptyMap;
  }

  @Value.Lazy
  public String baseSignerUri() {
    return properties().getOrDefault(S3_SIGNER_URI, properties().get(CatalogProperties.URI));
  }

  @Value.Lazy
  public String endpoint() {
    return properties().getOrDefault(S3_SIGNER_ENDPOINT, S3_SIGNER_DEFAULT_ENDPOINT);
  }

  /** A credential to exchange for a token in the OAuth2 client credentials flow. */
  @Nullable
  @Value.Lazy
  public String credential() {
    return properties().get(OAuth2Properties.CREDENTIAL);
  }

  /** Token endpoint URI to fetch token from if the Rest Catalog is not the authorization server. */
  @Value.Lazy
  public String oauth2ServerUri() {
    return properties().getOrDefault(OAuth2Properties.OAUTH2_SERVER_URI, ResourcePaths.tokens());
  }

  /** A Bearer token supplier which will be used for interaction with the server. */
  @Value.Default
  public Supplier<String> token() {
    return () -> properties().get(OAuth2Properties.TOKEN);
  }

  @Value.Lazy
  boolean keepTokenRefreshed() {
    return PropertyUtil.propertyAsBoolean(
        properties(),
        OAuth2Properties.TOKEN_REFRESH_ENABLED,
        OAuth2Properties.TOKEN_REFRESH_ENABLED_DEFAULT);
  }

  @VisibleForTesting
  ScheduledExecutorService tokenRefreshExecutor() {
    if (!keepTokenRefreshed()) {
      return null;
    }

    if (null == tokenRefreshExecutor) {
      synchronized (S3V4RestSignerClient.class) {
        if (null == tokenRefreshExecutor) {
          tokenRefreshExecutor = ThreadPools.newScheduledPool("s3-signer-token-refresh", 1);
        }
      }
    }

    return tokenRefreshExecutor;
  }

  private Cache<String, AuthSession> authSessionCache() {
    if (null == authSessionCache) {
      synchronized (S3V4RestSignerClient.class) {
        if (null == authSessionCache) {
          long expirationIntervalMs =
              PropertyUtil.propertyAsLong(
                  properties(),
                  CatalogProperties.AUTH_SESSION_TIMEOUT_MS,
                  CatalogProperties.AUTH_SESSION_TIMEOUT_MS_DEFAULT);

          authSessionCache =
              Caffeine.newBuilder()
                  .expireAfterAccess(Duration.ofMillis(expirationIntervalMs))
                  .removalListener(
                      (RemovalListener<String, AuthSession>)
                          (id, auth, cause) -> {
                            if (null != auth) {
                              LOG.trace("Stopping refresh for AuthSession");
                              auth.stopRefreshing();
                            }
                          })
                  .build();
        }
      }
    }

    return authSessionCache;
  }

  private RESTClient httpClient() {
    if (null == httpClient) {
      synchronized (S3V4RestSignerClient.class) {
        if (null == httpClient) {
          httpClient =
              HTTPClient.builder(properties())
                  .uri(baseSignerUri())
                  .withObjectMapper(S3ObjectMapper.mapper())
                  .build();
        }
      }
    }

    return httpClient;
  }

  private AuthSession authSession() {
    String token = token().get();
    if (null != token) {
      return authSessionCache()
          .get(
              token,
              id ->
                  AuthSession.fromAccessToken(
                      httpClient(),
                      tokenRefreshExecutor(),
                      token,
                      expiresAtMillis(properties()),
                      new AuthSession(
                          ImmutableMap.of(), token, null, credential(), SCOPE, oauth2ServerUri())));
    }

    if (credentialProvided()) {
      return authSessionCache()
          .get(
              credential(),
              id -> {
                AuthSession session =
                    new AuthSession(
                        ImmutableMap.of(), null, null, credential(), SCOPE, oauth2ServerUri());
                long startTimeMillis = System.currentTimeMillis();
                OAuthTokenResponse authResponse =
                    OAuth2Util.fetchToken(
                        httpClient(), session.headers(), credential(), SCOPE, oauth2ServerUri());
                return AuthSession.fromTokenResponse(
                    httpClient(), tokenRefreshExecutor(), authResponse, startTimeMillis, session);
              });
    }

    return AuthSession.empty();
  }

  private boolean credentialProvided() {
    return null != credential() && !credential().isEmpty();
  }

  private Long expiresAtMillis(Map<String, String> properties) {
    if (properties.containsKey(OAuth2Properties.TOKEN_EXPIRES_IN_MS)) {
      long expiresInMillis =
          PropertyUtil.propertyAsLong(
              properties,
              OAuth2Properties.TOKEN_EXPIRES_IN_MS,
              OAuth2Properties.TOKEN_EXPIRES_IN_MS_DEFAULT);
      return System.currentTimeMillis() + expiresInMillis;
    } else {
      return null;
    }
  }

  @Value.Check
  protected void check() {
    Preconditions.checkArgument(
        properties().containsKey(S3_SIGNER_URI) || properties().containsKey(CatalogProperties.URI),
        "S3 signer service URI is required");
  }

  @Override
  protected void processRequestPayload(
      SdkHttpFullRequest.Builder mutableRequest,
      byte[] signature,
      byte[] signingKey,
      Aws4SignerRequestParams signerRequestParams,
      AwsS3V4SignerParams signerParams) {
    checkSignerParams(signerParams);
  }

  @Override
  protected void processRequestPayload(
      SdkHttpFullRequest.Builder mutableRequest,
      byte[] signature,
      byte[] signingKey,
      Aws4SignerRequestParams signerRequestParams,
      AwsS3V4SignerParams signerParams,
      SdkChecksum sdkChecksum) {
    checkSignerParams(signerParams);
  }

  @Override
  protected String calculateContentHashPresign(
      SdkHttpFullRequest.Builder mutableRequest, Aws4PresignerParams signerParams) {
    return UNSIGNED_PAYLOAD;
  }

  @Override
  public SdkHttpFullRequest presign(
      SdkHttpFullRequest request, ExecutionAttributes executionAttributes) {
    throw new UnsupportedOperationException("Pre-signing not allowed.");
  }

  @Override
  public SdkHttpFullRequest sign(
      SdkHttpFullRequest request, ExecutionAttributes executionAttributes) {
    AwsS3V4SignerParams signerParams =
        extractSignerParams(AwsS3V4SignerParams.builder(), executionAttributes).build();

    S3SignRequest remoteSigningRequest =
        ImmutableS3SignRequest.builder()
            .method(request.method().name())
            .region(signerParams.signingRegion().id())
            .uri(request.getUri())
            .headers(request.headers())
            .properties(requestPropertiesSupplier().get())
            .body(bodyAsString(request))
            .build();

    Key cacheKey = Key.from(remoteSigningRequest);
    SignedComponent cachedSignedComponent = SIGNED_COMPONENT_CACHE.getIfPresent(cacheKey);
    SignedComponent signedComponent;

    if (null != cachedSignedComponent) {
      signedComponent = cachedSignedComponent;
    } else {
      Map<String, String> responseHeaders = Maps.newHashMap();
      Consumer<Map<String, String>> responseHeadersConsumer = responseHeaders::putAll;
      S3SignResponse s3SignResponse =
          httpClient()
              .post(
                  endpoint(),
                  remoteSigningRequest,
                  S3SignResponse.class,
                  () -> authSession().headers(),
                  ErrorHandlers.defaultErrorHandler(),
                  responseHeadersConsumer);

      signedComponent =
          ImmutableSignedComponent.builder()
              .headers(s3SignResponse.headers())
              .signedURI(s3SignResponse.uri())
              .build();

      if (canBeCached(responseHeaders)) {
        SIGNED_COMPONENT_CACHE.put(cacheKey, signedComponent);
      }
    }

    // The SdkHttpFullRequest Builder appends the raw path from the input URI in .uri(),
    // so we need to clear the current path from the request
    SdkHttpFullRequest.Builder mutableRequest = request.toBuilder();
    mutableRequest.encodedPath("");
    mutableRequest.uri(signedComponent.signedURI());
    reconstructHeaders(signedComponent.headers(), mutableRequest);

    return mutableRequest.build();
  }

  /**
   * Only add body for DeleteObjectsRequest. Refer to
   * https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html#API_DeleteObjects_RequestSyntax
   */
  private String bodyAsString(SdkHttpFullRequest request) {
    if (isDeleteObjectsRequest(request) && request.contentStreamProvider().isPresent()) {
      try (InputStream is = request.contentStreamProvider().get().newStream()) {
        return IoUtils.toUtf8String(is);
      } catch (IOException e) {
        LOG.debug("Failed to determine body for S3 sign request", e);
      }
    }

    return null;
  }

  private boolean isDeleteObjectsRequest(SdkHttpFullRequest request) {
    return request.method() == SdkHttpMethod.POST
        && request.rawQueryParameters().containsKey("delete");
  }

  private void reconstructHeaders(
      Map<String, List<String>> signedAndUnsignedHeaders,
      SdkHttpFullRequest.Builder mutableRequest) {
    Map<String, List<String>> headers = Maps.newHashMap(signedAndUnsignedHeaders);
    // we need to remove the Cache-Control header that is being sent by the server
    headers.remove(CACHE_CONTROL);

    // we need to overwrite whatever headers the server signed/unsigned with the ones from the
    // original request and then put all headers back to the request
    headers.putAll(mutableRequest.headers());
    headers.forEach(mutableRequest::putHeader);
  }

  private boolean canBeCached(Map<String, String> responseHeaders) {
    return CACHE_CONTROL_PRIVATE.equals(responseHeaders.get(CACHE_CONTROL));
  }

  private void checkSignerParams(AwsS3V4SignerParams signerParams) {
    if (signerParams.enablePayloadSigning()) {
      throw new UnsupportedOperationException("Payload signing not supported");
    }

    if (signerParams.enableChunkedEncoding()) {
      throw new UnsupportedOperationException("Chunked encoding not supported");
    }
  }

  @Value.Immutable
  interface Key {
    String method();

    String region();

    String uri();

    static Key from(S3SignRequest request) {
      return ImmutableKey.builder()
          .method(request.method())
          .region(request.region())
          .uri(request.uri().toString())
          .build();
    }
  }

  @Value.Immutable
  interface SignedComponent {
    Map<String, List<String>> headers();

    URI signedURI();
  }

  public static S3V4RestSignerClient create(Map<String, String> properties) {
    return ImmutableS3V4RestSignerClient.builder().properties(properties).build();
  }
}
