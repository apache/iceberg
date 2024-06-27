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

import static java.lang.String.format;
import static org.apache.iceberg.rest.RESTCatalogAdapter.castRequest;
import static org.apache.iceberg.rest.RESTCatalogAdapter.castResponse;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStreamReader;
import java.io.Reader;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.io.CharStreams;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.signer.AwsS3V4Signer;
import software.amazon.awssdk.auth.signer.params.AwsS3V4SignerParams;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

/**
 * The {@link S3V4RestSignerClient} performs OAuth and S3 sign requests against a REST server. The
 * {@link S3SignerServlet} provides a simple servlet implementation to emulate the server-side
 * behavior of signing S3 requests and handling OAuth.
 */
public class S3SignerServlet extends HttpServlet {

  private static final Logger LOG = LoggerFactory.getLogger(S3SignerServlet.class);

  static final Clock SIGNING_CLOCK = Clock.fixed(Instant.now(), ZoneId.of("UTC"));
  static final Set<String> UNSIGNED_HEADERS =
      Sets.newHashSet(
          Arrays.asList("range", "x-amz-date", "amz-sdk-invocation-id", "amz-sdk-retry"));
  private static final String POST = "POST";

  private static final Set<SdkHttpMethod> CACHEABLE_METHODS =
      Stream.of(SdkHttpMethod.GET, SdkHttpMethod.HEAD).collect(Collectors.toSet());

  private final Map<String, String> responseHeaders =
      ImmutableMap.of(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
  private final ObjectMapper mapper;

  private List<SignRequestValidator> s3SignRequestValidators = Lists.newArrayList();

  /**
   * SignRequestValidator is a wrapper class used for validating the contents of the S3SignRequest
   * and thus verifying the behavior of the client during testing.
   */
  public static class SignRequestValidator {
    private final Predicate<S3SignRequest> requestMatcher;
    private final Predicate<S3SignRequest> requestExpectation;
    private final String assertMessage;

    public SignRequestValidator(
        Predicate<S3SignRequest> requestExpectation,
        Predicate<S3SignRequest> requestMatcher,
        String assertMessage) {
      this.requestExpectation = requestExpectation;
      this.requestMatcher = requestMatcher;
      this.assertMessage = assertMessage;
    }

    void validateRequest(S3SignRequest request) {
      if (requestMatcher.test(request)) {
        assertThat(requestExpectation.test(request)).as(assertMessage).isTrue();
      }
    }
  }

  public S3SignerServlet(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  public S3SignerServlet(ObjectMapper mapper, List<SignRequestValidator> s3SignRequestValidators) {
    this.mapper = mapper;
    this.s3SignRequestValidators = s3SignRequestValidators;
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) {
    execute(request, response);
  }

  @Override
  protected void doHead(HttpServletRequest request, HttpServletResponse response) {
    execute(request, response);
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) {
    execute(request, response);
  }

  @Override
  protected void doDelete(HttpServletRequest request, HttpServletResponse response) {
    execute(request, response);
  }

  private OAuthTokenResponse handleOAuth(Map<String, String> requestMap) {
    String grantType = requestMap.get("grant_type");
    switch (grantType) {
      case "client_credentials":
        return castResponse(
            OAuthTokenResponse.class,
            OAuthTokenResponse.builder()
                .withToken("client-credentials-token:sub=" + requestMap.get("client_id"))
                .withIssuedTokenType("urn:ietf:params:oauth:token-type:access_token")
                .withTokenType("Bearer")
                .setExpirationInSeconds(100)
                .build());

      case "urn:ietf:params:oauth:grant-type:token-exchange":
        String actor = requestMap.get("actor_token");
        String token =
            String.format(
                "token-exchange-token:sub=%s%s",
                requestMap.get("subject_token"), actor != null ? ",act=" + actor : "");
        return castResponse(
            OAuthTokenResponse.class,
            OAuthTokenResponse.builder()
                .withToken(token)
                .withIssuedTokenType("urn:ietf:params:oauth:token-type:access_token")
                .withTokenType("Bearer")
                .setExpirationInSeconds(100)
                .build());

      default:
        throw new UnsupportedOperationException("Unsupported grant_type: " + grantType);
    }
  }

  private S3SignResponse signRequest(S3SignRequest request) {
    AwsS3V4SignerParams signingParams =
        AwsS3V4SignerParams.builder()
            .awsCredentials(TestS3RestSigner.CREDENTIALS_PROVIDER.resolveCredentials())
            .enablePayloadSigning(false)
            .signingClockOverride(SIGNING_CLOCK)
            .enableChunkedEncoding(false)
            .signingRegion(Region.of(request.region()))
            .doubleUrlEncode(false)
            .timeOffset(0)
            .signingName("s3")
            .build();

    Map<String, List<String>> unsignedHeaders =
        request.headers().entrySet().stream()
            .filter(e -> UNSIGNED_HEADERS.contains(e.getKey().toLowerCase(Locale.ROOT)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    Map<String, List<String>> signedHeaders =
        request.headers().entrySet().stream()
            .filter(e -> !UNSIGNED_HEADERS.contains(e.getKey().toLowerCase(Locale.ROOT)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    SdkHttpFullRequest sign =
        AwsS3V4Signer.create()
            .sign(
                SdkHttpFullRequest.builder()
                    .uri(request.uri())
                    .method(SdkHttpMethod.fromValue(request.method()))
                    .headers(signedHeaders)
                    .build(),
                signingParams);

    Map<String, List<String>> headers = Maps.newHashMap(sign.headers());
    headers.putAll(unsignedHeaders);

    return ImmutableS3SignResponse.builder().uri(request.uri()).headers(headers).build();
  }

  protected void execute(HttpServletRequest request, HttpServletResponse response) {
    response.setStatus(HttpServletResponse.SC_OK);
    responseHeaders.forEach(response::setHeader);

    String path = request.getRequestURI().substring(1);
    Object requestBody;
    try {
      // we only need to handle oauth tokens & s3 sign request routes here as those are the only
      // requests that are being done by the S3V4RestSignerClient
      if (POST.equals(request.getMethod())
          && S3V4RestSignerClient.S3_SIGNER_DEFAULT_ENDPOINT.equals(path)) {
        S3SignRequest s3SignRequest =
            castRequest(
                S3SignRequest.class, mapper.readValue(request.getReader(), S3SignRequest.class));
        s3SignRequestValidators.forEach(validator -> validator.validateRequest(s3SignRequest));
        S3SignResponse s3SignResponse = signRequest(s3SignRequest);
        if (CACHEABLE_METHODS.contains(SdkHttpMethod.fromValue(s3SignRequest.method()))) {
          // tell the client this can be cached
          response.setHeader(
              S3V4RestSignerClient.CACHE_CONTROL, S3V4RestSignerClient.CACHE_CONTROL_PRIVATE);
        } else {
          response.setHeader(
              S3V4RestSignerClient.CACHE_CONTROL, S3V4RestSignerClient.CACHE_CONTROL_NO_CACHE);
        }

        mapper.writeValue(response.getWriter(), s3SignResponse);
      } else if (POST.equals(request.getMethod()) && ResourcePaths.tokens().equals(path)) {
        try (Reader reader = new InputStreamReader(request.getInputStream())) {
          requestBody = RESTUtil.decodeFormData(CharStreams.toString(reader));
        }

        OAuthTokenResponse oAuthTokenResponse =
            handleOAuth((Map<String, String>) castRequest(Map.class, requestBody));
        mapper.writeValue(response.getWriter(), oAuthTokenResponse);
      } else {
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        mapper.writeValue(
            response.getWriter(),
            ErrorResponse.builder()
                .responseCode(400)
                .withType("BadRequestException")
                .withMessage(format("No route for request: %s %s", request.getMethod(), path))
                .build());
      }
    } catch (RESTException e) {
      LOG.error("Error processing REST request", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    } catch (Exception e) {
      LOG.error("Unexpected exception when processing REST request", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }
}
