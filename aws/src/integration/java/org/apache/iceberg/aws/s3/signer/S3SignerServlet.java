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

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.rest.HttpMethod;
import org.apache.iceberg.rest.RemoteSignerServlet;
import org.apache.iceberg.rest.RemoteSigningClient;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.iceberg.rest.responses.ImmutableRemoteSignResponse;
import org.apache.iceberg.rest.responses.RemoteSignResponse;
import software.amazon.awssdk.auth.signer.AwsS3V4Signer;
import software.amazon.awssdk.auth.signer.params.AwsS3V4SignerParams;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

/**
 * The {@link S3V4RestSignerClient} performs OAuth and S3 sign requests against a REST server. The
 * {@link S3SignerServlet} provides a simple servlet implementation to emulate the server-side
 * behavior of signing S3 requests and handling OAuth.
 */
public class S3SignerServlet extends RemoteSignerServlet {

  static final Clock SIGNING_CLOCK = Clock.fixed(Instant.now(), ZoneId.of("UTC"));
  static final Set<String> UNSIGNED_HEADERS =
      Sets.newHashSet(
          Arrays.asList("range", "x-amz-date", "amz-sdk-invocation-id", "amz-sdk-retry"));

  /** A fake remote signing endpoint for testing purposes. */
  static final String S3_SIGNER_ENDPOINT = "v1/namespaces/ns1/tables/t1/sign";

  private final ThreadLocal<Boolean> preSignedUrlRequested = ThreadLocal.withInitial(() -> false);
  private final URI s3Endpoint;
  private final Region s3Region;
  private volatile RemoteSignRequest lastRequest;

  public S3SignerServlet() {
    this(null, null);
  }

  public S3SignerServlet(URI s3Endpoint, Region s3Region) {
    super(S3_SIGNER_ENDPOINT);
    this.s3Endpoint = s3Endpoint;
    this.s3Region = s3Region;
  }

  @Override
  protected void execute(HttpServletRequest request, HttpServletResponse response) {
    String delegation = request.getHeader(RemoteSigningClient.ACCESS_DELEGATION_HEADER);
    preSignedUrlRequested.set(RemoteSigningClient.PRESIGNED_URLS.equals(delegation));
    try {
      super.execute(request, response);
    } finally {
      preSignedUrlRequested.remove();
    }
  }

  @Override
  protected void validateSignRequest(RemoteSignRequest request) {
    Preconditions.checkArgument(
        request.provider() == null || "s3".equalsIgnoreCase(request.provider()),
        "Unsupported provider: %s",
        request.provider());
    if (HttpMethod.POST.name().equalsIgnoreCase(request.method())
        && request.uri().getQuery().contains("delete")) {
      String body = request.body();
      Preconditions.checkArgument(
          body != null && !body.isEmpty(),
          "Sign request for delete objects should have a request body");
    }
  }

  RemoteSignRequest lastRequest() {
    return lastRequest;
  }

  @Override
  protected RemoteSignResponse signRequest(RemoteSignRequest request) {
    this.lastRequest = request;
    if (preSignedUrlRequested.get()) {
      return preSign(request);
    }

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

    return ImmutableRemoteSignResponse.builder().uri(request.uri()).headers(headers).build();
  }

  private RemoteSignResponse preSign(RemoteSignRequest request) {
    Preconditions.checkState(s3Endpoint != null, "This signer does not pre-sign");
    URI location = request.uri();
    Preconditions.checkArgument(
        "s3".equalsIgnoreCase(location.getScheme()), "Not an S3 location: %s", location);

    try (S3Presigner presigner =
        S3Presigner.builder()
            .endpointOverride(s3Endpoint)
            .region(request.region().isEmpty() ? s3Region : Region.of(request.region()))
            .credentialsProvider(TestS3RestSigner.CREDENTIALS_PROVIDER)
            .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
            .build()) {
      PresignedGetObjectRequest presigned =
          presigner.presignGetObject(
              GetObjectPresignRequest.builder()
                  .signatureDuration(Duration.ofMinutes(10))
                  .getObjectRequest(
                      GetObjectRequest.builder()
                          .bucket(location.getAuthority())
                          .key(location.getPath().substring(1))
                          .build())
                  .build());

      return ImmutableRemoteSignResponse.builder()
          .uri(URI.create(presigned.url().toString()))
          .build();
    }
  }
}
