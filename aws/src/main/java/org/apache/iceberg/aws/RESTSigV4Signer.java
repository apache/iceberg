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
package org.apache.iceberg.aws;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.auth.AuthSession;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.internal.SignerConstant;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.auth.signer.params.SignerChecksumParams;
import software.amazon.awssdk.core.checksums.Algorithm;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

/**
 * Provides a request interceptor for use with the HTTPClient that calculates the required signature
 * for the SigV4 protocol and adds the necessary headers for all requests created by the client.
 *
 * <p>See <a
 * href="https://docs.aws.amazon.com/general/latest/gr/signing-aws-api-requests.html">Signing AWS
 * API requests</a> for details about the protocol.
 */
public class RESTSigV4Signer implements AuthSession {
  static final String EMPTY_BODY_SHA256 =
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
  static final String RELOCATED_HEADER_PREFIX = "Original-";

  private final Aws4Signer signer = Aws4Signer.create();
  private AwsCredentialsProvider credentialsProvider;

  private String signingName;
  private Region signingRegion;

  public void initialize(Map<String, String> properties) {
    AwsProperties awsProperties = new AwsProperties(properties);

    this.signingRegion = awsProperties.restSigningRegion();
    this.signingName = awsProperties.restSigningName();
    this.credentialsProvider = awsProperties.restCredentialsProvider();
  }

  @Override
  public void authenticate(HTTPRequest request) {
    Aws4SignerParams params =
        Aws4SignerParams.builder()
            .signingName(signingName)
            .signingRegion(signingRegion)
            .awsCredentials(credentialsProvider.resolveCredentials())
            .checksumParams(
                SignerChecksumParams.builder()
                    .algorithm(Algorithm.SHA256)
                    .isStreamingRequest(false)
                    .checksumHeaderName(SignerConstant.X_AMZ_CONTENT_SHA256)
                    .build())
            .build();

    SdkHttpFullRequest.Builder sdkRequestBuilder = SdkHttpFullRequest.builder();

    sdkRequestBuilder
        .method(SdkHttpMethod.fromValue(request.method()))
        .protocol(request.uri().getScheme())
        .uri(request.uri())
        .headers(convertHeaders(request.headers()));

    if (request.body() == null) {
      // This is a workaround for the signer implementation incorrectly producing
      // an invalid content checksum for empty body requests.
      sdkRequestBuilder.putHeader(SignerConstant.X_AMZ_CONTENT_SHA256, EMPTY_BODY_SHA256);
    } else if (request.body() instanceof String) {
      sdkRequestBuilder.contentStreamProvider(
          () -> IOUtils.toInputStream((String) request.body(), StandardCharsets.UTF_8));
    } else {
      throw new UnsupportedOperationException(
          "Unsupported entity type: " + request.body().getClass());
    }

    SdkHttpFullRequest signedSdkRequest = signer.sign(sdkRequestBuilder.build(), params);
    updateRequestHeaders(request, signedSdkRequest.headers());
  }

  private Map<String, List<String>> convertHeaders(Map<String, List<String>> headers) {
    return headers.entrySet().stream()
        .collect(
            Collectors.groupingBy(
                // Relocate Authorization header as SigV4 takes precedence
                entry ->
                    HttpHeaders.AUTHORIZATION.equals(entry.getKey())
                        ? RELOCATED_HEADER_PREFIX + entry.getKey()
                        : entry.getKey(),
                Collectors.reducing(
                    Lists.newArrayList(),
                    Map.Entry::getValue,
                    (a, b) -> {
                      a.addAll(b);
                      return a;
                    })));
  }

  private void updateRequestHeaders(HTTPRequest request, Map<String, List<String>> headers) {
    headers.forEach(
        (name, values) -> {
          if (request.containsHeader(name)) {
            List<String> original = request.headers(name);
            request.removeHeaders(name);
            original.forEach(
                header -> {
                  // Relocate headers if there is a conflict with signed headers
                  if (!values.contains(header)) {
                    request.addHeader(RELOCATED_HEADER_PREFIX + name, header);
                  }
                });
          }

          values.forEach(value -> request.setHeader(name, value));
        });
  }
}
