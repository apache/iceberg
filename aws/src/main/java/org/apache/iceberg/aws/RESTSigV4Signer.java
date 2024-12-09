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

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
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
 * A SigV4 signer that calculates the required signature for the SigV4 protocol and adds the
 * necessary headers for all requests created by the client.
 *
 * <p>See <a href="https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv.html">Signing AWS
 * API requests</a> for details about the protocol.
 */
public class RESTSigV4Signer {
  static final String EMPTY_BODY_SHA256 =
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
  static final String RELOCATED_HEADER_PREFIX = "Original-";

  private final Aws4Signer signer = Aws4Signer.create();
  private final AwsCredentialsProvider credentialsProvider;

  private final String signingName;
  private final Region signingRegion;

  public RESTSigV4Signer(Map<String, String> properties) {
    AwsProperties awsProperties = new AwsProperties(properties);

    this.signingRegion = awsProperties.restSigningRegion();
    this.signingName = awsProperties.restSigningName();
    this.credentialsProvider = awsProperties.restCredentialsProvider();
  }

  public HTTPRequest sign(HTTPRequest request) {
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

    URI uri = request.requestUri();
    sdkRequestBuilder
        .method(SdkHttpMethod.fromValue(request.method().name()))
        .protocol(uri.getScheme())
        .uri(uri)
        .headers(convertHeaders(request.headers()));

    String body = request.encodedBody();
    if (body == null) {
      // This is a workaround for the signer implementation incorrectly producing
      // an invalid content checksum for empty body requests.
      sdkRequestBuilder.putHeader(SignerConstant.X_AMZ_CONTENT_SHA256, EMPTY_BODY_SHA256);
    } else {
      sdkRequestBuilder.contentStreamProvider(
          () -> IOUtils.toInputStream(body, StandardCharsets.UTF_8));
    }

    SdkHttpFullRequest signedSdkRequest = signer.sign(sdkRequestBuilder.build(), params);
    Map<String, List<String>> newHeaders =
        updateRequestHeaders(request, signedSdkRequest.headers());
    return ImmutableHTTPRequest.builder().from(request).headers(newHeaders).build();
  }

  private Map<String, List<String>> convertHeaders(Map<String, List<String>> headers) {
    Map<String, List<String>> converted = Maps.newHashMap();
    headers.forEach(
        (name, values) -> {
          if (name.equals(HttpHeaders.AUTHORIZATION)) {
            converted.merge(
                RELOCATED_HEADER_PREFIX + name,
                values,
                (v1, v2) -> {
                  List<String> merged = Lists.newArrayList(v1);
                  merged.addAll(v2);
                  return List.copyOf(merged);
                });
          } else {
            converted.put(name, values);
          }
        });
    return converted;
  }

  private Map<String, List<String>> updateRequestHeaders(
      HTTPRequest request, Map<String, List<String>> signedHeaders) {
    Map<String, List<String>> newHeaders = Maps.newLinkedHashMap();
    newHeaders.putAll(request.headers());
    signedHeaders.forEach(
        (name, signedValues) -> {
          if (request.containsHeader(name)) {
            List<String> originalValues = request.headers(name);
            newHeaders.remove(name);
            originalValues.forEach(
                originalValue -> {
                  // Relocate headers if there is a conflict with signed headers
                  if (!signedValues.contains(originalValue)) {
                    newHeaders.compute(
                        RELOCATED_HEADER_PREFIX + name,
                        (k, v) -> {
                          if (v == null) {
                            return List.of(originalValue);
                          } else {
                            List<String> merged = Lists.newArrayList(v);
                            merged.add(originalValue);
                            return List.copyOf(merged);
                          }
                        });
                  }
                });
          }

          newHeaders.put(name, signedValues);
        });
    return newHeaders;
  }
}
