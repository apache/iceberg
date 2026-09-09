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
package org.apache.iceberg.aliyun.odps.auth;

// Signing logic ported from com.aliyun.odps:odps-sdk-core's AliyunRequestSigner /
// SecurityUtils so this module can be used without pulling in the full ODPS SDK.

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.iceberg.aliyun.AliyunProperties;
import org.apache.iceberg.aliyun.auth.AliyunRequestSigner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.HTTPHeaders.HTTPHeader;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.ImmutableHTTPHeaders;
import org.apache.iceberg.rest.ImmutableHTTPRequest;

/** Signs each outgoing request with the ODPS V2 or V4 protocol. */
public final class OdpsRequestSigner implements AliyunRequestSigner {

  static final String HEADER_PREFIX = "x-odps-";
  static final String CONTENT_TYPE = "Content-Type";
  static final String CONTENT_MD5 = "Content-MD5";
  static final String DATE = "Date";

  private static final String AUTHORIZATION = "Authorization";
  private static final String NEW_LINE = "\n";
  private static final DateTimeFormatter DATE_FMT =
      DateTimeFormatter.ofPattern("yyyyMMdd", Locale.ROOT);
  private static final DateTimeFormatter RFC1123 =
      DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.ENGLISH);

  private final String accessId;
  private final String accessKey;
  private final String region;
  private final String corporation;
  private final String stsToken;
  private final Clock clock;

  public OdpsRequestSigner(AliyunProperties aliyunProperties, Map<String, String> properties) {
    Preconditions.checkNotNull(aliyunProperties, "Invalid aliyunProperties: null");
    Preconditions.checkArgument(
        aliyunProperties.restAccessKeyId() != null && !aliyunProperties.restAccessKeyId().isEmpty(),
        "accessKeyId should not be empty");
    Preconditions.checkArgument(
        aliyunProperties.restAccessKeySecret() != null
            && !aliyunProperties.restAccessKeySecret().isEmpty(),
        "accessKeySecret should not be empty");
    this.accessId = aliyunProperties.restAccessKeyId();
    this.accessKey = aliyunProperties.restAccessKeySecret();
    this.region = aliyunProperties.restSigningRegion();
    String corp =
        properties.getOrDefault(
            OdpsAuthProperties.CORPORATION, OdpsAuthProperties.CORPORATION_DEFAULT);
    this.corporation = corp.isEmpty() ? OdpsAuthProperties.CORPORATION_DEFAULT : corp;
    this.stsToken = aliyunProperties.restStsToken();
    this.clock = Clock.systemUTC();
  }

  @Override
  public HTTPRequest sign(HTTPRequest request) {
    URI uri = request.requestUri();
    String resource = resource(uri);

    Map<String, String> headerMap = normalizeHeaders(request.headers());
    String authValue =
        authorization(request.method().name(), resource, headerMap, request.queryParameters());

    ImmutableHTTPHeaders.Builder builder = ImmutableHTTPHeaders.builder();
    headerMap.forEach((name, value) -> builder.addEntry(HTTPHeader.of(name, value)));
    builder.addEntry(HTTPHeader.of(AUTHORIZATION, authValue));
    if (stsToken != null && !stsToken.isEmpty()) {
      builder.addEntry(HTTPHeader.of(OdpsAuthProperties.STS_TOKEN_HEADER, stsToken));
    }

    HTTPHeaders signedHeaders = builder.build();
    return ImmutableHTTPRequest.builder().from(request).headers(signedHeaders).build();
  }

  private static String resource(URI uri) {
    String rawPath = uri.getRawPath();
    return rawPath == null || rawPath.isEmpty()
        ? "/"
        : URLDecoder.decode(rawPath, StandardCharsets.UTF_8);
  }

  String authorization(
      String method,
      String resource,
      Map<String, String> headers,
      Map<String, String> queryParams) {
    ensureDateHeader(headers);
    String canonical = canonicalString(method, resource, headers, queryParams);
    if (region == null || region.isEmpty()) {
      return signV2(canonical);
    }
    String scopeDate = ZonedDateTime.now(clock.withZone(ZoneOffset.UTC)).format(DATE_FMT);
    return signV4(canonical, scopeDate);
  }

  private void ensureDateHeader(Map<String, String> headers) {
    if (headers.get(DATE) == null) {
      ZonedDateTime now = ZonedDateTime.now(clock.withZone(ZoneOffset.UTC));
      headers.put(DATE, RFC1123.format(now));
    }
  }

  private String signV2(String stringToSign) {
    byte[] sig =
        hmacSha1(
            stringToSign.getBytes(StandardCharsets.UTF_8),
            accessKey.getBytes(StandardCharsets.UTF_8));
    return "ODPS " + accessId + ":" + Base64.getEncoder().encodeToString(sig).trim();
  }

  private String signV4(String stringToSign, String date) {
    String credential =
        accessId + "/" + date + "/" + region + "/odps/" + corporation + "_v4_request";
    byte[] derivedKey = derivedV4Key(date);
    byte[] sig = hmacSha1(stringToSign.getBytes(StandardCharsets.UTF_8), derivedKey);
    return "ODPS " + credential + ":" + Base64.getEncoder().encodeToString(sig);
  }

  private byte[] derivedV4Key(String date) {
    byte[] kSecret = (corporation + "_v4" + accessKey).getBytes(StandardCharsets.UTF_8);
    byte[] kDate = hmacSha256(date.getBytes(StandardCharsets.UTF_8), kSecret);
    byte[] kRegion = hmacSha256(region.getBytes(StandardCharsets.UTF_8), kDate);
    byte[] kService = hmacSha256("odps".getBytes(StandardCharsets.UTF_8), kRegion);
    return hmacSha256((corporation + "_v4_request").getBytes(StandardCharsets.UTF_8), kService);
  }

  static String canonicalString(
      String method,
      String resource,
      Map<String, String> headers,
      Map<String, String> queryParams) {
    StringBuilder builder = new StringBuilder();
    builder.append(method).append(NEW_LINE);

    Map<String, String> headersToSign = Maps.newTreeMap();
    addHeadersToSign(headersToSign, headers);
    headersToSign.putIfAbsent(CONTENT_TYPE.toLowerCase(Locale.ROOT), "");
    headersToSign.putIfAbsent(CONTENT_MD5.toLowerCase(Locale.ROOT), "");

    if (queryParams != null) {
      for (Map.Entry<String, String> e : queryParams.entrySet()) {
        if (e.getKey() != null && e.getKey().startsWith(HEADER_PREFIX)) {
          headersToSign.put(e.getKey(), e.getValue());
        }
      }
    }

    appendCanonicalHeaders(builder, headersToSign);
    builder.append(canonicalResource(resource, queryParams));
    return builder.toString();
  }

  private static void addHeadersToSign(
      Map<String, String> headersToSign, Map<String, String> headers) {
    if (headers == null) {
      return;
    }

    for (Map.Entry<String, String> e : headers.entrySet()) {
      if (e.getKey() == null) {
        continue;
      }

      String lower = e.getKey().toLowerCase(Locale.ROOT);
      if (shouldSignHeader(lower)) {
        headersToSign.put(lower, e.getValue());
      }
    }
  }

  private static boolean shouldSignHeader(String headerName) {
    return headerName.equals(CONTENT_TYPE.toLowerCase(Locale.ROOT))
        || headerName.equals(CONTENT_MD5.toLowerCase(Locale.ROOT))
        || headerName.equals(DATE.toLowerCase(Locale.ROOT))
        || headerName.startsWith(HEADER_PREFIX);
  }

  private static void appendCanonicalHeaders(
      StringBuilder builder, Map<String, String> headersToSign) {
    for (Map.Entry<String, String> entry : headersToSign.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (key.startsWith(HEADER_PREFIX)) {
        builder.append(key).append(':');
        if (value != null) {
          builder.append(value);
        }
      } else {
        builder.append(value == null ? "" : value);
      }
      builder.append(NEW_LINE);
    }
  }

  private static String canonicalResource(String resource, Map<String, String> params) {
    StringBuilder builder = new StringBuilder();
    builder.append(resource);
    if (params != null && !params.isEmpty()) {
      String[] names = params.keySet().toArray(new String[0]);
      Arrays.sort(names);
      char separator = '?';
      for (String name : names) {
        builder.append(separator).append(name);
        String value = params.get(name);
        if (value != null && !value.isEmpty()) {
          builder.append('=').append(value);
        }
        separator = '&';
      }
    }
    return builder.toString();
  }

  private static Map<String, String> normalizeHeaders(HTTPHeaders headers) {
    Map<String, HTTPHeader> normalized = new LinkedHashMap<>();
    headers
        .entries()
        .forEach(
            header -> {
              if (header.name().equalsIgnoreCase(AUTHORIZATION)) {
                return;
              }

              String lowerCaseName = header.name().toLowerCase(Locale.ROOT);
              HTTPHeader existing = normalized.get(lowerCaseName);
              Preconditions.checkArgument(
                  existing == null || existing.value().equals(header.value()),
                  "ODPS auth does not support multiple values for header %s",
                  header.name());
              if (existing == null) {
                normalized.put(
                    lowerCaseName,
                    HTTPHeader.of(canonicalHeaderName(header.name()), header.value()));
              }
            });

    Map<String, String> normalizedValues = new LinkedHashMap<>();
    normalized.values().forEach(header -> normalizedValues.put(header.name(), header.value()));
    return normalizedValues;
  }

  private static String canonicalHeaderName(String name) {
    String lowerCaseName = name.toLowerCase(Locale.ROOT);
    if (CONTENT_TYPE.toLowerCase(Locale.ROOT).equals(lowerCaseName)) {
      return CONTENT_TYPE;
    }

    if (CONTENT_MD5.toLowerCase(Locale.ROOT).equals(lowerCaseName)) {
      return CONTENT_MD5;
    }

    if (DATE.toLowerCase(Locale.ROOT).equals(lowerCaseName)) {
      return DATE;
    }

    return name;
  }

  private static byte[] hmacSha1(byte[] data, byte[] key) {
    return hmac("HmacSHA1", data, key);
  }

  private static byte[] hmacSha256(byte[] data, byte[] key) {
    return hmac("HmacSHA256", data, key);
  }

  private static byte[] hmac(String algo, byte[] data, byte[] key) {
    try {
      Mac mac = Mac.getInstance(algo);
      mac.init(new SecretKeySpec(key, algo));
      return mac.doFinal(data);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("Algorithm not available: " + algo, e);
    } catch (InvalidKeyException e) {
      throw new IllegalStateException("Invalid key for " + algo, e);
    }
  }
}
