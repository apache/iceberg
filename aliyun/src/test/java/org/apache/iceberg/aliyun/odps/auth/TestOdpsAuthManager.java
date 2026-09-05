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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.util.Map;
import java.util.stream.Stream;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.iceberg.aliyun.AliyunProperties;
import org.apache.iceberg.aliyun.auth.AliyunAuthManager;
import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.HTTPHeaders.HTTPHeader;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthManagers;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.AuthSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestOdpsAuthManager {

  private static final String AK = "LTAI-fake-id";
  private static final String SK = "fake-secret";
  private static final String STS = "fake-sts-token";
  private static final String FIXED_DATE = "Thu, 28 May 2026 12:00:00 GMT";

  @Test
  void create() {
    try (AuthManager manager =
        AuthManagers.loadAuthManager(
            "test",
            Map.of(
                AuthProperties.AUTH_TYPE,
                AuthProperties.AUTH_TYPE_ALIYUN,
                AliyunProperties.REST_SIGNING_NAME,
                "odps",
                AliyunProperties.REST_ACCESS_KEY_ID,
                AK,
                AliyunProperties.REST_ACCESS_KEY_SECRET,
                SK))) {
      assertThat(manager).isInstanceOf(AliyunAuthManager.class);
    }
  }

  @ParameterizedTest
  @MethodSource("missingCredentialCases")
  void missingCredential(Map<String, String> properties, String missingProperty) {
    try (AuthManager mgr = new AliyunAuthManager("aliyun")) {
      assertThatThrownBy(() -> mgr.catalogSession(null, properties))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(missingProperty);
    }
  }

  @Test
  void v2SignatureAddsAuthorizationAndDate() {
    Map<String, String> properties =
        Map.of(
            AliyunProperties.REST_SIGNING_NAME, "odps",
            AliyunProperties.REST_ACCESS_KEY_ID, AK,
            AliyunProperties.REST_ACCESS_KEY_SECRET, SK);
    try (AuthManager mgr = new AliyunAuthManager("aliyun");
        AuthSession session = mgr.catalogSession(null, properties)) {
      HTTPRequest signed = session.authenticate(getRequest());

      assertThat(signed.headers().firstEntry("Authorization"))
          .isPresent()
          .hasValueSatisfying(h -> assertThat(h.value()).startsWith("ODPS " + AK + ":"));
      assertThat(signed.headers().firstEntry("Date")).isPresent();
    }
  }

  @Test
  void v4SignatureUsesCredentialScope() {
    Map<String, String> properties =
        Map.of(
            AliyunProperties.REST_SIGNING_NAME,
            "odps",
            AliyunProperties.REST_ACCESS_KEY_ID,
            AK,
            AliyunProperties.REST_ACCESS_KEY_SECRET,
            SK,
            AliyunProperties.REST_SIGNING_REGION,
            "cn-hangzhou");
    try (AuthManager mgr = new AliyunAuthManager("aliyun");
        AuthSession session = mgr.catalogSession(null, properties)) {
      HTTPRequest signed = session.authenticate(getRequest());

      String auth = signed.headers().firstEntry("Authorization").orElseThrow().value();
      assertThat(auth)
          .startsWith("ODPS " + AK + "/")
          .contains("/cn-hangzhou/odps/aliyun_v4_request:");
    }
  }

  @Test
  void stsTokenAddsHeaderWithoutAffectingSignature() {
    Map<String, String> baseProps =
        Map.of(
            AliyunProperties.REST_SIGNING_NAME, "odps",
            AliyunProperties.REST_ACCESS_KEY_ID, AK,
            AliyunProperties.REST_ACCESS_KEY_SECRET, SK);
    Map<String, String> stsProps =
        Map.of(
            AliyunProperties.REST_SIGNING_NAME, "odps",
            AliyunProperties.REST_ACCESS_KEY_ID, AK,
            AliyunProperties.REST_ACCESS_KEY_SECRET, SK,
            AliyunProperties.REST_STS_TOKEN, STS);

    HTTPRequest fixedRequest = getRequestWithDate("Thu, 28 May 2026 12:00:00 GMT");

    HTTPRequest withoutSts;
    HTTPRequest withSts;
    try (AuthManager mgr = new AliyunAuthManager("aliyun");
        AuthSession session = mgr.catalogSession(null, baseProps)) {
      withoutSts = session.authenticate(fixedRequest);
    }
    try (AuthManager mgr = new AliyunAuthManager("aliyun");
        AuthSession session = mgr.catalogSession(null, stsProps)) {
      withSts = session.authenticate(fixedRequest);
    }

    // The Authorization signature must be byte-for-byte identical: STS token does not feed into
    // the canonical string, mirroring com.aliyun.odps.account.StsRequestSigner.
    assertThat(withSts.headers().firstEntry("Authorization").orElseThrow().value())
        .isEqualTo(withoutSts.headers().firstEntry("Authorization").orElseThrow().value());

    assertThat(withSts.headers().firstEntry(OdpsAuthProperties.STS_TOKEN_HEADER))
        .isPresent()
        .hasValueSatisfying(h -> assertThat(h.value()).isEqualTo(STS));

    assertThat(withoutSts.headers().firstEntry(OdpsAuthProperties.STS_TOKEN_HEADER)).isEmpty();
  }

  @Test
  void duplicateHeadersWithSameValueAreCollapsed() {
    Map<String, String> properties =
        Map.of(
            AliyunProperties.REST_SIGNING_NAME, "odps",
            AliyunProperties.REST_ACCESS_KEY_ID, AK,
            AliyunProperties.REST_ACCESS_KEY_SECRET, SK);
    HTTPRequest request =
        ImmutableHTTPRequest.builder()
            .baseUri(URI.create("https://service.cn-hangzhou.maxcompute.aliyun.com"))
            .method(HTTPRequest.HTTPMethod.GET)
            .path("api/v1/namespaces/test/tables/foo")
            .headers(
                HTTPHeaders.of(
                    HTTPHeader.of("Content-Type", "application/json"),
                    HTTPHeader.of("x-odps-user-agent", "iceberg-test"),
                    HTTPHeader.of("X-ODPS-USER-AGENT", "iceberg-test"),
                    HTTPHeader.of("date", "Thu, 28 May 2026 12:00:00 GMT")))
            .queryParameters(Map.of())
            .build();

    try (AuthManager mgr = new AliyunAuthManager("aliyun");
        AuthSession session = mgr.catalogSession(null, properties)) {
      HTTPRequest signed = session.authenticate(request);

      assertThat(signed.headers().entries("x-odps-user-agent")).hasSize(1);
      assertThat(signed.headers().entries("Date"))
          .singleElement()
          .satisfies(h -> assertThat(h.value()).isEqualTo("Thu, 28 May 2026 12:00:00 GMT"));
    }
  }

  @Test
  void duplicateHeadersWithDifferentValuesAreRejected() {
    Map<String, String> properties =
        Map.of(
            AliyunProperties.REST_SIGNING_NAME, "odps",
            AliyunProperties.REST_ACCESS_KEY_ID, AK,
            AliyunProperties.REST_ACCESS_KEY_SECRET, SK);
    HTTPRequest request =
        ImmutableHTTPRequest.builder()
            .baseUri(URI.create("https://service.cn-hangzhou.maxcompute.aliyun.com"))
            .method(HTTPRequest.HTTPMethod.GET)
            .path("api/v1/namespaces/test/tables/foo")
            .headers(
                HTTPHeaders.of(
                    HTTPHeader.of("Content-Type", "application/json"),
                    HTTPHeader.of("x-odps-user-agent", "iceberg-test"),
                    HTTPHeader.of("X-ODPS-USER-AGENT", "other-test")))
            .queryParameters(Map.of())
            .build();

    try (AuthManager mgr = new AliyunAuthManager("aliyun");
        AuthSession session = mgr.catalogSession(null, properties)) {
      assertThatThrownBy(() -> session.authenticate(request))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("ODPS auth does not support multiple values for header");
    }
  }

  @Test
  void pathWithSpaceUsesOdpsSdkResourceDecoding() {
    OdpsRequestSigner signer =
        new OdpsRequestSigner(new AliyunProperties(credentialProperties()), credentialProperties());
    HTTPRequest signed =
        signer.sign(getRequestWithPathAndDate("api/v1/namespaces/a+b/tables/t", FIXED_DATE));

    String expected =
        signer.authorization(
            "GET",
            "/api/v1/namespaces/a b/tables/t",
            Map.of("Content-Type", "application/json", "Date", FIXED_DATE),
            Map.of());
    assertThat(signed.headers().firstEntry("Authorization").orElseThrow().value())
        .isEqualTo(expected);
  }

  @Test
  void pathWithEncodedPlusPreservesLiteralPlus() {
    OdpsRequestSigner signer =
        new OdpsRequestSigner(new AliyunProperties(credentialProperties()), credentialProperties());
    HTTPRequest signed =
        signer.sign(getRequestWithPathAndDate("api/v1/namespaces/a%2Bb/tables/t", FIXED_DATE));

    String expected =
        signer.authorization(
            "GET",
            "/api/v1/namespaces/a+b/tables/t",
            Map.of("Content-Type", "application/json", "Date", FIXED_DATE),
            Map.of());
    assertThat(signed.headers().firstEntry("Authorization").orElseThrow().value())
        .isEqualTo(expected);
  }

  @Test
  void canonicalStringMatchesOdpsContract() throws Exception {
    Map<String, String> headers =
        Map.of(
            "Content-Type", "application/json",
            "Date", "Thu, 28 May 2026 12:00:00 GMT",
            "x-odps-user-agent", "iceberg-test");
    Map<String, String> params = Map.of("maxResults", "10");

    String canonical =
        OdpsRequestSigner.canonicalString("GET", "/api/v1/namespaces/ns/tables/t", headers, params);

    String expected =
        "GET\n"
            + "\n"
            + "application/json\n"
            + "Thu, 28 May 2026 12:00:00 GMT\n"
            + "x-odps-user-agent:iceberg-test\n"
            + "/api/v1/namespaces/ns/tables/t?maxResults=10";
    assertThat(canonical).isEqualTo(expected);

    OdpsRequestSigner signer =
        new OdpsRequestSigner(
            new AliyunProperties(
                Map.of(
                    AliyunProperties.REST_ACCESS_KEY_ID, AK,
                    AliyunProperties.REST_ACCESS_KEY_SECRET, SK)),
            Map.of());
    String authorization =
        signer.authorization("GET", "/api/v1/namespaces/ns/tables/t", headers, params);

    Mac mac = Mac.getInstance("HmacSHA1");
    mac.init(new SecretKeySpec(SK.getBytes(), "HmacSHA1"));
    String expectedSig =
        java.util.Base64.getEncoder().encodeToString(mac.doFinal(expected.getBytes())).trim();
    assertThat(authorization).isEqualTo("ODPS " + AK + ":" + expectedSig);
  }

  private static Stream<Arguments> missingCredentialCases() {
    return Stream.of(
        Arguments.of(
            Map.of(AliyunProperties.REST_SIGNING_NAME, "odps"), "accessKeyId should not be empty"),
        Arguments.of(
            Map.of(
                AliyunProperties.REST_SIGNING_NAME,
                "odps",
                AliyunProperties.REST_ACCESS_KEY_ID,
                AK),
            "accessKeySecret should not be empty"));
  }

  private static Map<String, String> credentialProperties() {
    return Map.of(
        AliyunProperties.REST_SIGNING_NAME,
        "odps",
        AliyunProperties.REST_ACCESS_KEY_ID,
        AK,
        AliyunProperties.REST_ACCESS_KEY_SECRET,
        SK);
  }

  private static HTTPRequest getRequest() {
    return ImmutableHTTPRequest.builder()
        .baseUri(URI.create("https://service.cn-hangzhou.maxcompute.aliyun.com"))
        .method(HTTPRequest.HTTPMethod.GET)
        .path("api/v1/namespaces/test/tables/foo")
        .headers(HTTPHeaders.of(HTTPHeader.of("Content-Type", "application/json")))
        .queryParameters(Map.of())
        .build();
  }

  private static HTTPRequest getRequestWithDate(String date) {
    return getRequestWithPathAndDate("api/v1/namespaces/test/tables/foo", date);
  }

  private static HTTPRequest getRequestWithPathAndDate(String path, String date) {
    return ImmutableHTTPRequest.builder()
        .baseUri(URI.create("https://service.cn-hangzhou.maxcompute.aliyun.com"))
        .method(HTTPRequest.HTTPMethod.GET)
        .path(path)
        .headers(
            HTTPHeaders.of(
                HTTPHeader.of("Content-Type", "application/json"), HTTPHeader.of("Date", date)))
        .queryParameters(Map.of())
        .build();
  }
}
