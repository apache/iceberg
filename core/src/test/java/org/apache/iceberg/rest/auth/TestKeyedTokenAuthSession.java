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
package org.apache.iceberg.rest.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.HTTPHeaders.HTTPHeader;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TestKeyedTokenAuthSession {
  private static final HTTPRequest REQUEST =
      ImmutableHTTPRequest.builder()
          .method(HTTPMethod.GET)
          .baseUri(URI.create("https://localhost"))
          .path("path")
          .build();

  private final String key = UUID.randomUUID().toString();
  private final Map<String, IssuedToken> delivered = Maps.newConcurrentMap();
  private final TokenResolver resolver =
      resolvedKey -> {
        IssuedToken token = delivered.get(resolvedKey);
        if (token == null) {
          throw new NotAuthorizedException("No token delivered for key: %s", resolvedKey);
        }

        return token;
      };

  @AfterEach
  void after() {
    KeyedTokenAuthSession.uninstallResolver();
  }

  private static TokenSource fixed(String token) {
    return () -> IssuedToken.of(token, null);
  }

  private static String authorization(AuthSession session) {
    return session
        .authenticate(REQUEST)
        .headers()
        .firstEntry("Authorization")
        .map(HTTPHeader::value)
        .orElse(null);
  }

  @Test
  void presentsResolvedToken() {
    KeyedTokenAuthSession.installResolver(resolver);
    KeyedTokenAuthSession session = KeyedTokenAuthSession.of(key);
    delivered.put(key, IssuedToken.of("first", null));
    assertThat(authorization(session)).isEqualTo("Bearer first");

    delivered.put(key, IssuedToken.of("second", null));
    assertThat(authorization(session)).isEqualTo("Bearer second");
  }

  @Test
  void releasedKeyFailsClosed() {
    KeyedTokenAuthSession.installResolver(resolver);
    delivered.put(key, IssuedToken.of("delivered", null));
    delivered.remove(key);

    assertThatThrownBy(() -> KeyedTokenAuthSession.of(key).authenticate(REQUEST))
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessage("No token for auth session: %s", key)
        .cause()
        .hasMessage("No token delivered for key: %s", key);
  }

  @Test
  void missingResolverFailsClosed() {
    assertThatThrownBy(() -> KeyedTokenAuthSession.of(key).authenticate(REQUEST))
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessage("No token resolver for auth session: %s", key)
        .hasNoCause();
  }

  @Test
  void resolverIsInstalledOnce() {
    KeyedTokenAuthSession.installResolver(resolver);
    KeyedTokenAuthSession.installResolver(resolver);

    assertThatThrownBy(() -> KeyedTokenAuthSession.installResolver(resolvedKey -> null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot install token resolver: a different resolver is installed");
  }

  @Test
  void expiredTokenFailsClosed() {
    KeyedTokenAuthSession.installResolver(resolver);
    delivered.put(key, IssuedToken.of("expired", System.currentTimeMillis() - 1_000));

    assertThatThrownBy(() -> KeyedTokenAuthSession.of(key).authenticate(REQUEST))
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessage("Expired token for auth session: %s", key)
        .hasNoCause();
  }

  @Test
  void nullTokenFailsClosed() {
    KeyedTokenAuthSession.installResolver(resolvedKey -> null);

    assertThatThrownBy(() -> KeyedTokenAuthSession.of(key).authenticate(REQUEST))
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessage("No token for auth session: %s", key)
        .hasNoCause();
  }

  @Test
  void sourceFailureIsKeptAsCause() {
    NotAuthorizedException failure = new NotAuthorizedException("Subject token rejected");
    KeyedTokenAuthSession session =
        KeyedTokenAuthSession.of(
            key,
            () -> {
              throw failure;
            });

    assertThatThrownBy(() -> session.authenticate(REQUEST))
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessage("No token for auth session: %s", key)
        .hasCause(failure);
    assertThatThrownBy(session::currentToken)
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessage("No token for auth session: %s", key)
        .hasCause(failure);
  }

  @Test
  void sourceTakesPrecedenceOverResolver() {
    KeyedTokenAuthSession.installResolver(resolver);
    delivered.put(key, IssuedToken.of("resolved", null));

    assertThat(authorization(KeyedTokenAuthSession.of(key, fixed("source"))))
        .isEqualTo("Bearer source");
  }

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  void deserializedCopyPresentsResolvedToken(
      TestHelpers.RoundTripSerializer<KeyedTokenAuthSession> serializer)
      throws IOException, ClassNotFoundException {
    KeyedTokenAuthSession.installResolver(resolver);
    delivered.put(key, IssuedToken.of("resolved", null));

    KeyedTokenAuthSession copy = serializer.apply(KeyedTokenAuthSession.of(key, fixed("source")));

    assertThat(copy.key()).isEqualTo(key);
    assertThat(authorization(copy)).isEqualTo("Bearer resolved");
  }
}
