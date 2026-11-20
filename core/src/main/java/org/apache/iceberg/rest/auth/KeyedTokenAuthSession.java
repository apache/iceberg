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

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.HTTPRequest;

/**
 * An {@link AuthSession} whose serialized form is only its key.
 *
 * <p>The token is presented as a bearer token.
 */
public final class KeyedTokenAuthSession implements AuthSession, TokenSource, Serializable {
  private static final AtomicReference<TokenResolver> RESOLVER = new AtomicReference<>();

  private final String key;
  private final transient TokenSource source;

  private KeyedTokenAuthSession(String key, TokenSource source) {
    Preconditions.checkArgument(key != null && !key.isEmpty(), "Invalid key: null or empty");
    this.key = key;
    this.source = source;
  }

  public static KeyedTokenAuthSession of(String key) {
    return new KeyedTokenAuthSession(key, null);
  }

  public static KeyedTokenAuthSession of(String key, TokenSource source) {
    Preconditions.checkArgument(source != null, "Invalid token source: null");
    return new KeyedTokenAuthSession(key, source);
  }

  public static void installResolver(TokenResolver resolver) {
    Preconditions.checkArgument(resolver != null, "Invalid token resolver: null");
    Preconditions.checkState(
        RESOLVER.compareAndSet(null, resolver) || RESOLVER.get() == resolver,
        "Cannot install token resolver: a different resolver is installed");
  }

  @VisibleForTesting
  static void uninstallResolver() {
    RESOLVER.set(null);
  }

  public String key() {
    return key;
  }

  @Override
  public IssuedToken currentToken() {
    TokenSource tokenSource = resolveTokenSource();
    IssuedToken token;
    try {
      token = tokenSource.currentToken();
    } catch (NotAuthorizedException e) {
      throw new NotAuthorizedException(e, "No token for auth session: %s", key);
    }

    if (token == null) {
      throw new NotAuthorizedException("No token for auth session: %s", key);
    }

    if (token.expiresAtMillis() != null && token.expiresAtMillis() <= System.currentTimeMillis()) {
      throw new NotAuthorizedException("Expired token for auth session: %s", key);
    }

    return token;
  }

  @Override
  public HTTPRequest authenticate(HTTPRequest request) {
    String token = currentToken().value();
    return DefaultAuthSession.of(HTTPHeaders.of(OAuth2Util.authHeaders(token)))
        .authenticate(request);
  }

  private TokenSource resolveTokenSource() {
    if (source != null) {
      return source;
    }

    TokenResolver resolver = RESOLVER.get();
    if (resolver == null) {
      throw new NotAuthorizedException("No token resolver for auth session: %s", key);
    }

    return () -> resolver.token(key);
  }

  @Override
  public void close() {}

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("key", key).toString();
  }
}
