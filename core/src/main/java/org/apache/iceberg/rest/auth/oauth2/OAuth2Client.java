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
package org.apache.iceberg.rest.auth.oauth2;

import com.nimbusds.oauth2.sdk.token.AccessToken;
import java.io.Closeable;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fetches an access token when created and renews it before it expires by running the client
 * credentials grant again on the {@linkplain OAuth2Runtime#executor() runtime executor}.
 *
 * <p>Token requests are retried with exponential backoff on transient failures (network errors,
 * HTTP 429 and 5xx responses). If a renewal fails for good, the current token is kept and another
 * renewal is attempted later.
 */
final class OAuth2Client implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(OAuth2Client.class);

  private static final int TOKEN_REQUEST_RETRIES = 5;
  private static final long TOKEN_REQUEST_MIN_RETRY_WAIT_MS = 100;
  private static final long TOKEN_REQUEST_MAX_RETRY_WAIT_MS = TimeUnit.SECONDS.toMillis(10);
  private static final long TOKEN_REQUEST_TOTAL_RETRY_TIME_MS = TimeUnit.MINUTES.toMillis(1);
  private static final int HTTP_TOO_MANY_REQUESTS = 429;
  private static final int HTTP_SERVER_ERROR = 500;

  private static final Duration MIN_RENEWAL_DELAY = Duration.ofSeconds(1);
  private static final Duration FAILED_RENEWAL_DELAY = Duration.ofSeconds(10);

  private final OAuth2Config config;
  private final ScheduledExecutorService executor;
  private final Clock clock;
  private final ClientCredentialsFlow flow;

  private volatile boolean closed;
  private volatile TokensResult currentTokens;
  private volatile ScheduledFuture<?> renewalFuture;

  /**
   * Creates a client and fetches its first access token synchronously.
   *
   * @throws OAuth2Exception if the authorization server rejected the token request
   */
  OAuth2Client(OAuth2Config config, OAuth2Runtime runtime) {
    this.config = config;
    this.executor = runtime.executor();
    this.clock = runtime.clock();
    this.flow = new ClientCredentialsFlow(config, runtime);
    this.currentTokens = fetchTokens();
    scheduleRenewal(nextRenewalDelay(currentTokens));
  }

  /**
   * Returns the current access token.
   *
   * @throws IllegalStateException if the client is closed
   */
  AccessToken authenticate() {
    return currentTokens().accessToken();
  }

  /** Cancels any scheduled renewal and discards the current token. This method is idempotent. */
  @Override
  public synchronized void close() {
    this.closed = true;
    ScheduledFuture<?> renewal = renewalFuture;
    if (renewal != null) {
      renewal.cancel(true);
    }

    this.renewalFuture = null;
    this.currentTokens = null;
  }

  @VisibleForTesting
  TokensResult currentTokens() {
    TokensResult tokens = currentTokens;
    Preconditions.checkState(tokens != null, "OAuth2 client is closed");
    return tokens;
  }

  /**
   * Fetches a new access token and makes it current. If the request fails, the current token is
   * kept and another renewal is scheduled. This method is normally executed as a scheduled task.
   */
  @VisibleForTesting
  void renewTokens() {
    if (closed) {
      return;
    }

    TokensResult newTokens;
    try {
      newTokens = fetchTokens();
    } catch (RuntimeException e) {
      LOG.warn("Failed to renew access token, keeping the current token", e);
      scheduleRenewal(FAILED_RENEWAL_DELAY);
      return;
    }

    synchronized (this) {
      if (!closed) {
        this.currentTokens = newTokens;
        scheduleRenewal(nextRenewalDelay(newTokens));
      }
    }
  }

  private TokensResult fetchTokens() {
    AtomicReference<TokensResult> result = new AtomicReference<>();
    Tasks.foreach(result)
        .retry(TOKEN_REQUEST_RETRIES)
        .exponentialBackoff(
            TOKEN_REQUEST_MIN_RETRY_WAIT_MS,
            TOKEN_REQUEST_MAX_RETRY_WAIT_MS,
            TOKEN_REQUEST_TOTAL_RETRY_TIME_MS,
            2.0 /* exponential */)
        .shouldRetryTest(OAuth2Client::isTransient)
        .throwFailureWhenFinished()
        .run(holder -> holder.set(flow.fetchTokens()));
    TokensResult tokens = result.get();
    LOG.debug("Fetched access token expiring at {}", tokens.expirationTime());
    return tokens;
  }

  /** Network errors, rate limiting and server errors are transient; other OAuth2 errors are not. */
  private static boolean isTransient(Exception error) {
    if (error instanceof OAuth2Exception) {
      int statusCode = ((OAuth2Exception) error).statusCode();
      return statusCode == HTTP_TOO_MANY_REQUESTS || statusCode >= HTTP_SERVER_ERROR;
    }

    return error instanceof UncheckedIOException;
  }

  private synchronized void scheduleRenewal(Duration delay) {
    if (closed) {
      return;
    }

    LOG.debug("Scheduling token renewal in {}", delay);
    ScheduledFuture<?> renewal;
    try {
      renewal = executor.schedule(this::renewTokens, delay.toMillis(), TimeUnit.MILLISECONDS);
    } catch (RejectedExecutionException e) {
      // the executor is shutting down (typically at JVM exit): the current token remains usable
      // until it expires
      LOG.debug("Token renewal could not be scheduled, executor is shut down", e);
      return;
    }

    this.renewalFuture = renewal;
  }

  private Duration nextRenewalDelay(TokensResult tokens) {
    Duration delay =
        Duration.between(clock.instant(), expirationTime(tokens)).minus(config.prefetch());
    Duration jitter = config.jitter();
    if (!jitter.isZero()) {
      delay = delay.minusMillis(ThreadLocalRandom.current().nextLong(jitter.toMillis()));
    }

    return delay.compareTo(MIN_RENEWAL_DELAY) < 0 ? MIN_RENEWAL_DELAY : delay;
  }

  /**
   * Returns the access token expiration time as reported by the server, or the time the token was
   * received plus the configured access token lifespan if the server did not report one.
   */
  private Instant expirationTime(TokensResult tokens) {
    Instant expirationTime = tokens.expirationTime();
    return expirationTime != null
        ? expirationTime
        : tokens.receivedAt().plus(config.accessTokenLifespan());
  }
}
