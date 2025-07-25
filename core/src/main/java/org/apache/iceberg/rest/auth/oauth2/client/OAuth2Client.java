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
package org.apache.iceberg.rest.auth.oauth2.client;

import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.nimbusds.oauth2.sdk.token.Tokens;
import java.io.Closeable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Throwables;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Config;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Exception;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Runtime;
import org.apache.iceberg.rest.auth.oauth2.flow.Flow;
import org.apache.iceberg.rest.auth.oauth2.flow.FlowFactory;
import org.apache.iceberg.rest.auth.oauth2.flow.TokensResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An OAuth2 client is responsible for fetching and refreshing tokens, following the configuration
 * provided by an {@link OAuth2Config} object.
 */
public final class OAuth2Client implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(OAuth2Client.class);

  private static final Duration MIN_REFRESH_DELAY = Duration.ofSeconds(1);

  // Internal state
  private final OAuth2Config config;
  private final ScheduledExecutorService executor;
  private final FlowFactory flowFactory;
  private final Clock clock;

  // Lifecycle
  private final AtomicBoolean closed = new AtomicBoolean();

  // Token management & refresh scheduling
  private volatile CompletableFuture<TokensResult> currentTokensFuture;
  private volatile ScheduledFuture<?> tokenRefreshFuture;

  public OAuth2Client(OAuth2Config cfg, OAuth2Runtime runtime) {
    config = cfg;
    executor = runtime.executor();
    clock = runtime.clock();
    flowFactory = FlowFactory.of(cfg, runtime);
    config
        .basicConfig()
        .token()
        .ifPresentOrElse(this::initWithStaticToken, this::initWithDynamicToken);
  }

  /** Copy constructor. Only accessible from the {@link #copy()} method. */
  private OAuth2Client(OAuth2Client toCopy) {
    LOGGER.debug("Copying client");
    config = toCopy.config;
    executor = toCopy.executor;
    flowFactory = toCopy.flowFactory;
    clock = toCopy.clock;
    tokenRefreshFuture = null;
    TokensResult currentTokens = getNow(toCopy.currentTokensFuture);
    currentTokensFuture =
        (currentTokens != null
                ? CompletableFuture.completedFuture(currentTokens)
                : CompletableFuture.supplyAsync(this::fetchNewTokens, executor)
                    .thenCompose(Function.identity()))
            .whenComplete((tokens, error) -> scheduleNextRenewal(tokens));
  }

  /**
   * Authenticates the client synchronously, waiting for the authentication to complete, and returns
   * the current access token. If the authentication fails, or if the client is closed, an exception
   * is thrown.
   *
   * <p><b>Important:</b> This method must not be called from a task running on the same {@link
   * ScheduledExecutorService} used by this client (see {@link OAuth2Runtime#executor()}), as it
   * blocks the calling thread until the token is available. If the token has not been fetched yet,
   * doing so would deadlock the executor. Use {@link #authenticateAsync()} instead when calling
   * from executor tasks.
   */
  public AccessToken authenticate() {
    return authenticateInternal().tokens().getAccessToken();
  }

  /**
   * Authenticates the client asynchronously and returns a future that completes when the
   * authentication completes (either successfully or with an error).
   */
  public CompletionStage<AccessToken> authenticateAsync() {
    return authenticateAsyncInternal()
        .thenApply(TokensResult::tokens)
        .thenApply(Tokens::getAccessToken);
  }

  /**
   * Creates a copy of this client. The copy will share the same config, executor and flow factory
   * as the original client, as well as its current tokens, if any. If token refresh is enabled, the
   * copy will create its own token refresh schedule.
   */
  public OAuth2Client copy() {
    return new OAuth2Client(this);
  }

  /** Closes this client, releasing any resources held by it. This method is idempotent. */
  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      try {
        LOGGER.debug("Closing...");
        ScheduledFuture<?> refreshFuture = tokenRefreshFuture;
        if (refreshFuture != null) {
          refreshFuture.cancel(true);
        }
        CompletableFuture<TokensResult> tokensFuture = currentTokensFuture;
        if (tokensFuture != null) {
          tokensFuture.cancel(true);
        }
      } finally {
        tokenRefreshFuture = null;
        // Don't clear currentTokensFuture, we'll need it in case this client is copied.
        LOGGER.debug("Closed");
      }
    }
  }

  /**
   * Same as {@link #authenticate()} but returns the full {@link TokensResult} object, including the
   * refresh token, if any. Only intended for testing.
   */
  @VisibleForTesting
  TokensResult authenticateInternal() {
    LOGGER.debug("Authenticating synchronously");
    return currentTokens();
  }

  /**
   * Same as {@link #authenticateAsync()} but returns the full {@link TokensResult} object,
   * including the refresh token, if any. Only intended for testing.
   */
  @VisibleForTesting
  CompletionStage<TokensResult> authenticateAsyncInternal() {
    LOGGER.debug("Authenticating asynchronously");
    return currentTokensFuture;
  }

  /** Returns the current tokens, waiting for them to be available if necessary. */
  private TokensResult currentTokens() {
    try {
      // This wait is guaranteed to never block indefinitely,
      // because the current tokens future always completes eventually
      // (either with tokens or with an error – including timeouts).
      return currentTokensFuture.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof TimeoutException) {
        throw new RuntimeException("Token acquisition timed out", cause);
      } else if (cause instanceof Error) {
        throw (Error) cause;
      } else if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      } else {
        throw new RuntimeException("Token acquisition failed", cause);
      }
    }
  }

  /**
   * Initializes the client with a static initial access token. In this situation, a static access
   * token has been provided in the configuration, and it will be used as-is as the first initial
   * token.
   */
  private void initWithStaticToken(AccessToken token) {
    TokensResult currentTokens = TokensResult.of(token, clock);
    currentTokensFuture = CompletableFuture.completedFuture(currentTokens);
    if (refreshWithTokenExchange()) {
      // A static token can only be refreshed if using the token exchange grant.
      scheduleNextRenewal(currentTokens);
    }
  }

  /** Initializes the client with a dynamically-obtained initial access token. */
  private void initWithDynamicToken() {
    currentTokensFuture =
        CompletableFuture.supplyAsync(this::fetchNewTokens, executor)
            .thenCompose(Function.identity())
            .whenComplete(this::logRenewal)
            .whenComplete((tokens, error) -> scheduleNextRenewal(tokens));
  }

  /**
   * Executes a token renewal operation, either by refreshing the current access token (if any) or
   * by fetching a new access token.
   *
   * <p>This method rotates the current tokens future; the new future is completed when the renewal
   * operation completes and the next one is scheduled.
   *
   * <p>This method is always executed as a scheduled task.
   */
  @VisibleForTesting
  @SuppressWarnings("FutureReturnValueIgnored")
  void renewTokens() {
    if (!closed.get()) {
      CompletableFuture<TokensResult> oldTokensFuture = currentTokensFuture;
      TokensResult oldTokens = getNow(oldTokensFuture);
      CompletableFuture<TokensResult> newTokensFuture =
          oldTokensFuture
              // 1) try refreshing the current access token, if any, and if possible
              .thenCompose(this::refreshCurrentTokens)
              // 2) if that fails, try fetching brand-new tokens using the configured initial grant
              .handle(this::handleRefreshResult)
              .thenCompose(Function.identity())
              // 3) Log the result of the token renewal attempt
              .whenComplete(this::logRenewal)
              // 4) if the renewal attempt failed, keep the old tokens if available
              .handle((newTokens, error) -> handleRenewalResult(oldTokens, newTokens, error))
              .thenCompose(Function.identity());
      currentTokensFuture = newTokensFuture;
      // 5) schedule the next token renewal
      newTokensFuture.whenComplete((tokens, error) -> scheduleNextRenewal(tokens));
      if (closed.get()) {
        // We raced with close(): cancel the future we just created.
        newTokensFuture.cancel(true);
      }
    }
  }

  /** Fetches new tokens using the configured initial grant. */
  @VisibleForTesting
  CompletionStage<TokensResult> fetchNewTokens() {
    Flow flow = flowFactory.newInitialFlow();
    LOGGER.debug("Fetching new access token using {}", flow.grantType());
    Duration timeout = config.basicConfig().tokenAcquisitionTimeout();
    return flow.execute()
        .toCompletableFuture()
        .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  /** Refreshes the current tokens using the configured refresh grant. */
  @VisibleForTesting
  CompletionStage<TokensResult> refreshCurrentTokens(TokensResult currentTokens) {
    if (currentTokens.tokens().getRefreshToken() == null) {
      if (!refreshWithTokenExchange()) {
        LOGGER.debug("Must fetch new tokens, refresh token is null");
        return MUST_FETCH_NEW_TOKENS_FUTURE;
      }
    }

    Flow flow = flowFactory.newRefreshFlow(currentTokens.tokens());
    LOGGER.debug("Refreshing tokens using {}", flow.grantType());
    Duration timeout = config.basicConfig().tokenAcquisitionTimeout();
    return flow.execute()
        .toCompletableFuture()
        .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  /**
   * Whether to use the token exchange grant to refresh tokens. If unspecified in the config, the
   * default is to use token exchange only if the initial grant is {@link
   * GrantType#CLIENT_CREDENTIALS}, in order to maintain backward compatibility.
   */
  private boolean refreshWithTokenExchange() {
    return config
        .tokenRefreshConfig()
        .tokenExchangeEnabled()
        .orElseGet(() -> config.basicConfig().grantType().equals(GrantType.CLIENT_CREDENTIALS));
  }

  /**
   * Handles the result of a token refresh attempt. If the refresh wasn't successful, or if the
   * refreshed access token lifespan is too short, the refreshed token is discarded and a new token
   * is fetched.
   *
   * <p>Refreshed access token lifespans can sometimes be too short to be usable, because their
   * lifetime is bound by the refresh token's lifetime, and hence, by the user session expiration
   * time. In such cases, it's better to fetch a new token (and thus, ask the user to
   * re-authenticate) rather than keep using a too short-lived token.
   */
  private CompletionStage<TokensResult> handleRefreshResult(
      @Nullable TokensResult newTokens, @Nullable Throwable error) {
    if (newTokens != null) {
      Instant now = clock.instant();
      Instant expirationTime = expirationTime(newTokens);
      Duration safetyMargin = config.tokenRefreshConfig().safetyMargin();
      if (expirationTime.minus(safetyMargin).isAfter(now)) {
        return CompletableFuture.completedFuture(newTokens);
      } else {
        LOGGER.debug("Refreshed access token is too short: fetching new tokens instead");
      }
    } else if (error != null) {
      Throwable root = Throwables.getRootCause(error);
      if (!(root instanceof MustFetchNewTokensException)) {
        LOGGER.debug("Refresh failed unexpectedly, fetching new tokens instead", error);
      }
    }

    return fetchNewTokens();
  }

  /**
   * Handles the result of a token renewal attempt. If the renewal wasn't successful, the old tokens
   * are kept if available; otherwise, the error is propagated.
   */
  private CompletionStage<TokensResult> handleRenewalResult(
      @Nullable TokensResult oldTokens,
      @Nullable TokensResult newTokens,
      @Nullable Throwable error) {
    return error == null
        ? CompletableFuture.completedFuture(newTokens)
        : oldTokens != null
            ? CompletableFuture.completedFuture(oldTokens)
            : CompletableFuture.failedFuture(error);
  }

  private void scheduleNextRenewal(@Nullable TokensResult currentTokens) {
    if (config.tokenRefreshConfig().enabled() && !closed.get()) {
      Instant now = clock.instant();
      Duration delay = nextRenewal(currentTokens, now);
      LOGGER.debug("Scheduling token renewal in {}", delay);
      ScheduledFuture<?> refreshFuture =
          executor.schedule(this::renewTokens, delay.toMillis(), TimeUnit.MILLISECONDS);
      tokenRefreshFuture = refreshFuture;
      if (closed.get()) {
        // We raced with close(): clear the field and cancel the future we just created.
        tokenRefreshFuture = null;
        refreshFuture.cancel(true);
      }
    }
  }

  /**
   * Determines when the next token renewal should be scheduled, based on the current tokens and the
   * current time.
   */
  private Duration nextRenewal(@Nullable TokensResult currentTokens, Instant now) {
    if (currentTokens == null) {
      return MIN_REFRESH_DELAY;
    }

    Instant expirationTime = expirationTime(currentTokens);
    Duration safetyMargin = config.tokenRefreshConfig().safetyMargin();
    Duration delay = Duration.between(now, expirationTime).minus(safetyMargin);
    if (delay.compareTo(MIN_REFRESH_DELAY) < 0) {
      LOGGER.debug("Next renewal delay was too short: {}", delay);
      delay = MIN_REFRESH_DELAY;
    }

    return delay;
  }

  /**
   * Returns the access token expiration time, based on {@link
   * TokensResult#accessTokenExpirationTime()} if available, or by adding the default access token
   * lifespan to {@link TokensResult#receivedAt()} otherwise.
   */
  private Instant expirationTime(TokensResult currentTokens) {
    return currentTokens
        .accessTokenExpirationTime()
        .orElseGet(
            () ->
                currentTokens.receivedAt().plus(config.tokenRefreshConfig().accessTokenLifespan()));
  }

  private void logRenewal(@Nullable TokensResult newTokens, @Nullable Throwable error) {
    if (!closed.get()) {
      if (newTokens != null) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "Successfully renewed tokens. Access token expiration time: {}",
              newTokens.accessTokenExpirationTime().orElse(null));
        }
      } else if (error != null) {
        Throwable root = Throwables.getRootCause(error);
        if (root instanceof OAuth2Exception) {
          // Don't include the stack trace if the error is an OAuth2Exception,
          // since it's not very useful and just clutters the logs.
          LOGGER.warn("Failed to renew tokens: {}", root.toString());
        } else {
          LOGGER.warn("Failed to renew tokens", root);
        }
      }
    }
  }

  @Nullable
  private static <T> T getNow(@Nullable CompletableFuture<T> future) {
    return future != null && future.isDone() && !future.isCompletedExceptionally()
        ? future.getNow(null)
        : null;
  }

  /**
   * Internal exception used solely to signal that a new token must be fetched. This exception is
   * not propagated to the user and is only used to short-circuit the token refresh logic.
   */
  @VisibleForTesting
  static class MustFetchNewTokensException extends RuntimeException {}

  private static final CompletableFuture<TokensResult> MUST_FETCH_NEW_TOKENS_FUTURE =
      CompletableFuture.failedFuture(new MustFetchNewTokensException());
}
