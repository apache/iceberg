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
package org.apache.iceberg.rest;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import javax.net.ssl.SSLException;
import org.apache.hc.client5.http.HttpRequestRetryStrategy;
import org.apache.hc.client5.http.utils.DateUtils;
import org.apache.hc.core5.concurrent.CancellableDependency;
import org.apache.hc.core5.http.ConnectionClosedException;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.util.TimeValue;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

/**
 * Defines an exponential HTTP request retry strategy and provides the same characteristics as the
 * {@link org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy}, using the following list
 * of non-retriable I/O exception classes:
 *
 * <ul>
 *   <li>InterruptedIOException
 *   <li>UnknownHostException
 *   <li>ConnectException
 *   <li>ConnectionClosedException
 *   <li>NoRouteToHostException
 *   <li>SSLException
 * </ul>
 *
 * The following retriable HTTP status codes are defined:
 *
 * <ul>
 *   <li>SC_TOO_MANY_REQUESTS (429)
 *   <li>SC_SERVICE_UNAVAILABLE (503)
 * </ul>
 *
 * Most code and behavior is taken from {@link
 * org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy}, with minor modifications to
 * {@link #getRetryInterval(HttpResponse, int, HttpContext)} to achieve exponential backoff.
 */
class ExponentialHttpRequestRetryStrategy implements HttpRequestRetryStrategy {
  private final int maxRetries;
  private final Set<Class<? extends IOException>> nonRetriableExceptions;
  private final Set<Integer> retriableCodes;

  ExponentialHttpRequestRetryStrategy(int maximumRetries) {
    Preconditions.checkArgument(
        maximumRetries > 0, "Cannot set retries to %s, the value must be positive", maximumRetries);
    this.maxRetries = maximumRetries;
    this.retriableCodes =
        ImmutableSet.of(HttpStatus.SC_TOO_MANY_REQUESTS, HttpStatus.SC_SERVICE_UNAVAILABLE);
    this.nonRetriableExceptions =
        ImmutableSet.of(
            InterruptedIOException.class,
            UnknownHostException.class,
            ConnectException.class,
            ConnectionClosedException.class,
            NoRouteToHostException.class,
            SSLException.class);
  }

  @Override
  public boolean retryRequest(
      HttpRequest request, IOException exception, int execCount, HttpContext context) {
    if (execCount > maxRetries) {
      // Do not retry if over max retries
      return false;
    }

    if (nonRetriableExceptions.contains(exception.getClass())) {
      return false;
    } else {
      for (Class<? extends IOException> rejectException : nonRetriableExceptions) {
        if (rejectException.isInstance(exception)) {
          return false;
        }
      }
    }

    if (request instanceof CancellableDependency
        && ((CancellableDependency) request).isCancelled()) {
      return false;
    }

    // Retry if the request is considered idempotent
    return Method.isIdempotent(request.getMethod());
  }

  @Override
  public boolean retryRequest(HttpResponse response, int execCount, HttpContext context) {
    return execCount <= maxRetries && retriableCodes.contains(response.getCode());
  }

  @Override
  public TimeValue getRetryInterval(HttpResponse response, int execCount, HttpContext context) {
    // a server may send a 429 / 503 with a Retry-After header
    // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After
    Header header = response.getFirstHeader(HttpHeaders.RETRY_AFTER);
    TimeValue retryAfter = null;
    if (header != null) {
      String value = header.getValue();
      try {
        retryAfter = TimeValue.ofSeconds(Long.parseLong(value));
      } catch (NumberFormatException ignore) {
        Instant retryAfterDate = DateUtils.parseStandardDate(value);
        if (retryAfterDate != null) {
          retryAfter =
              TimeValue.ofMilliseconds(retryAfterDate.toEpochMilli() - System.currentTimeMillis());
        }
      }

      if (TimeValue.isPositive(retryAfter)) {
        return retryAfter;
      }
    }

    int delayMillis = 1000 * (int) Math.min(Math.pow(2.0, (long) execCount - 1), 64.0);
    int jitter = ThreadLocalRandom.current().nextInt(Math.max(1, (int) (delayMillis * 0.1)));

    return TimeValue.ofMilliseconds(delayMillis + jitter);
  }
}
