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
package org.apache.iceberg.io;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import dev.failsafe.RetryPolicyBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;
import javax.net.ssl.SSLException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * RetryableInputStream wraps over an underlying InputStream and retries failures encountered when
 * reading through the stream. On retries, the underlying streams will be reinitialized.
 */
public class RetryableInputStream extends InputStream {

  private final InputStream underlyingStream;
  private final RetryPolicy<Object> retryPolicy;

  private RetryableInputStream(InputStream underlyingStream, RetryPolicy<Object> retryPolicy) {
    this.underlyingStream = underlyingStream;
    this.retryPolicy = retryPolicy;
  }

  @Override
  public int read() throws IOException {
    return Failsafe.with(retryPolicy).get(() -> underlyingStream.read());
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return Failsafe.with(retryPolicy).get(() -> underlyingStream.read(b, off, len));
  }

  @Override
  public void close() throws IOException {
    underlyingStream.close();
  }

  public static RetryableInputStream.Builder builderFor(Supplier<InputStream> newStreamSupplier) {
    return new Builder(newStreamSupplier);
  }

  public static class Builder {

    private InputStream underlyingStream;
    private final Supplier<InputStream> newStreamProvider;
    private List<Class<? extends Exception>> retryableExceptions =
        ImmutableList.of(SSLException.class, SocketTimeoutException.class);
    private int numRetries = 3;

    private long durationMs = 500;

    private Builder(Supplier<InputStream> newStreamProvider) {
      this.underlyingStream = newStreamProvider.get();
      this.newStreamProvider = newStreamProvider;
    }

    public Builder retryOn(Class<? extends Exception>... exceptions) {
      this.retryableExceptions = Lists.newArrayList(exceptions);
      return this;
    }

    public Builder withRetries(int numRetries) {
      this.numRetries = numRetries;
      return this;
    }

    public Builder withRetryDelay(long durationMs) {
      this.durationMs = durationMs;
      return this;
    }

    public RetryableInputStream build() {
      RetryPolicyBuilder<Object> retryPolicyBuilder = RetryPolicy.builder();
      retryableExceptions.forEach(retryPolicyBuilder::handle);
      retryPolicyBuilder.onRetry(
          (event) -> {
            this.underlyingStream = newStreamProvider.get();
          });
      return new RetryableInputStream(
          underlyingStream,
          retryPolicyBuilder
              .withMaxRetries(numRetries)
              .withDelay(Duration.ofMillis(durationMs))
              .build());
    }
  }
}
