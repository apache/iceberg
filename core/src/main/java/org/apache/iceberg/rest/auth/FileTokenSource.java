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

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BearerTokenSource} that reads the token from a local file, and re-reads it once the
 * token is older than the configured reload interval.
 *
 * <p>No background thread is used: reloads happen lazily, on the calling thread, when the token is
 * requested. Only one thread reloads at a time; concurrent callers get the current token without
 * waiting.
 *
 * <p>If a reload fails, the last valid token is kept, and the next reload is attempted after a full
 * reload interval. As a consequence, deleting the token file does not revoke the token.
 */
final class FileTokenSource implements BearerTokenSource {

  private static final Logger LOG = LoggerFactory.getLogger(FileTokenSource.class);

  /**
   * Maximum token file size. Large enough for JWTs signed with the largest post-quantum signature
   * schemes.
   */
  static final int MAX_FILE_SIZE = 256 * 1024;

  private final Path path;
  private final Duration reloadInterval;
  private final long reloadIntervalNanos;
  private final LongSupplier nanoTime;
  private final AtomicReference<State> state;

  /**
   * An immutable snapshot of the source's mutable state, swapped atomically.
   *
   * @param token the last valid token
   * @param readAtNanos when the token was last read, or when the last reload attempt failed
   * @param reloading whether a thread has claimed the current reload
   * @param failing whether the last reload attempt failed, used to log on transitions only
   */
  private record State(String token, long readAtNanos, boolean reloading, boolean failing) {}

  FileTokenSource(Path path, Duration reloadInterval) {
    this(path, reloadInterval, System::nanoTime);
  }

  FileTokenSource(Path path, Duration reloadInterval, LongSupplier nanoTime) {
    Preconditions.checkArgument(path != null, "Invalid token file: null");
    Preconditions.checkArgument(reloadInterval != null, "Invalid reload interval: null");
    this.path = path;
    this.reloadInterval = reloadInterval;
    this.reloadIntervalNanos = reloadInterval.toNanos();
    this.nanoTime = nanoTime;
    // the initial read must succeed, otherwise the session fails to initialize
    this.state = new AtomicReference<>(new State(readToken(), nanoTime.getAsLong(), false, false));
  }

  @Override
  public String token() {
    State current = state.get();
    if (current.reloading || isFresh(current)) {
      return current.token;
    }

    State claimed = new State(current.token, current.readAtNanos, true, current.failing);
    if (!state.compareAndSet(current, claimed)) {
      // another thread claimed the reload
      return current.token;
    }

    // only the thread holding the claim gets here, so a plain set() is enough to both publish the
    // new state and release the claim; on unexpected errors, the unclaimed state is restored so
    // that the next call retries again.
    State next = current;
    try {
      next = reload(current);
    } finally {
      state.set(next);
    }

    return next.token;
  }

  private State reload(State current) {
    String token;
    try {
      token = readToken();
    } catch (RuntimeException e) {
      if (!current.failing) {
        LOG.warn(
            "Failed to reload bearer token from {}, will keep using the last valid token", path, e);
      }

      // keep the last valid token and wait a full interval before the next attempt
      return new State(current.token, nanoTime.getAsLong(), false, true);
    }

    if (current.failing) {
      LOG.info("Successfully reloaded bearer token from {}", path);
    }

    return new State(token, nanoTime.getAsLong(), false, false);
  }

  private boolean isFresh(State current) {
    return nanoTime.getAsLong() - current.readAtNanos < reloadIntervalNanos;
  }

  private String readToken() {
    try {
      Preconditions.checkArgument(
          Files.isRegularFile(path),
          "Invalid token file: %s does not exist or is not a file",
          path);
      Preconditions.checkArgument(
          Files.isReadable(path), "Invalid token file: %s is not readable", path);
      Preconditions.checkArgument(
          Files.size(path) <= MAX_FILE_SIZE,
          "Invalid token file: %s is larger than %s bytes",
          path,
          MAX_FILE_SIZE);
      byte[] bytes;
      // Files.newInputStream() follows symlinks by default
      try (InputStream in = Files.newInputStream(path)) {
        // read one byte past the limit, so that a file that grew after the size check is rejected
        // instead of silently truncated
        bytes = in.readNBytes(MAX_FILE_SIZE + 1);
      }

      Preconditions.checkArgument(
          bytes.length <= MAX_FILE_SIZE,
          "Invalid token file: %s is larger than %s bytes",
          path,
          MAX_FILE_SIZE);
      return BearerTokenSource.validate(new String(bytes, StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format("Failed to read token file %s: %s", path, e.getMessage()), e);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("path", path)
        .add("reloadInterval", reloadInterval)
        .toString();
  }
}
