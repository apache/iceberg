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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestFileTokenSource {

  private static final Duration INTERVAL = Duration.ofMinutes(1);

  @TempDir private Path tempDir;

  private final AtomicLong nanos = new AtomicLong();
  private Path tokenFile;

  @BeforeEach
  void before() {
    this.tokenFile = tempDir.resolve("token");
  }

  @Test
  void initialRead() throws IOException {
    write(tokenFile, "token1\n");
    assertThat(newSource().token()).isEqualTo("token1");
  }

  @Test
  void missingFile() {
    assertThatThrownBy(this::newSource)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid token file: %s does not exist or is not a file", tokenFile);
  }

  @Test
  void directory() {
    assertThatThrownBy(() -> new FileTokenSource(tempDir, INTERVAL, nanos::get))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid token file: %s does not exist or is not a file", tempDir);
  }

  @Test
  void emptyFile() throws IOException {
    write(tokenFile, "\n");
    assertThatThrownBy(this::newSource)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid bearer token: empty");
  }

  @Test
  void invalidContents() throws IOException {
    write(tokenFile, "{\"token\": \"s3cr3t\"}");
    assertThatThrownBy(this::newSource)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid bearer token: contains characters not allowed by RFC 6750");
  }

  @Test
  void invalidUtf8() throws IOException {
    Files.write(tokenFile, new byte[] {'t', 'o', (byte) 0xC3, 'k'});
    assertThatThrownBy(this::newSource)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid bearer token: contains characters not allowed by RFC 6750");
  }

  @Test
  void reloadAfterInterval() throws IOException {
    write(tokenFile, "token1");
    FileTokenSource source = newSource();
    assertThat(source.token()).isEqualTo("token1");
    write(tokenFile, "token2");
    advance(INTERVAL);
    assertThat(source.token()).isEqualTo("token2");
    write(tokenFile, "token3");
    advance(INTERVAL.minusNanos(1));
    assertThat(source.token()).isEqualTo("token2");
    advance(Duration.ofNanos(1));
    assertThat(source.token()).isEqualTo("token3");
  }

  @Test
  void reloadFailureKeepsLastToken() throws IOException {
    write(tokenFile, "token1");
    FileTokenSource source = newSource();

    // invalid contents
    write(tokenFile, "");
    advance(INTERVAL);
    assertThat(source.token()).isEqualTo("token1");

    // the failure resets the read time: the next attempt waits a full interval
    write(tokenFile, "token2");
    advance(INTERVAL.minusNanos(1));
    assertThat(source.token()).isEqualTo("token1");
    advance(Duration.ofNanos(1));
    assertThat(source.token()).isEqualTo("token2");
  }

  @Test
  void deletedFileKeepsLastToken() throws IOException {
    write(tokenFile, "token1");
    FileTokenSource source = newSource();
    assertThat(source.token()).isEqualTo("token1");
    Files.delete(tokenFile);
    advance(INTERVAL);
    assertThat(source.token()).isEqualTo("token1");
    write(tokenFile, "token2");
    advance(INTERVAL);
    assertThat(source.token()).isEqualTo("token2");
  }

  @Test
  void followsSymlinks() throws IOException {
    // mimics the Kubernetes atomic writer: a symlink to a timestamped directory, swapped atomically
    Path dir1 = Files.createDirectory(tempDir.resolve("..2026_10_27_00_00_00.1"));
    Path dir2 = Files.createDirectory(tempDir.resolve("..2026_10_27_01_00_00.2"));
    write(dir1.resolve("token"), "token1");
    write(dir2.resolve("token"), "token2");
    Path data = tempDir.resolve("..data");
    Files.createSymbolicLink(data, dir1.getFileName());
    Files.createSymbolicLink(tokenFile, data.getFileName().resolve("token"));

    FileTokenSource source = newSource();
    assertThat(source.token()).isEqualTo("token1");

    Path tmp = tempDir.resolve("..data_tmp");
    Files.createSymbolicLink(tmp, dir2.getFileName());
    Files.move(tmp, data, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    advance(INTERVAL);
    assertThat(source.token()).isEqualTo("token2");
  }

  @Test
  void concurrentAccess() throws Exception {
    write(tokenFile, "token1");
    FileTokenSource source = newSource();
    assertThat(source.token()).isEqualTo("token1");

    // The next access will trigger the reload
    write(tokenFile, "token2");
    advance(INTERVAL);

    int numThreads = 8;

    List<Callable<String>> tasks = Lists.newArrayList();
    for (int i = 0; i < numThreads; i++) {
      tasks.add(source::token);
    }

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    try {
      for (Future<String> future : executor.invokeAll(tasks)) {
        // callers that lost the race to reload get the previous token
        assertThat(future.get()).isIn("token1", "token2");
      }
    } finally {
      executor.shutdownNow();
    }

    assertThat(source.token()).isEqualTo("token2");
  }

  @Test
  void toStringDoesNotContainToken() throws IOException {
    write(tokenFile, "s3cr3t");
    assertThat(newSource().toString()).contains(tokenFile.toString()).doesNotContain("s3cr3t");
  }

  private FileTokenSource newSource() {
    return new FileTokenSource(tokenFile, INTERVAL, nanos::get);
  }

  private void advance(Duration duration) {
    nanos.addAndGet(duration.toNanos());
  }

  private static void write(Path path, String contents) throws IOException {
    Files.writeString(path, contents, StandardCharsets.UTF_8);
  }
}
