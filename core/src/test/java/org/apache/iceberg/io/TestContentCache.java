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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.junit.jupiter.api.Test;

public class TestContentCache {
  private static final byte[] CONTENT =
      "manifest-list file content".getBytes(StandardCharsets.UTF_8);

  @Test
  public void testTryCacheSkipsLengthCheckWhenContentIsCached() throws IOException {
    ContentCache cache = new ContentCache(0, 1024 * 1024, 1024);
    InMemoryInputFile file = new InMemoryInputFile("memory:/cached-file", CONTENT);

    assertThat(readFully(cache.tryCache(file))).isEqualTo(CONTENT);
    assertThat(cache.estimatedCacheSize()).isEqualTo(1);

    long hits = cache.stats().hitCount();
    long misses = cache.stats().missCount();

    // a cached location must be wrapped without resolving the input's length
    InputFile cached = cache.tryCache(new NoLengthInputFile(file));

    // the cache probe must not record stats
    assertThat(cache.stats().hitCount()).isEqualTo(hits);
    assertThat(cache.stats().missCount()).isEqualTo(misses);

    // reads are served from the cache without calling the underlying input
    assertThat(readFully(cached)).isEqualTo(CONTENT);
    assertThat(cache.stats().hitCount()).isEqualTo(hits + 1);
    assertThat(cache.stats().missCount()).isEqualTo(misses);
    assertThat(cached.getLength()).isEqualTo(CONTENT.length);
  }

  @Test
  public void testTryCacheResolvesLengthForUncachedLocation() {
    ContentCache cache = new ContentCache(0, 1024 * 1024, 1024);
    InMemoryInputFile file = new InMemoryInputFile("memory:/uncached-file", CONTENT);

    assertThatThrownBy(() -> cache.tryCache(new NoLengthInputFile(file)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("getLength should not be called");
  }

  @Test
  public void testTryCacheDoesNotWrapOversizedFile() throws IOException {
    ContentCache cache = new ContentCache(0, 1024 * 1024, CONTENT.length - 1);
    InMemoryInputFile file = new InMemoryInputFile("memory:/oversized-file", CONTENT);

    InputFile result = cache.tryCache(file);
    assertThat(result).isSameAs(file);
    assertThat(readFully(result)).isEqualTo(CONTENT);
    assertThat(cache.estimatedCacheSize()).isZero();
  }

  private static byte[] readFully(InputFile file) throws IOException {
    try (SeekableInputStream stream = file.newStream()) {
      return ByteStreams.toByteArray(stream);
    }
  }

  /** An InputFile that does not know its length locally, like a manifest-list location. */
  private static class NoLengthInputFile implements InputFile {
    private final InputFile delegate;

    NoLengthInputFile(InputFile delegate) {
      this.delegate = delegate;
    }

    @Override
    public long getLength() {
      throw new UnsupportedOperationException("getLength should not be called");
    }

    @Override
    public SeekableInputStream newStream() {
      return delegate.newStream();
    }

    @Override
    public String location() {
      return delegate.location();
    }

    @Override
    public boolean exists() {
      return delegate.exists();
    }
  }
}
