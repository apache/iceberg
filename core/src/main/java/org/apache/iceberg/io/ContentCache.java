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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.function.Function;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that provides file-content caching during reading.
 *
 * <p>The file-content caching is initiated by calling {@link ContentCache#tryCache(FileIO, String,
 * long)}. Given a FileIO, a file location string, and file length that is within allowed limit,
 * ContentCache will return a {@link CachingInputFile} that is backed by the cache. Calling {@link
 * CachingInputFile#newStream()} will return a {@link ByteBufferInputStream} backed by list of
 * {@link ByteBuffer} from the cache if such file-content exist in the cache. If the file-content
 * does not exist in the cache yet, a regular InputFile will be instantiated, read-ahead, and loaded
 * into the cache before returning ByteBufferInputStream. The regular InputFile is also used as a
 * fallback if cache loading fail.
 */
public class ContentCache {
  private static final Logger LOG = LoggerFactory.getLogger(ContentCache.class);
  private static final int BUFFER_CHUNK_SIZE = 4 * 1024 * 1024; // 4MB

  private final long expireAfterAccessMs;
  private final long maxTotalBytes;
  private final long maxContentLength;
  private final Cache<String, CacheEntry> cache;

  /**
   * Constructor for ContentCache class.
   *
   * @param expireAfterAccessMs controls the duration for which entries in the ContentCache are hold
   *     since last access. Must be greater or equal than 0. Setting 0 means cache entries expire
   *     only if it gets evicted due to memory pressure.
   * @param maxTotalBytes controls the maximum total amount of bytes to cache in ContentCache. Must
   *     be greater than 0.
   * @param maxContentLength controls the maximum length of file to be considered for caching. Must
   *     be greater than 0.
   */
  public ContentCache(long expireAfterAccessMs, long maxTotalBytes, long maxContentLength) {
    ValidationException.check(expireAfterAccessMs >= 0, "expireAfterAccessMs is less than 0");
    ValidationException.check(maxTotalBytes > 0, "maxTotalBytes is equal or less than 0");
    ValidationException.check(maxContentLength > 0, "maxContentLength is equal or less than 0");
    this.expireAfterAccessMs = expireAfterAccessMs;
    this.maxTotalBytes = maxTotalBytes;
    this.maxContentLength = maxContentLength;

    Caffeine<Object, Object> builder = Caffeine.newBuilder();
    if (expireAfterAccessMs > 0) {
      builder = builder.expireAfterAccess(Duration.ofMillis(expireAfterAccessMs));
    }

    this.cache =
        builder
            .maximumWeight(maxTotalBytes)
            .weigher(
                (Weigher<String, CacheEntry>)
                    (key, value) -> (int) Math.min(value.length, Integer.MAX_VALUE))
            .softValues()
            .removalListener(
                (location, cacheEntry, cause) ->
                    LOG.debug("Evicted {} from ContentCache ({})", location, cause))
            .recordStats()
            .build();
  }

  public long expireAfterAccess() {
    return expireAfterAccessMs;
  }

  public long maxContentLength() {
    return maxContentLength;
  }

  public long maxTotalBytes() {
    return maxTotalBytes;
  }

  public CacheStats stats() {
    return cache.stats();
  }

  public CacheEntry get(String key, Function<String, CacheEntry> mappingFunction) {
    return cache.get(key, mappingFunction);
  }

  public CacheEntry getIfPresent(String location) {
    return cache.getIfPresent(location);
  }

  /**
   * Try cache the file-content of file in the given location upon stream reading.
   *
   * <p>If length is longer than maximum length allowed by ContentCache, a regular {@link InputFile}
   * and no caching will be done for that file. Otherwise, this method will return a {@link
   * CachingInputFile} that serve file reads backed by ContentCache.
   *
   * @param io a FileIO associated with the location.
   * @param location URL/path of a file accessible by io.
   * @param length the known length of such file.
   * @return a {@link CachingInputFile} if length is within allowed limit. Otherwise, a regular
   *     {@link InputFile} for given location.
   */
  public InputFile tryCache(FileIO io, String location, long length) {
    if (length <= maxContentLength) {
      return new CachingInputFile(this, io, location, length);
    }
    return io.newInputFile(location, length);
  }

  public void invalidate(String key) {
    cache.invalidate(key);
  }

  public void invalidateAll() {
    cache.invalidateAll();
  }

  public void cleanUp() {
    cache.cleanUp();
  }

  public long estimatedCacheSize() {
    return cache.estimatedSize();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("expireAfterAccessMs", expireAfterAccessMs)
        .add("maxContentLength", maxContentLength)
        .add("maxTotalBytes", maxTotalBytes)
        .add("cacheStats", cache.stats())
        .toString();
  }

  private static class CacheEntry {
    private final long length;
    private final List<ByteBuffer> buffers;

    private CacheEntry(long length, List<ByteBuffer> buffers) {
      this.length = length;
      this.buffers = buffers;
    }
  }

  /**
   * A subclass of {@link InputFile} that is backed by a {@link ContentCache}.
   *
   * <p>Calling {@link CachingInputFile#newStream()} will return a {@link ByteBufferInputStream}
   * backed by list of {@link ByteBuffer} from the cache if such file-content exist in the cache. If
   * the file-content does not exist in the cache, a regular InputFile will be instantiated,
   * read-ahead, and loaded into the cache before returning ByteBufferInputStream. The regular
   * InputFile is also used as a fallback if cache loading fail.
   */
  private static class CachingInputFile implements InputFile {
    private final ContentCache contentCache;
    private final FileIO io;
    private final String location;
    private final long length;
    private InputFile fallbackInputFile = null;

    private CachingInputFile(ContentCache cache, FileIO io, String location, long length) {
      this.contentCache = cache;
      this.io = io;
      this.location = location;
      this.length = length;
    }

    private InputFile wrappedInputFile() {
      if (fallbackInputFile == null) {
        fallbackInputFile = io.newInputFile(location, length);
      }
      return fallbackInputFile;
    }

    @Override
    public long getLength() {
      CacheEntry buf = contentCache.getIfPresent(location);
      if (buf != null) {
        return buf.length;
      } else if (fallbackInputFile != null) {
        return fallbackInputFile.getLength();
      } else {
        return length;
      }
    }

    /**
     * Opens a new {@link SeekableInputStream} for the underlying data file, either from cache or
     * from the inner FileIO.
     *
     * <p>If data file is not cached yet, and it can fit in the cache, the file content will be
     * cached first before returning a {@link ByteBufferInputStream}. Otherwise, return a new
     * SeekableInputStream from the inner FIleIO.
     *
     * @return a {@link ByteBufferInputStream} if file exist in the cache or can fit in the cache.
     *     Otherwise, return a new SeekableInputStream from the inner FIleIO.
     */
    @Override
    public SeekableInputStream newStream() {
      try {
        // read from cache if file length is less than or equal to maximum length allowed to
        // cache.
        if (getLength() <= contentCache.maxContentLength()) {
          return cachedStream();
        }

        // fallback to non-caching input stream.
        return wrappedInputFile().newStream();
      } catch (FileNotFoundException e) {
        throw new NotFoundException(
            e, "Failed to open input stream for file %s: %s", location, e.toString());
      } catch (IOException e) {
        throw new UncheckedIOException(
            String.format("Failed to open input stream for file %s: %s", location, e), e);
      }
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public boolean exists() {
      CacheEntry buf = contentCache.getIfPresent(location);
      return buf != null || wrappedInputFile().exists();
    }

    private CacheEntry cacheEntry() {
      long start = System.currentTimeMillis();
      try (SeekableInputStream stream = wrappedInputFile().newStream()) {
        long fileLength = getLength();
        long totalBytesToRead = fileLength;
        List<ByteBuffer> buffers = Lists.newArrayList();

        while (totalBytesToRead > 0) {
          // read the stream in 4MB chunk
          int bytesToRead = (int) Math.min(BUFFER_CHUNK_SIZE, totalBytesToRead);
          byte[] buf = new byte[bytesToRead];
          int bytesRead = IOUtil.readRemaining(stream, buf, 0, bytesToRead);
          totalBytesToRead -= bytesRead;

          if (bytesRead < bytesToRead) {
            // Read less than it should be, possibly hitting EOF. Abandon caching by throwing
            // IOException and let the caller fallback to non-caching input file.
            throw new IOException(
                String.format(
                    "Expected to read %d bytes, but only %d bytes read.",
                    fileLength, fileLength - totalBytesToRead));
          } else {
            buffers.add(ByteBuffer.wrap(buf));
          }
        }

        CacheEntry newEntry = new CacheEntry(fileLength, buffers);
        LOG.debug("cacheEntry took {} ms for {}", (System.currentTimeMillis() - start), location);
        return newEntry;
      } catch (IOException ex) {
        throw new UncheckedIOException(ex);
      }
    }

    private SeekableInputStream cachedStream() throws IOException {
      try {
        CacheEntry entry = contentCache.get(location, k -> cacheEntry());
        Preconditions.checkNotNull(
            entry, "CacheEntry should not be null when there is no RuntimeException occurs");
        LOG.debug("Cache stats: {}", contentCache.stats());
        return ByteBufferInputStream.wrap(entry.buffers);
      } catch (UncheckedIOException ex) {
        throw ex.getCause();
      } catch (RuntimeException ex) {
        throw new IOException("Caught an error while reading from cache", ex);
      }
    }
  }
}
