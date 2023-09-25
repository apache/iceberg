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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.iceberg.Files;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.relocated.com.google.common.hash.Hashing;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SerializableMap;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link FileIO} implementation that caches files when an {@link InputFile} is created. */
public class AsyncFileIO extends ResolvingFileIO {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncFileIO.class);

  private static final String FILE_PREFIX = "file:";
  private static final String ASYNC_ENABLED = "async.enabled";
  private static final String CACHE_LOCATION = "async.cache-location";

  // Using a holder instead of double-checked locking
  // see https://stackoverflow.com/questions/6189099/
  private static final class SharedPools {
    static final ExecutorService DOWNLOAD_POOL = ThreadPools.newWorkerPool("async-file-io");
  }

  private SerializableMap<String, String> properties = null;
  private FileIO local = null;
  private boolean enabled = true;
  private String cacheLocation = null;

  private transient LoadingCache<String, InputFile> lazyCache = null;

  // detect when this has been serialized using a transient variable
  private transient Boolean isRemote = false;

  @Override
  public InputFile newInputFile(String path) {
    if (enabled /*&& null == isRemote*/) {
      return cache().get(path);
    }

    return super.newInputFile(path);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return super.newOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    super.deleteFile(path);
  }

  @Override
  public Map<String, String> properties() {
    return properties.immutableMap();
  }

  @Override
  public void initialize(Map<String, String> props) {
    super.initialize(props);

    this.properties = SerializableMap.copyOf(props);
    this.enabled = PropertyUtil.propertyAsBoolean(props, ASYNC_ENABLED, true);

    String location = props.getOrDefault(CACHE_LOCATION, System.getProperty("java.io.tmpdir"));
    this.cacheLocation = cacheLocation(location);

    String scheme = scheme(location);
    if (enabled) {
      if (null == scheme || "file".equals(scheme)) {
        this.local = new LocalFileIO();
      } else if ("memory".equals(scheme)) {
        this.local = new InMemoryFileIO();
      } else {
        LOG.warn("Invalid location for async file cache: {}, using in-memory", location);
        this.local = new InMemoryFileIO();
      }

      local.initialize(props);
    }
  }

  @Override
  public void close() {
    if (lazyCache != null) {
      lazyCache.invalidateAll();
      lazyCache.cleanUp();
    }

    super.close();

    if (local != null) {
      local.close();
    }
  }

  private LoadingCache<String, InputFile> cache() {
    if (null == lazyCache) {
      this.lazyCache = Caffeine.newBuilder()
          .softValues()
          .removalListener(
              (RemovalListener<String, InputFile>) (path, cached, cause) -> deleteCached(path))
          .build(this::download);
    }

    return lazyCache;
  }

  private InputFile download(String path) {
    String cachedPath = cachePath(cacheLocation, path);
    InputFile source = super.newInputFile(path);

    Future<InputFile> downloadFuture =
        SharedPools.DOWNLOAD_POOL.submit(
            () -> {
              LOG.info("Starting download for {}", path);

              OutputFile copy = local.newOutputFile(cachedPath);
              try (InputStream in = source.newStream();
                  OutputStream out = copy.createOrOverwrite()) {
                ByteStreams.copy(in, out);
              } catch (IOException e) {
                LOG.warn("Failed to download {} to {}", path, cachedPath, e);
                return null;
              }

              LOG.info("Finished download for {}", path);

              return copy.toInputFile();
            });

    return new FutureInputFile(source, downloadFuture);
  }

  private void deleteCached(String path) {
    local.deleteFile(cachePath(cacheLocation, path));
  }

  private static String scheme(String location) {
    int colonPos = location.indexOf(":");
    if (colonPos > 0) {
      return location.substring(0, colonPos);
    }

    return null;
  }

  private static String cacheLocation(String configPath) {
    String withoutPrefix;
    if (configPath.startsWith(FILE_PREFIX)) {
      withoutPrefix = configPath.substring(FILE_PREFIX.length());
    } else {
      withoutPrefix = configPath;
    }

    return String.format("%s/%s", withoutPrefix, "async-file-io");
  }

  private static String cachePath(String cacheLocation, String path) {
    String hash = Hashing.murmur3_32_fixed().hashString(path, StandardCharsets.UTF_8).toString();
    return String.format("%s/%s/%s", cacheLocation, hash.substring(0, 2), hash.substring(2));
  }

  private static class LocalFileIO implements FileIO {
    @Override
    public InputFile newInputFile(String path) {
      return Files.localInput(path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return Files.localOutput(path);
    }

    @Override
    public void deleteFile(String path) {
      if (!new File(path).delete()) {
        throw new RuntimeIOException("Failed to delete file: " + path);
      }
    }
  }

  private static class FutureInputFile implements InputFile {
    private final InputFile source;
    private final Future<InputFile> future;
    private InputFile delegate = null;

    private FutureInputFile(InputFile source, Future<InputFile> future) {
      this.source = source;
      this.future = future;
    }

    private InputFile delegate() {
      if (delegate == null) {
        try {
          this.delegate = future.get();
          LOG.info("Using downloaded copy of {}", source.location());
        } catch (ExecutionException e) {
          LOG.warn("Download failed", e);
          this.delegate = source;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          this.delegate = source;
        }
      }

      return delegate;
    }

    @Override
    public long getLength() {
      // use the local copy if there is one
      if (future.isDone()) {
        return delegate().getLength();
      } else {
        return source.getLength();
      }
    }

    @Override
    public SeekableInputStream newStream() {
      return delegate().newStream();
    }

    @Override
    public String location() {
      return source.location();
    }

    @Override
    public boolean exists() {
      return source.exists();
    }
  }
}
