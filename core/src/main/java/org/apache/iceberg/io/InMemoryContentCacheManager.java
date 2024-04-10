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
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.SystemConfigs;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class InMemoryContentCacheManager implements ContentCacheManager {
    private static final Logger LOG = LoggerFactory.getLogger(InMemoryContentCacheManager.class);
    private final Cache<Object, InMemoryContentCache> cache = newManifestCacheBuilder().build();
    private static final InMemoryContentCacheManager DEFAULT_CONTENT_CACHES = new InMemoryContentCacheManager();

    private InMemoryContentCacheManager() {
        // singleton
    }

    public static InMemoryContentCacheManager create(Map<String, String> unused) {
        return DEFAULT_CONTENT_CACHES;
    }

    @Override
    public FileIOContentCache contentCache(FileIO io) {
        return cache.get(
                io,
                fileIO -> new InMemoryContentCache(cacheDurationMs(io), cacheTotalBytes(io), cacheMaxContentLength(io)));
    }

    @Override
    public synchronized void dropCache(FileIO fileIO) {
        cache.invalidate(fileIO);
        cache.cleanUp();
    }

    static long cacheDurationMs(FileIO io) {
        return PropertyUtil.propertyAsLong(
                io.properties(),
                CatalogProperties.IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS,
                CatalogProperties.IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS_DEFAULT);
    }

    static long cacheTotalBytes(FileIO io) {
        return PropertyUtil.propertyAsLong(
                io.properties(),
                CatalogProperties.IO_MANIFEST_CACHE_MAX_TOTAL_BYTES,
                CatalogProperties.IO_MANIFEST_CACHE_MAX_TOTAL_BYTES_DEFAULT);
    }

    static long cacheMaxContentLength(FileIO io) {
        return PropertyUtil.propertyAsLong(
                io.properties(),
                CatalogProperties.IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH,
                CatalogProperties.IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH_DEFAULT);
    }

    @VisibleForTesting
    static Caffeine<Object, Object> newManifestCacheBuilder() {
        int maxSize = SystemConfigs.IO_MANIFEST_CACHE_MAX_FILEIO.value();
        return Caffeine.newBuilder()
                .weakKeys()
                .softValues()
                .maximumSize(maxSize)
                .removalListener(
                        (io, contentCache, cause) ->
                                LOG.debug("Evicted {} from FileIO-level cache ({})", io, cause))
                .recordStats();
    }
}
