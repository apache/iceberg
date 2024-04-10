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

/**
 * Class that provides file-content caching during reading.
 *
 * <p>The file-content caching is initiated by calling {@link FileIOContentCache#tryCache(InputFile)}.
 * Given a FileIO, a file location string, and file length that is within allowed limit,
 * ContentCache will return an implementation of a {@link InputFile} that may be cached.
 */
public interface FileIOContentCache {

    /**
     * Try to cache the input file.  Returns an implementation of {@link InputFile} that may be cached.
     * @param input the input file to cache
     * @return an implementation of {@link InputFile} that may be cached, or the input if it cannot be cached
     */
    InputFile tryCache(InputFile input);

    /**
     * Invalidate the cache for the given key.
     * @param key the cache key
     */
    void invalidate(String key);

    /**
     * Invalidate all entries in the cache.
     */
    void invalidateAll();

    /**
     * Estimate the size of the cache in this application's memory in bytes.
     */
    long estimatedCacheSize();
}
