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
 * Manages content caches for FileIO implementations, capable of creating or retrieving
 * a {@link ContentCache} given a {@link FileIO}, or dropping a cache for a {@link FileIO}.
 */
public interface ContentCacheManager {

    /**
     * Create or retrieve a content cache object for a FileIO.
     * @param io the FileIO to create or retrieve a content cache for
     * @return the content cache
     */
    FileIOContentCache contentCache(FileIO io);

    /**
     * Drop manifest file cache object for a FileIO if exists.
     * @param fileIO the FileIO to drop cache for
     * */
    void dropCache(FileIO fileIO);
}
