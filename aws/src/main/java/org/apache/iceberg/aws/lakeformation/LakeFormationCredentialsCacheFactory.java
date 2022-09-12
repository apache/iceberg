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
package org.apache.iceberg.aws.lakeformation;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

class LakeFormationCredentialsCacheFactory {
  private final Cache<LakeFormationIdentity, LakeFormationCredentialsCache> caches;

  private static LakeFormationCredentialsCacheFactory instance;

  private LakeFormationCredentialsCacheFactory(int maxCacheSize, long cacheExpirationInMillis) {
    this.caches =
        Caffeine.newBuilder()
            .expireAfterAccess(cacheExpirationInMillis, TimeUnit.MILLISECONDS)
            .maximumSize(maxCacheSize)
            .recordStats()
            .build();
  }

  public static synchronized LakeFormationCredentialsCacheFactory getInstance(
      int maxCacheSize, long cacheExpirationInMillis) {
    if (instance == null) {
      instance = new LakeFormationCredentialsCacheFactory(maxCacheSize, cacheExpirationInMillis);
    }
    return instance;
  }

  public static Optional<LakeFormationCredentialsCacheFactory> getInstanceIfExists() {
    return Optional.ofNullable(instance);
  }

  synchronized LakeFormationCredentialsCache buildCache(
      LakeFormationIdentity lakeFormationIdentity, long cacheExpiryLeadTimeInMillis) {
    try {
      LakeFormationCredentialsCache cache =
          caches.get(
              lakeFormationIdentity,
              identity -> {
                LakeFormationCredentialsCacheLoader loader =
                    new LakeFormationCredentialsCacheLoader(identity.lakeFormationClient());
                LoadingCache<String, LakeFormationTemporaryCredentials> loadingCache =
                    Caffeine.newBuilder().build(loader);
                return new LakeFormationCredentialsCache(loadingCache, cacheExpiryLeadTimeInMillis);
              });
      return cache;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
