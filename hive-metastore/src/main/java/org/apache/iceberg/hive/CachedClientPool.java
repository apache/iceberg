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
package org.apache.iceberg.hive;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.thrift.TException;
import org.immutables.value.Value;

public class CachedClientPool implements ClientPool<IMetaStoreClient, TException> {

  private static final String CONF_ELEMENT_PREFIX = "conf:";

  private static Cache<Key, HiveClientPool> clientPoolCache;

  private final Configuration conf;
  private final int clientPoolSize;
  private final long evictionInterval;
  private final List<Supplier<Object>> keySuppliers;

  CachedClientPool(Configuration conf, Map<String, String> properties) {
    this.conf = conf;
    this.clientPoolSize =
        PropertyUtil.propertyAsInt(
            properties,
            CatalogProperties.CLIENT_POOL_SIZE,
            CatalogProperties.CLIENT_POOL_SIZE_DEFAULT);
    this.evictionInterval =
        PropertyUtil.propertyAsLong(
            properties,
            CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
            CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_DEFAULT);
    this.keySuppliers =
        extractKeySuppliers(properties.get(CatalogProperties.CLIENT_POOL_CACHE_KEYS), conf);
    init();
  }

  @VisibleForTesting
  HiveClientPool clientPool() {
    return clientPoolCache.get(toKey(keySuppliers), k -> new HiveClientPool(clientPoolSize, conf));
  }

  private synchronized void init() {
    if (clientPoolCache == null) {
      clientPoolCache =
          Caffeine.newBuilder()
              .expireAfterAccess(evictionInterval, TimeUnit.MILLISECONDS)
              .removalListener((key, value, cause) -> ((HiveClientPool) value).close())
              .build();
    }
  }

  @VisibleForTesting
  static Cache<Key, HiveClientPool> clientPoolCache() {
    return clientPoolCache;
  }

  @Override
  public <R> R run(Action<R, IMetaStoreClient, TException> action)
      throws TException, InterruptedException {
    return clientPool().run(action);
  }

  @Override
  public <R> R run(Action<R, IMetaStoreClient, TException> action, boolean retry)
      throws TException, InterruptedException {
    return clientPool().run(action, retry);
  }

  @VisibleForTesting
  static Key toKey(List<Supplier<Object>> suppliers) {
    return Key.of(suppliers.stream().map(Supplier::get).collect(Collectors.toList()));
  }

  @VisibleForTesting
  static List<Supplier<Object>> extractKeySuppliers(String cacheKeys, Configuration conf) {
    URIElement uri = URIElement.of(conf.get(HiveConf.ConfVars.METASTOREURIS.varname, ""));
    if (cacheKeys == null || cacheKeys.isEmpty()) {
      return Collections.singletonList(() -> uri);
    }

    // generate key elements in a certain order, so that the Key instances are comparable
    Set<KeyElementType> types = Sets.newTreeSet(Comparator.comparingInt(Enum::ordinal));
    Map<String, String> confElements = Maps.newTreeMap();
    for (String element : cacheKeys.split(",", -1)) {
      String trimmed = element.trim();
      if (trimmed.toLowerCase(Locale.ROOT).startsWith(CONF_ELEMENT_PREFIX)) {
        String key = trimmed.substring(CONF_ELEMENT_PREFIX.length());
        ValidationException.check(
            !confElements.containsKey(key), "Conf key element %s already specified", key);
        confElements.put(key, conf.get(key));
      } else {
        KeyElementType type = KeyElementType.valueOf(trimmed.toUpperCase());
        switch (type) {
          case URI:
          case UGI:
          case USER_NAME:
            ValidationException.check(
                types.add(type), "%s key element already specified", type.name());
            break;
          default:
            throw new ValidationException("Unknown key element %s", trimmed);
        }
      }
    }
    ImmutableList.Builder<Supplier<Object>> suppliers = ImmutableList.builder();
    for (KeyElementType type : types) {
      switch (type) {
        case URI:
          suppliers.add(() -> uri);
          break;
        case UGI:
          suppliers.add(
              () -> {
                try {
                  return UserGroupInformation.getCurrentUser();
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              });
          break;
        case USER_NAME:
          suppliers.add(
              () -> {
                try {
                  String userName = UserGroupInformation.getCurrentUser().getUserName();
                  return UserNameElement.of(userName);
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              });
          break;
        default:
          throw new RuntimeException("Unexpected key element " + type.name());
      }
    }
    for (String key : confElements.keySet()) {
      ConfElement element = ConfElement.of(key, confElements.get(key));
      suppliers.add(() -> element);
    }
    return suppliers.build();
  }

  @Value.Immutable
  abstract static class Key {

    abstract List<Object> elements();

    private static Key of(Iterable<?> elements) {
      return ImmutableKey.builder().elements(elements).build();
    }
  }

  @Value.Immutable
  abstract static class ConfElement {
    abstract String key();

    @Nullable
    abstract String value();

    static ConfElement of(String key, String value) {
      return ImmutableConfElement.builder().key(key).value(value).build();
    }
  }

  @Value.Immutable
  abstract static class URIElement {
    abstract String metastoreUri();

    static URIElement of(String metastoreUri) {
      return ImmutableURIElement.builder().metastoreUri(metastoreUri).build();
    }
  }

  @Value.Immutable
  abstract static class UserNameElement {
    abstract String userName();

    static UserNameElement of(String userName) {
      return ImmutableUserNameElement.builder().userName(userName).build();
    }
  }

  private enum KeyElementType {
    URI,
    UGI,
    USER_NAME,
    CONF
  }
}
