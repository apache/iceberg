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

package org.apache.iceberg;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.events.IncrementalScanEvent;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.util.Pair;

public class CatalogProperties {

  private CatalogProperties() {
  }

  public static final String CATALOG_IMPL = "catalog-impl";
  public static final String FILE_IO_IMPL = "io-impl";
  public static final String WAREHOUSE_LOCATION = "warehouse";

  /**
   * Controls whether the catalog will cache table entries upon load.
   * <p>
   * If {@link #CACHE_EXPIRATION_INTERVAL_MS} is set to zero, this value
   * will be ignored and the cache will be disabled.
   */
  public static final String CACHE_ENABLED = "cache-enabled";
  public static final boolean CACHE_ENABLED_DEFAULT = true;

  /**
   * Controls the duration for which entries in the catalog are cached.
   * <p>
   * Behavior of specific values of cache.expiration-interval-ms:
   * <ul>
   *   <li> Zero - Caching and cache expiration are both disabled</li>
   *   <li> Negative Values - Cache expiration is turned off and entries expire only on refresh etc</li>
   *   <li> Positive Values - Cache entries expire if not accessed via the cache after this many milliseconds</li>
   * </ul>
   */
  public static final String CACHE_EXPIRATION_INTERVAL_MS = "cache.expiration-interval-ms";
  public static final long CACHE_EXPIRATION_INTERVAL_MS_DEFAULT = TimeUnit.SECONDS.toMillis(30);
  public static final long CACHE_EXPIRATION_INTERVAL_MS_OFF = -1;

  public static final String URI = "uri";
  public static final String CLIENT_POOL_SIZE = "clients";
  public static final int CLIENT_POOL_SIZE_DEFAULT = 2;
  public static final String CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS = "client.pool.cache.eviction-interval-ms";
  public static final long CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_DEFAULT = TimeUnit.MINUTES.toMillis(5);

  public static final String LOCK_IMPL = "lock-impl";

  public static final String LOCK_HEARTBEAT_INTERVAL_MS = "lock.heartbeat-interval-ms";
  public static final long LOCK_HEARTBEAT_INTERVAL_MS_DEFAULT = TimeUnit.SECONDS.toMillis(3);

  public static final String LOCK_HEARTBEAT_TIMEOUT_MS = "lock.heartbeat-timeout-ms";
  public static final long LOCK_HEARTBEAT_TIMEOUT_MS_DEFAULT = TimeUnit.SECONDS.toMillis(15);

  public static final String LOCK_HEARTBEAT_THREADS = "lock.heartbeat-threads";
  public static final int LOCK_HEARTBEAT_THREADS_DEFAULT = 4;

  public static final String LOCK_ACQUIRE_INTERVAL_MS = "lock.acquire-interval-ms";
  public static final long LOCK_ACQUIRE_INTERVAL_MS_DEFAULT = TimeUnit.SECONDS.toMillis(5);

  public static final String LOCK_ACQUIRE_TIMEOUT_MS = "lock.acquire-timeout-ms";
  public static final long LOCK_ACQUIRE_TIMEOUT_MS_DEFAULT = TimeUnit.MINUTES.toMillis(3);

  public static final String LOCK_TABLE = "lock.table";

  public static final String APP_ID = "app-id";
  public static final String USER = "user";

  /**
   * Listeners are registered using catalog properties following the pattern of
   * listeners.(listener-name).(listener-property)=(property-value)
   * <p>
   * A listener name cannot contain dot (.) character
   * The specified listener is registered when a catalog is initialized
   * <p>
   * For example, there is the set of catalog properties registering an AWS SQS listener of name prod:
   * <ul>
   *   <li>listener.prod.impl=org.apache.iceberg.aws.sns.SnsListener
   *   <li>listener.prod.event-types=scan,incremental-scan
   *   <li>listener.prod.sns.topic-arn=arn:aws:sns:us-east-2:123456789012:MyTopic
   * </ul>
   */
  public static String listenerCatalogProperty(String listenerName, String listenerProperty) {
    return "listener." + listenerName + "." + listenerProperty;
  }

  /**
   * Parse the listener name and listener property from a catalog property string
   * @param listenerCatalogProperty listener catalog property
   * @return a pair of the listener name and listener property
   */
  public static Optional<Pair<String, String>> parseListenerCatalogProperty(String listenerCatalogProperty) {
    Matcher matcher = Pattern.compile("^listener[.](?<name>[^\\.]+)[.](?<property>.+)$")
        .matcher(listenerCatalogProperty);
    if (matcher.matches()) {
      return Optional.of(Pair.of(matcher.group("name"), matcher.group("property")));
    }

    return Optional.empty();
  }

  /**
   * Listener property describing the implementation Java class name of the listener for dynamic loading
   */
  public static final String LISTENER_PROPERTY_IMPL = "impl";

  /**
   * Listener property describing the event types that a listener subscribes to.
   * The value is a comma delimited list of event types (Java class name),
   * e.g. org.apache.iceberg.events.ScanEvent,org.apache.iceberg.events.IncrementalScanEvent.
   * If not specified, the listener subscribes to events listed in {@link #LISTENER_EVENT_TYPES_DEFAULT}
   */
  public static final String LISTENER_PROPERTY_EVENT_TYPES = "event-types";
  public static final Set<Class<?>> LISTENER_EVENT_TYPES_DEFAULT = ImmutableSet.of(
      ScanEvent.class,
      IncrementalScanEvent.class,
      CreateSnapshotEvent.class
  );
}
