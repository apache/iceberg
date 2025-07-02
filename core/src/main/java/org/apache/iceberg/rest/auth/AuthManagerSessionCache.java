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

import java.util.function.Function;

/** A cache for {@link AuthSession} instances. */
public interface AuthManagerSessionCache<K, V extends AuthSession> extends AutoCloseable {

  /**
   * Returns a cached session for the given key, loading it with the given loader if it is not
   * already cached.
   *
   * @param key the key to use for the session.
   * @param loader the loader to use to load the session if it is not already cached.
   * @return the cached session.
   */
  V cachedSession(K key, Function<K, V> loader);

  @Override
  void close();
}
