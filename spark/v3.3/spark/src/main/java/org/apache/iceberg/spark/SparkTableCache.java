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
package org.apache.iceberg.spark;

import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class SparkTableCache {

  private static final SparkTableCache INSTANCE = new SparkTableCache();

  private final Map<String, Table> cache = Maps.newConcurrentMap();

  public static SparkTableCache get() {
    return INSTANCE;
  }

  public int size() {
    return cache.size();
  }

  public void add(String key, Table table) {
    cache.put(key, table);
  }

  public boolean contains(String key) {
    return cache.containsKey(key);
  }

  public Table get(String key) {
    return cache.get(key);
  }

  public Table remove(String key) {
    return cache.remove(key);
  }
}
