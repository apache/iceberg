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

package org.apache.iceberg.parquet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Copied from parquet-hadoop
 */
class ColumnConfigParser {

  private static class ConfigHelper<T> {
    private final String prefix;
    private final Function<String, T> function;
    private final BiConsumer<String, T> consumer;

    ConfigHelper(String prefix, Function<String, T> function, BiConsumer<String, T> consumer) {
      this.prefix = prefix;
      this.function = function;
      this.consumer = consumer;
    }

    public void processKey(String key) {
      if (key.startsWith(prefix)) {
        String columnPath = key.substring(prefix.length());
        T value = function.apply(key);
        consumer.accept(columnPath, value);
      }
    }
  }

  private final List<ConfigHelper<?>> helpers = new ArrayList<>();

  public <T> ColumnConfigParser withColumnConfig(String prefix, Function<String, T> function,
      BiConsumer<String, T> consumer) {
    helpers.add(new ConfigHelper<>(prefix, function, consumer));
    return this;
  }

  public void parseConfig(Map<String, String> conf) {
    for (Map.Entry<String, String> entry : conf.entrySet()) {
      for (ConfigHelper<?> helper : helpers) {
        // We retrieve the value from function instead of parsing from the string here to get the correct type
        helper.processKey(entry.getKey());
      }
    }
  }
}
