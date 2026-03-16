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
package org.apache.iceberg.hadoop;

import java.io.Serializable;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.SerializableSupplier;

/** Wraps a {@link Configuration} object in a {@link Serializable} layer. */
public class SerializableConfiguration implements SerializableSupplier<Configuration> {
  private final Map<String, String> confAsMap;
  private transient volatile Configuration hadoopConf = null;

  public SerializableConfiguration(Configuration hadoopConf) {
    this.confAsMap = Maps.newHashMapWithExpectedSize(hadoopConf.size());
    hadoopConf.forEach(entry -> confAsMap.put(entry.getKey(), entry.getValue()));
  }

  @Override
  public Configuration get() {
    if (hadoopConf == null) {
      synchronized (this) {
        if (hadoopConf == null) {
          Configuration newConf = new Configuration(false);
          confAsMap.forEach(newConf::set);
          this.hadoopConf = newConf;
        }
      }
    }

    return hadoopConf;
  }
}
