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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.signing.ImmutableRemoteSigningConfig;
import org.apache.iceberg.rest.signing.RemoteSigningConfig;
import org.apache.iceberg.util.SerializableMap;

/**
 * A serializable {@link RemoteSigningConfig} implementation suitable for use with {@link FileIO}
 * instances.
 *
 * <p>Internal maps and lists are all serializable to ensure compatibility with Kryo.
 */
public final class SerializableRemoteSigningConfig implements RemoteSigningConfig, Serializable {

  public static SerializableRemoteSigningConfig copyOf(RemoteSigningConfig config) {
    return new SerializableRemoteSigningConfig(config);
  }

  private final SerializableMap<String, String> properties;
  private final SerializableMap<String, List<String>> headers;

  private transient volatile RemoteSigningConfig immutableConfig;

  private SerializableRemoteSigningConfig(RemoteSigningConfig toCopy) {
    properties = SerializableMap.copyOf(toCopy.properties());
    headers =
        SerializableMap.copyOf(
            toCopy.headers().entrySet().stream()
                .map(
                    entry ->
                        Map.entry(
                            entry.getKey(),
                            entry.getValue().stream()
                                .<List<String>>collect(
                                    Lists::newArrayList, List::add, List::addAll)))
                .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));
    immutableConfig = ImmutableRemoteSigningConfig.copyOf(toCopy);
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public Map<String, List<String>> headers() {
    return headers;
  }

  public RemoteSigningConfig immutableConfig() {
    if (immutableConfig == null) {
      synchronized (this) {
        if (immutableConfig == null) {
          immutableConfig =
              ImmutableRemoteSigningConfig.builder()
                  .properties(properties)
                  .headers(headers)
                  .build();
        }
      }
    }

    return immutableConfig;
  }
}
