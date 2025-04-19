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
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.immutables.value.Value;

@Value.Immutable
public interface StorageCredential extends Serializable {

  String prefix();

  Map<String, String> config();

  @Value.Check
  default void validate() {
    Preconditions.checkArgument(!prefix().isEmpty(), "Invalid prefix: must be non-empty");
    Preconditions.checkArgument(!config().isEmpty(), "Invalid config: must be non-empty");
  }

  static StorageCredential create(String prefix, Map<String, String> config) {
    return ImmutableStorageCredential.builder().prefix(prefix).config(config).build();
  }

  static Map<String, String> toMap(List<StorageCredential> credentials) {
    Preconditions.checkArgument(null != credentials, "Invalid storage credentials: null");
    Map<String, String> config = Maps.newHashMap();
    for (StorageCredential credential : credentials) {
      String key = "storage-credential." + credential.prefix();
      config.put(key, credential.prefix());
      credential
          .config()
          .forEach(
              (k, v) -> {
                String innerKey = key + "." + k;
                config.put(innerKey, v);
              });
    }

    return config;
  }

  static List<StorageCredential> fromMap(Map<String, String> map) {
    Preconditions.checkArgument(null != map, "Invalid storage credentials config: null");
    Map<String, String> config = PropertyUtil.propertiesWithPrefix(map, "storage-credential.");
    List<StorageCredential> credentials = Lists.newArrayList();

    Set<String> prefixes =
        config.entrySet().stream()
            .filter(entry -> entry.getKey().equals(entry.getValue()))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());

    prefixes.forEach(
        prefix -> {
          Map<String, String> configForCredential =
              PropertyUtil.propertiesWithPrefix(config, prefix + ".");
          Preconditions.checkArgument(
              !configForCredential.isEmpty(),
              "Invalid storage credential config with prefix %s: null or empty",
              prefix);
          StorageCredential credential = create(prefix, configForCredential);
          credentials.add(credential);
        });

    return credentials;
  }
}
