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

package org.apache.iceberg.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;

final class JdbcUtil {
  private static final Joiner JOINER_DOT = Joiner.on('.');
  private static final Splitter SPLITTER_DOT = Splitter.on('.');

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private JdbcUtil() {
  }

  public static Namespace stringToNamespace(String namespace) {
    Preconditions.checkArgument(namespace != null, "Invalid namespace %s", namespace);
    return Namespace.of(Iterables.toArray(SPLITTER_DOT.split(namespace), String.class));
  }

  public static String namespaceToString(Namespace namespace) {
    return JOINER_DOT.join(namespace.levels());
  }

  public static TableIdentifier stringToTableIdentifier(String tableNamespace, String tableName) {
    return TableIdentifier.of(JdbcUtil.stringToNamespace(tableNamespace), tableName);
  }

  public static Properties filterAndRemovePrefix(Map<String, String> properties,
                                                 String prefix) {
    Properties result = new Properties();
    properties.forEach((key, value) -> {
      if (key.startsWith(prefix)) {
        result.put(key.substring(prefix.length()), value);
      }
    });

    return result;
  }

  public static Map<String, String> convertJsonStringToMap(String jsonString) {
    Map<String, String> resultMap;
    try {
      resultMap = objectMapper.readValue(
              jsonString, new TypeReference<HashMap<String, String>>() {
              });
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    return resultMap;
  }

  public static String convertMapToJsonString(Map<String, String> inputMap) {
    String resultJsonString;
    try {
      resultJsonString = objectMapper.writeValueAsString(inputMap);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    return resultJsonString;
  }
}
