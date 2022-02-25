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

package org.apache.iceberg.dell.ecs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convert Map properties to bytes.
 */
public interface PropertiesSerDes {

  /**
   * Version of current implementation.
   */
  String CURRENT_VERSION = "0";

  Logger LOG = LoggerFactory.getLogger(PropertiesSerDes.class);

  /**
   * Read properties
   *
   * @param version is the version of {@link PropertiesSerDes}
   */
  Map<String, String> read(InputStream input, String version);

  /**
   * Write properties, the version is {@link #currentVersion()}
   */
  byte[] toBytes(Map<String, String> value);

  /**
   * Get version of current serializer implementation.
   */
  String currentVersion();

  /**
   * Use {@link Properties} to serialize and deserialize properties.
   */
  static PropertiesSerDes current() {
    return new PropertiesSerDes() {
      @Override
      public Map<String, String> read(InputStream input, String version) {
        Preconditions.checkArgument(version.equals(CURRENT_VERSION),
            "Properties version is not match", version);
        Properties jdkProperties = new Properties();
        try {
          jdkProperties.load(new InputStreamReader(input, StandardCharsets.UTF_8));
        } catch (IOException e) {
          LOG.error("fail to read properties", e);
          throw new UncheckedIOException(e);
        }

        Set<String> propertyNames = jdkProperties.stringPropertyNames();
        Map<String, String> properties = Maps.newHashMap();
        for (String name : propertyNames) {
          properties.put(name, jdkProperties.getProperty(name));
        }

        return Collections.unmodifiableMap(properties);
      }

      @Override
      public byte[] toBytes(Map<String, String> value) {
        Properties jdkProperties = new Properties();
        for (Map.Entry<String, String> entry : value.entrySet()) {
          jdkProperties.setProperty(entry.getKey(), entry.getValue());
        }

        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
          jdkProperties.store(new OutputStreamWriter(output, StandardCharsets.UTF_8), null);
          return output.toByteArray();
        } catch (IOException e) {
          LOG.error("fail to store properties {} to file", value, e);
          throw new UncheckedIOException(e);
        }
      }

      @Override
      public String currentVersion() {
        return CURRENT_VERSION;
      }
    };
  }
}
