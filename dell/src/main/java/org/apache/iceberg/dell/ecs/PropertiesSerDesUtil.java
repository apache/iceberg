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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Convert Map properties to bytes. */
public class PropertiesSerDesUtil {

  private PropertiesSerDesUtil() {}

  /** Version of current implementation. */
  private static final String CURRENT_VERSION = "0";

  private static final Logger LOG = LoggerFactory.getLogger(PropertiesSerDesUtil.class);

  /**
   * Read properties
   *
   * @param version is the version of {@link PropertiesSerDesUtil}
   */
  public static Map<String, String> read(byte[] content, String version) {
    Preconditions.checkArgument(
        CURRENT_VERSION.equals(version), "Properties version is not match", version);
    Properties jdkProperties = new Properties();
    try (Reader reader =
        new InputStreamReader(new ByteArrayInputStream(content), StandardCharsets.UTF_8)) {
      jdkProperties.load(reader);
    } catch (IOException e) {
      LOG.error("Fail to read properties", e);
      throw new UncheckedIOException(e);
    }

    Set<String> propertyNames = jdkProperties.stringPropertyNames();
    Map<String, String> properties = Maps.newHashMap();
    for (String name : propertyNames) {
      properties.put(name, jdkProperties.getProperty(name));
    }

    return Collections.unmodifiableMap(properties);
  }

  /** Write properties, the version is {@link #currentVersion()} */
  public static byte[] toBytes(Map<String, String> value) {
    Properties jdkProperties = new Properties();
    for (Map.Entry<String, String> entry : value.entrySet()) {
      jdkProperties.setProperty(entry.getKey(), entry.getValue());
    }

    try (ByteArrayOutputStream output = new ByteArrayOutputStream();
        Writer writer = new OutputStreamWriter(output, StandardCharsets.UTF_8)) {
      jdkProperties.store(writer, null);
      return output.toByteArray();
    } catch (IOException e) {
      LOG.error("Fail to store properties {} to file", value, e);
      throw new UncheckedIOException(e);
    }
  }

  /** Get version of current serializer implementation. */
  public static String currentVersion() {
    return CURRENT_VERSION;
  }
}
