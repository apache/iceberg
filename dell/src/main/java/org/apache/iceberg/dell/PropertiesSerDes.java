/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.dell;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * convert Map properties to bytes.
 */
public interface PropertiesSerDes {

  /**
   * The current version of properties files.
   * <p>
   * This property set in the object metadata. And now it is only a placeholder and may distinct in the future.
   */
  String CURRENT_VERSION = "0";

  /**
   * E-Tag property name in results
   */
  String E_TAG_KEY = "ecs-object-e-tag";

  /**
   * Version property name in results
   */
  String PROPERTY_VERSION_KEY = "ecs-object-property-version";

  Logger log = LoggerFactory.getLogger(PropertiesSerDes.class);

  /**
   * read properties from stream
   *
   * @param input is stream
   * @return properties
   */
  Map<String, String> read(InputStream input);

  /**
   * Get the properties with the object content.
   * <p>
   * This method help to put additional properties in object metadata.
   */
  default Map<String, String> readProperties(byte[] content, String eTag, String version) {
    Map<String, String> propertiesInObject = read(new ByteArrayInputStream(content));
    Map<String, String> properties = new HashMap<>(propertiesInObject);
    properties.put(E_TAG_KEY, eTag);
    properties.put(PROPERTY_VERSION_KEY, version);
    return properties;
  }

  /**
   * Write properties to bytes.
   */
  byte[] toBytes(Map<String, String> value);

  /**
   * Create a {@link PropertiesSerDes} to serialize and deserialize properties with {@link Properties}.
   */
  static PropertiesSerDes useJdk() {
    return new PropertiesSerDes() {
      @Override
      public Map<String, String> read(InputStream input) {
        Properties jdkProperties = new Properties();
        try {
          jdkProperties.load(new InputStreamReader(input, StandardCharsets.UTF_8));
        } catch (IOException e) {
          log.error("fail to read properties", e);
          throw new UncheckedIOException(e);
        }
        Set<String> propertyNames = jdkProperties.stringPropertyNames();
        Map<String, String> properties = new HashMap<>();
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
          log.error("fail to store properties {} to file", value, e);
          throw new UncheckedIOException(e);
        }
      }
    };
  }
}
