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

package org.apache.iceberg.rest;

import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;

class RESTUtil {
  private static final Joiner NULL_JOINER = Joiner.on('\u0000');
  private static final Splitter NULL_SPLITTER = Splitter.on('\u0000');

  private RESTUtil() {
  }

  public static String stripTrailingSlash(String path) {
    if (path == null) {
      return null;
    }

    return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
  }

  public static Map<String, String> filterByPrefix(Map<String, String> properties, String prefix) {
    Preconditions.checkNotNull(properties, "Invalid properties map: null");
    Map<String, String> result = Maps.newHashMap();
    properties.forEach((key, value) -> {
      if (key.startsWith(prefix)) {
        result.put(key.substring(prefix.length()), value);
      }
    });

    return result;
  }

  /**
   * Returns a String representation of a namespace that is suitable for use in a URL / URI.
   * <p>
   * This function needs to be called when a namespace is used as a path variable (or query parameter etc.),
   * to format the namespace per the spec.
   * <p>
   * {@link #urlDecode} should be used to parse the namespace from a URL parameter.
   * @param ns namespace to encode
   * @return UTF-8 encoded string representing the namespace, suitable for use as a URL parameter
   */
  public static String urlEncode(Namespace ns) {
    String[] levels = ns.levels();
    String[] encodedLevels = new String[levels.length];

    for (int i = 0; i < levels.length; i++) {
      try {
        encodedLevels[i] = URLEncoder.encode(levels[i], StandardCharsets.UTF_8.name());
      } catch (UnsupportedEncodingException e) {
        throw new UncheckedIOException(
            String.format("Failed to URL encode namespace '%s' as UTF-8 encoding is not supported", ns), e);
      }
    }

    return NULL_JOINER.join(encodedLevels);
  }

  /**
   * Takes in a string representation of a namespace as used for a URL parameter
   * and returns the corresponding namespace.
   * <p>
   * See also {@link #urlEncode} for generating correctly formatted URLs.
   */
  public static Namespace urlDecode(String encodedNs) {
    Iterable<String> encodedLevels = NULL_SPLITTER.split(encodedNs);
    String[] decodedLevels =
        Streams
            .stream(encodedLevels)
            .map(encodedLevel -> {
              try {
                return URLDecoder.decode(encodedLevel, StandardCharsets.UTF_8.name());
              } catch (UnsupportedEncodingException e) {
                String exceptionMessage = String.format(
                    "Failed to URL decode namespace '%s' as UTF-8 encoding is not supported", encodedNs);
                throw new UncheckedIOException(exceptionMessage, e);
              }
            })
            .toArray(String[]::new);
    return Namespace.of(decodedLevels);
  }

}
