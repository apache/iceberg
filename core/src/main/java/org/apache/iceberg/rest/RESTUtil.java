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

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.UUIDUtil;

public class RESTUtil {
  private static final char NAMESPACE_SEPARATOR = '\u001f';
  private static final String NAMESPACE_ESCAPED_SEPARATOR = "%1F";
  private static final Joiner NAMESPACE_ESCAPED_JOINER = Joiner.on(NAMESPACE_ESCAPED_SEPARATOR);
  private static final Splitter NAMESPACE_ESCAPED_SPLITTER =
      Splitter.on(NAMESPACE_ESCAPED_SEPARATOR);

  /**
   * @deprecated since 1.11.0, will be removed in 1.12.0; use {@link
   *     RESTUtil#namespaceToQueryParam(Namespace)}} instead.
   */
  @Deprecated public static final Joiner NAMESPACE_JOINER = Joiner.on(NAMESPACE_SEPARATOR);

  /**
   * @deprecated since 1.11.0, will be removed in 1.12.0; use {@link
   *     RESTUtil#namespaceFromQueryParam(String)} instead.
   */
  @Deprecated public static final Splitter NAMESPACE_SPLITTER = Splitter.on(NAMESPACE_SEPARATOR);

  public static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";

  private RESTUtil() {}

  public static String stripTrailingSlash(String path) {
    if (path == null) {
      return null;
    }

    String result = path;
    while (result.endsWith("/")) {
      result = result.substring(0, result.length() - 1);
    }
    return result;
  }

  /**
   * Merge updates into a target string map.
   *
   * @param target a map to update
   * @param updates a map of updates
   * @return an immutable result map built from target and updates
   */
  public static Map<String, String> merge(Map<String, String> target, Map<String, String> updates) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    target.forEach(
        (key, value) -> {
          if (!updates.containsKey(key)) {
            builder.put(key, value);
          }
        });

    updates.forEach(builder::put);

    return builder.build();
  }

  /**
   * Takes in a map, and returns a copy filtered on the entries with keys beginning with the
   * designated prefix. The keys are returned with the prefix removed.
   *
   * <p>Any entries whose keys don't begin with the prefix are not returned.
   *
   * <p>This can be used to get a subset of the configuration related to the REST catalog, such as
   * all properties from a prefix of `spark.sql.catalog.my_catalog.rest.` to get REST catalog
   * specific properties from the spark configuration.
   */
  public static Map<String, String> extractPrefixMap(
      Map<String, String> properties, String prefix) {
    Preconditions.checkNotNull(properties, "Invalid properties map: null");
    return PropertyUtil.propertiesWithPrefix(properties, prefix);
  }

  private static final Joiner.MapJoiner FORM_JOINER = Joiner.on("&").withKeyValueSeparator("=");
  private static final Splitter.MapSplitter FORM_SPLITTER =
      Splitter.on("&").withKeyValueSeparator("=");

  /**
   * Encodes a map of form data as application/x-www-form-urlencoded.
   *
   * <p>This encodes the form with pairs separated by &amp; and keys separated from values by =.
   *
   * @param formData a map of form data
   * @return a String of encoded form data
   */
  public static String encodeFormData(Map<?, ?> formData) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    formData.forEach(
        (key, value) ->
            builder.put(encodeString(String.valueOf(key)), encodeString(String.valueOf(value))));
    return FORM_JOINER.join(builder.build());
  }

  /**
   * Decodes a map of form data from application/x-www-form-urlencoded.
   *
   * <p>This decodes the form with pairs separated by &amp; and keys separated from values by =.
   *
   * @param formString a map of form data
   * @return a map of key/value form data
   */
  public static Map<String, String> decodeFormData(String formString) {
    return FORM_SPLITTER.split(formString).entrySet().stream()
        .collect(
            ImmutableMap.toImmutableMap(
                e -> RESTUtil.decodeString(e.getKey()), e -> RESTUtil.decodeString(e.getValue())));
  }

  /**
   * Encodes a string using URL encoding
   *
   * <p>{@link #decodeString(String)} should be used to decode.
   *
   * @param toEncode string to encode
   * @return UTF-8 encoded string, suitable for use as a URL parameter
   */
  public static String encodeString(String toEncode) {
    Preconditions.checkArgument(toEncode != null, "Invalid string to encode: null");
    return URLEncoder.encode(toEncode, StandardCharsets.UTF_8);
  }

  /**
   * Decodes a URL-encoded string.
   *
   * <p>See also {@link #encodeString(String)} for URL encoding.
   *
   * @param encoded a string to decode
   * @return a decoded string
   */
  public static String decodeString(String encoded) {
    Preconditions.checkArgument(encoded != null, "Invalid string to decode: null");
    return URLDecoder.decode(encoded, StandardCharsets.UTF_8);
  }

  /**
   * This converts the given namespace to a string and separates each part in a multipart namespace
   * using the unicode character '\u001f'. Note that this method is different from {@link
   * RESTUtil#encodeNamespace(Namespace)}, which uses the UTF-8 escaped version of '\u001f', which
   * is '0x1F'.
   *
   * <p>{@link #namespaceFromQueryParam(String)} should be used to convert the namespace string back
   * to a {@link Namespace} instance.
   *
   * @param namespace The namespace to convert
   * @return The namespace converted to a string where each part in a multipart namespace is
   *     separated using the unicode character '\u001f'
   */
  public static String namespaceToQueryParam(Namespace namespace) {
    Preconditions.checkArgument(null != namespace, "Invalid namespace: null");
    return RESTUtil.NAMESPACE_JOINER.join(namespace.levels());
  }

  /**
   * This converts a namespace where each part in a multipart namespace has been separated using
   * '\u001f' to its original {@link Namespace} instance.
   *
   * <p>{@link #namespaceToQueryParam(Namespace)} should be used to convert the {@link Namespace} to
   * a string.
   *
   * @param namespace The namespace to convert
   * @return The namespace instance from the given namespace string, where each multipart separator
   *     ('\u001f') is converted to a separate namespace level
   */
  public static Namespace namespaceFromQueryParam(String namespace) {
    Preconditions.checkArgument(null != namespace, "Invalid namespace: null");
    return Namespace.of(
        RESTUtil.NAMESPACE_SPLITTER.splitToStream(namespace).toArray(String[]::new));
  }

  /**
   * Returns a String representation of a namespace that is suitable for use in a URL / URI.
   *
   * <p>This function needs to be called when a namespace is used as a path variable (or query
   * parameter etc.), to format the namespace per the spec.
   *
   * <p>{@link #decodeNamespace} should be used to parse the namespace from a URL parameter.
   *
   * @param ns namespace to encode
   * @return UTF-8 encoded string representing the namespace, suitable for use as a URL parameter
   */
  public static String encodeNamespace(Namespace ns) {
    Preconditions.checkArgument(ns != null, "Invalid namespace: null");
    String[] levels = ns.levels();
    String[] encodedLevels = new String[levels.length];

    for (int i = 0; i < levels.length; i++) {
      encodedLevels[i] = encodeString(levels[i]);
    }

    return NAMESPACE_ESCAPED_JOINER.join(encodedLevels);
  }

  /**
   * Takes in a string representation of a namespace as used for a URL parameter and returns the
   * corresponding namespace.
   *
   * <p>See also {@link #encodeNamespace} for generating correctly formatted URLs.
   *
   * @param encodedNs a namespace to decode
   * @return a namespace
   */
  public static Namespace decodeNamespace(String encodedNs) {
    Preconditions.checkArgument(encodedNs != null, "Invalid namespace: null");
    String[] levels = Iterables.toArray(NAMESPACE_ESCAPED_SPLITTER.split(encodedNs), String.class);

    // Decode levels in place
    for (int i = 0; i < levels.length; i++) {
      levels[i] = decodeString(levels[i]);
    }

    return Namespace.of(levels);
  }

  /**
   * Returns the catalog URI suffixed by the relative endpoint path. If the endpoint path is an
   * absolute path, then the absolute endpoint path is returned without using the catalog URI.
   *
   * @param catalogUri The catalog URI that is typically passed through {@link
   *     org.apache.iceberg.CatalogProperties#URI}
   * @param endpointPath Either an absolute or relative endpoint path
   * @return The actual endpoint path if it's an absolute path or the catalog uri suffixed by the
   *     endpoint path if the path is relative.
   */
  public static String resolveEndpoint(String catalogUri, String endpointPath) {
    if (null == endpointPath) {
      return null;
    }

    if (null == catalogUri
        || endpointPath.startsWith("http://")
        || endpointPath.startsWith("https://")) {
      return endpointPath;
    }

    return String.format(
        "%s%s",
        RESTUtil.stripTrailingSlash(catalogUri),
        endpointPath.startsWith("/") ? endpointPath : "/" + endpointPath);
  }

  public static Map<String, String> configHeaders(Map<String, String> properties) {
    return RESTUtil.extractPrefixMap(properties, "header.");
  }

  /**
   * Returns a single-use headers map containing a freshly generated idempotency key. The key is a
   * UUIDv7 string suitable for use in the Idempotency-Key header.
   */
  public static Map<String, String> idempotencyHeaders() {
    return ImmutableMap.of(IDEMPOTENCY_KEY_HEADER, UUIDUtil.generateUuidV7().toString());
  }
}
