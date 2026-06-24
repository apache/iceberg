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
package org.apache.iceberg.connect.data;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * A decorator router that filters records via predicates before delegating to another router.
 * Records that don't pass the filter are dropped (empty route list).
 *
 * <p>Configuration properties (prefix {@code iceberg.tables.route.filter.}):
 *
 * <ul>
 *   <li>{@code delegate-class} — fully-qualified class name of the router to delegate to (required)
 *   <li>{@code mode} — {@code include} (only matching records pass) or {@code exclude} (matching
 *       records are dropped). Default: include
 *   <li>{@code topic-regex} — regex that the topic name must match
 *   <li>{@code field} — record value field to evaluate (dot notation for nesting)
 *   <li>{@code field-regex} — regex that the extracted field value must match
 *   <li>{@code header} — header name to evaluate
 *   <li>{@code header-regex} — regex that the extracted header value must match
 *   <li>{@code field-exists} — field path that must exist (non-null) in the record
 * </ul>
 */
public class FilterRouter implements RecordRouter {

  static final String CONFIG_PREFIX = "iceberg.tables.route.filter.";
  static final String DELEGATE_CLASS_PROP = "delegate-class";
  static final String MODE_PROP = "mode";
  static final String TOPIC_REGEX_PROP = "topic-regex";
  static final String FIELD_PROP = "field";
  static final String FIELD_REGEX_PROP = "field-regex";
  static final String HEADER_PROP = "header";
  static final String HEADER_REGEX_PROP = "header-regex";
  static final String FIELD_EXISTS_PROP = "field-exists";

  private RecordRouter delegate;
  private boolean includeMode;
  private Pattern topicRegex;
  private String field;
  private Pattern fieldRegex;
  private String header;
  private Pattern headerRegex;
  private String fieldExists;

  @Override
  public void configure(Map<String, String> props) {
    Map<String, String> filterProps = PropertyUtil.propertiesWithPrefix(props, CONFIG_PREFIX);

    String delegateClass = filterProps.get(DELEGATE_CLASS_PROP);
    if (delegateClass == null || delegateClass.trim().isEmpty()) {
      throw new ConfigException(
          "Must specify delegate router class in " + CONFIG_PREFIX + DELEGATE_CLASS_PROP);
    }

    this.delegate = loadDelegate(delegateClass, props);
    this.includeMode = !"exclude".equalsIgnoreCase(filterProps.getOrDefault(MODE_PROP, "include"));

    String topicRegexStr = filterProps.get(TOPIC_REGEX_PROP);
    this.topicRegex = topicRegexStr != null ? Pattern.compile(topicRegexStr) : null;

    this.field = filterProps.get(FIELD_PROP);
    String fieldRegexStr = filterProps.get(FIELD_REGEX_PROP);
    this.fieldRegex = fieldRegexStr != null ? Pattern.compile(fieldRegexStr) : null;

    this.header = filterProps.get(HEADER_PROP);
    String headerRegexStr = filterProps.get(HEADER_REGEX_PROP);
    this.headerRegex = headerRegexStr != null ? Pattern.compile(headerRegexStr) : null;

    this.fieldExists = filterProps.get(FIELD_EXISTS_PROP);

    if (topicRegex == null && field == null && header == null && fieldExists == null) {
      throw new ConfigException(
          "Must specify at least one filter predicate: "
              + CONFIG_PREFIX
              + "topic-regex, field, header, or field-exists");
    }
  }

  private RecordRouter loadDelegate(String className, Map<String, String> props) {
    try {
      Class<?> clazz = org.apache.iceberg.common.DynClasses.builder().impl(className).build();
      RecordRouter router =
          (RecordRouter)
              org.apache.iceberg.common.DynConstructors.builder()
                  .hiddenImpl(clazz)
                  .build()
                  .newInstance();
      router.configure(props);
      return router;
    } catch (Exception e) {
      throw new ConfigException(
          "Failed to create delegate router from class: " + className + ": " + e.getMessage());
    }
  }

  @Override
  public List<RouteTarget> route(SinkRecord record) {
    boolean predicatesMatch = evaluatePredicates(record);

    if (includeMode) {
      return predicatesMatch ? delegate.route(record) : ImmutableList.of();
    } else {
      return predicatesMatch ? ImmutableList.of() : delegate.route(record);
    }
  }

  private boolean evaluatePredicates(SinkRecord record) {
    return matchesTopic(record)
        && matchesFieldExists(record)
        && matchesField(record)
        && matchesHeader(record);
  }

  private boolean matchesTopic(SinkRecord record) {
    if (topicRegex == null) {
      return true;
    }
    return record.topic() != null && topicRegex.matcher(record.topic()).matches();
  }

  private boolean matchesFieldExists(SinkRecord record) {
    if (fieldExists == null) {
      return true;
    }
    if (record.value() == null) {
      return false;
    }
    try {
      Object value = RecordUtils.extractFromRecordValue(record.value(), fieldExists);
      return value != null;
    } catch (Exception e) {
      return false;
    }
  }

  private boolean matchesField(SinkRecord record) {
    if (field == null) {
      return true;
    }
    String fieldValue = extractFieldValue(record);
    if (fieldValue == null) {
      return false;
    }
    return fieldRegex == null || fieldRegex.matcher(fieldValue).matches();
  }

  private boolean matchesHeader(SinkRecord record) {
    if (header == null) {
      return true;
    }
    String headerValue = extractHeaderValue(record);
    if (headerValue == null) {
      return false;
    }
    return headerRegex == null || headerRegex.matcher(headerValue).matches();
  }

  private String extractFieldValue(SinkRecord record) {
    if (record.value() == null) {
      return null;
    }
    try {
      Object value = RecordUtils.extractFromRecordValue(record.value(), field);
      return value != null ? value.toString() : null;
    } catch (Exception e) {
      return null;
    }
  }

  private String extractHeaderValue(SinkRecord record) {
    if (record.headers() == null) {
      return null;
    }
    for (Header h : record.headers()) {
      if (header.equals(h.key())) {
        Object value = h.value();
        if (value == null) {
          return null;
        }
        if (value instanceof byte[]) {
          return new String((byte[]) value, StandardCharsets.UTF_8);
        }
        return value.toString();
      }
    }
    return null;
  }

  @Override
  public void close() {
    if (delegate != null) {
      delegate.close();
    }
  }
}
