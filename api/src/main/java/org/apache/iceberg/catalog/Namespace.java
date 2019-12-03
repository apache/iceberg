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

package org.apache.iceberg.catalog;

import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * A namespace in a {@link Catalog}.
 */
public class Namespace {
  private static final Namespace EMPTY_NAMESPACE = new Namespace(new String[] {});
  private static final Joiner DOT = Joiner.on('.');

  public static Namespace empty() {
    return EMPTY_NAMESPACE;
  }

  public static Namespace of(String... levels) {
    if (levels.length == 0) {
      return empty();
    }

    return new Namespace(levels);
  }

  private final String[] levels;

  private Namespace(String[] levels) {
    this.levels = levels;
  }

  public String[] levels() {
    return levels;
  }

  public String level(int pos) {
    return levels[pos];
  }

  public boolean isEmpty() {
    return levels.length == 0;
  }

  private String uuid;

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public String getUuid() {
    return this.uuid;
  }

  private String location;

  public void setLocation(String location) {
    this.location = location;
  }

  public String getLocation() {
    return this.location;
  }

  private long timestampMillis;

  public void setTimestampMillis(Long timestampMillis) {
    this.timestampMillis = timestampMillis;
  }

  public Long getTimestampMillis() {
    return this.timestampMillis;
  }

  private Map<String, String> properties = new HashMap<>();

  public void setProperties(String key, String value) {
    this.properties.put(key, value);
  }

  public Map<String, String> getProperties() {
    return this.properties;
  }

  public String getProperties(String key) {
    return this.properties.get(key);
  }

  public boolean hasProperties(String key) {
    return this.properties.containsKey(key);
  }

  private  List<String> tables = new ArrayList<>();

  public void addTables(String tableName) {
    this.tables.add(tableName);
  }

  public void deletTables(String tableName) {
    this.tables.remove(tableName);
  }

  public void setTables(List<String> tableList) {
    this.tables.addAll(tableList);
  }

  public List<String> getTables() {
    return this.tables;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    Namespace namespace = (Namespace) other;
    return Arrays.equals(levels, namespace.levels);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(levels);
  }

  @Override
  public String toString() {
    return DOT.join(levels);
  }

}
