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

import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;

public class ResourcePaths {
  private static final Joiner SLASH = Joiner.on("/").skipNulls();
  private static final String PREFIX = "prefix";

  public static ResourcePaths forCatalogProperties(Map<String, String> properties) {
    return new ResourcePaths(properties.get(PREFIX));
  }

  public static String config() {
    return "v1/config";
  }

  public static String tokens() {
    return "v1/oauth/tokens";
  }

  private final String prefix;

  public ResourcePaths(String prefix) {
    this.prefix = prefix;
  }

  public String namespaces() {
    return SLASH.join("v1", prefix, "namespaces");
  }

  public String namespace(Namespace ns) {
    return SLASH.join("v1", prefix, "namespaces", RESTUtil.encodeNamespace(ns));
  }

  public String namespaceProperties(Namespace ns) {
    return SLASH.join("v1", prefix, "namespaces", RESTUtil.encodeNamespace(ns), "properties");
  }

  public String tables(Namespace ns) {
    return SLASH.join("v1", prefix, "namespaces", RESTUtil.encodeNamespace(ns), "tables");
  }

  public String table(TableIdentifier ident) {
    return SLASH.join(
        "v1",
        prefix,
        "namespaces",
        RESTUtil.encodeNamespace(ident.namespace()),
        "tables",
        RESTUtil.encodeString(ident.name()));
  }

  public String rename() {
    return SLASH.join("v1", prefix, "tables", "rename");
  }

  public String metrics(TableIdentifier identifier) {
    return SLASH.join(
        "v1",
        prefix,
        "namespaces",
        RESTUtil.encodeNamespace(identifier.namespace()),
        "tables",
        RESTUtil.encodeString(identifier.name()),
        "metrics");
  }

  public String commitTransaction() {
    return SLASH.join("v1", prefix, "transactions", "commit");
  }
}
