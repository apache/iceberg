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

/**
 * Identifies a table in iceberg catalog
 */
public class TableIdentifier {
  private final Namespace namespace;
  private final String name;

  public TableIdentifier(Namespace namespace, String name) {
    if (namespace == null) {
      throw new IllegalArgumentException("namespace can not be null");
    }

    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("name can not be null or empty");
    }

    this.namespace = namespace;
    this.name = name;
  }

  public Namespace namespace() {
    return namespace;
  }

  public String name() {
    return name;
  }
}
