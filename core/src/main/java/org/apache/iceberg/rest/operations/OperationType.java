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
package org.apache.iceberg.rest.operations;

import java.util.regex.Pattern;

/**
 * Enum representing the type of operation performed on a {@link
 * org.apache.iceberg.catalog.CatalogObject}
 */
public enum OperationType {
  CREATE_TABLE("create-table"),
  REGISTER_TABLE("register-table"),
  DROP_TABLE("drop-table"),
  UPDATE_TABLE("update-table"),
  RENAME_TABLE("rename-table"),
  CREATE_VIEW("create-view"),
  DROP_VIEW("drop-view"),
  UPDATE_VIEW("update-view"),
  RENAME_VIEW("rename-view"),
  CREATE_NAMESPACE("create-namespace"),
  UPDATE_NAMESPACE_PROPERTIES("update-namespace-properties"),
  DROP_NAMESPACE("drop-namespace"),
  CUSTOM("custom");

  private final String type;

  OperationType(String type) {
    this.type = type;
  }

  public String type() {
    return type;
  }

  /** Custom operation type for catalog-specific extensions. */
  public class CustomOperationType {
    private final Pattern PATTERN = Pattern.compile("^x-[a-zA-Z0-9-_.]+$");
    private final String type;

    public CustomOperationType(String type) {
      if (!PATTERN.matcher(type).matches()) {
        throw new IllegalArgumentException(
            "Custom operation type must start with 'x-' followed by an implementation-specific identifier.");
      }

      this.type = type;
    }

    public String type() {
      return type;
    }
  }
}
