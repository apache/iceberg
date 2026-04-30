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

import java.util.Locale;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** The type of a catalog object referenced by a {@link CatalogObjectIdentifier}. */
public enum CatalogObjectType {
  TABLE("table"),
  VIEW("view"),
  NAMESPACE("namespace");

  private final String type;

  CatalogObjectType(String type) {
    this.type = type;
  }

  public String type() {
    return type;
  }

  public static CatalogObjectType fromName(String type) {
    Preconditions.checkArgument(type != null, "Invalid catalog object type: null");
    try {
      return CatalogObjectType.valueOf(type.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(String.format("Invalid catalog object type: %s", type), e);
    }
  }
}
