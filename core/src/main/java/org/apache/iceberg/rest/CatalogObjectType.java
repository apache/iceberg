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

import java.util.Locale;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * The type of a catalog object returned by the universal load relation endpoints. Currently {@link
 * #TABLE} and {@link #VIEW} are defined. Future spec versions may add values such as {@code
 * MATERIALIZED_VIEW}.
 */
public enum CatalogObjectType {
  TABLE("table"),
  VIEW("view");

  private final String value;

  CatalogObjectType(String value) {
    this.value = value;
  }

  public String value() {
    return value;
  }

  @Override
  public String toString() {
    return value;
  }

  public static CatalogObjectType fromString(String value) {
    Preconditions.checkArgument(value != null, "Invalid catalog object type: null");
    try {
      return CatalogObjectType.valueOf(value.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(String.format("Invalid catalog object type: %s", value));
    }
  }
}
