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
package org.apache.iceberg.rest.events;

import java.util.Locale;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Kind of catalog object that an event may refer to. */
public enum CatalogObjectType {
  NAMESPACE,
  TABLE,
  VIEW;

  public static CatalogObjectType fromName(String type) {
    Preconditions.checkArgument(type != null, "Invalid object type: null");
    try {
      return CatalogObjectType.valueOf(type.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(String.format("Invalid object type: %s", type), e);
    }
  }

  @Override
  public String toString() {
    return name().toLowerCase(Locale.ROOT);
  }
}
