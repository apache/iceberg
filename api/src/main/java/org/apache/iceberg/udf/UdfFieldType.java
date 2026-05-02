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
package org.apache.iceberg.udf;

import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** A field within a {@link UdfStructType}, with a name and a type. */
public final class UdfFieldType {

  private final String name;
  private final UdfType type;

  public static UdfFieldType of(String name, UdfType type) {
    Preconditions.checkArgument(name != null, "Invalid field name: null");
    Preconditions.checkArgument(type != null, "Invalid field type: null");
    return new UdfFieldType(name, type);
  }

  private UdfFieldType(String name, UdfType type) {
    this.name = name;
    this.type = type;
  }

  public String name() {
    return name;
  }

  public UdfType type() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof UdfFieldType)) {
      return false;
    }

    UdfFieldType that = (UdfFieldType) o;
    return Objects.equals(name, that.name) && Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type);
  }

  @Override
  public String toString() {
    return String.format("%s:%s", name, type);
  }
}
