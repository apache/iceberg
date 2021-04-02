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

package org.apache.iceberg;

import java.io.Serializable;
import java.util.Objects;

/**
 * An identifier field in {@link RowKey}
 * <p>
 * The field must be:
 *  1. a required column in the table schema
 *  2. a primitive type column
 */
public class RowKeyIdentifierField implements Serializable {

  private final int sourceId;

  RowKeyIdentifierField(int sourceId) {
    this.sourceId = sourceId;
  }

  public int sourceId() {
    return sourceId;
  }

  @Override
  public String toString() {
    return "(" + sourceId + ")";
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (other == null || getClass() != other.getClass()) {
      return false;
    }

    RowKeyIdentifierField that = (RowKeyIdentifierField) other;
    return sourceId == that.sourceId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(sourceId);
  }
}
