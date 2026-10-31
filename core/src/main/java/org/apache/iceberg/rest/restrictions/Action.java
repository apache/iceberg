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
package org.apache.iceberg.rest.restrictions;

import java.io.Serializable;
import org.apache.iceberg.functions.IcebergFunction;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** A function that a reader must apply to the column with the given field id. */
public class Action implements Serializable {
  private final int fieldId;
  private final IcebergFunction<?, ?> function;

  public Action(int fieldId, IcebergFunction<?, ?> function) {
    Preconditions.checkArgument(fieldId > 0, "Invalid field id: %s (must be positive)", fieldId);
    Preconditions.checkArgument(function != null, "Invalid function: null");
    this.fieldId = fieldId;
    this.function = function;
  }

  public int fieldId() {
    return fieldId;
  }

  public IcebergFunction<?, ?> function() {
    return function;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("fieldId", fieldId)
        .add("function", function)
        .toString();
  }
}
