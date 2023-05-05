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
package org.apache.spark.sql.connector.iceberg.catalog;

import java.util.Objects;
import org.apache.spark.sql.types.DataType;

/** A {@link ProcedureParameter} implementation. */
class ProcedureParameterImpl implements ProcedureParameter {
  private final String name;
  private final DataType dataType;
  private final boolean required;

  ProcedureParameterImpl(String name, DataType dataType, boolean required) {
    this.name = name;
    this.dataType = dataType;
    this.required = required;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public DataType dataType() {
    return dataType;
  }

  @Override
  public boolean required() {
    return required;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (other == null || getClass() != other.getClass()) {
      return false;
    }

    ProcedureParameterImpl that = (ProcedureParameterImpl) other;
    return required == that.required
        && Objects.equals(name, that.name)
        && Objects.equals(dataType, that.dataType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, dataType, required);
  }

  @Override
  public String toString() {
    return String.format(
        "ProcedureParameter(name='%s', type=%s, required=%b)", name, dataType, required);
  }
}
