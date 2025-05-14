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

import org.apache.spark.sql.types.DataType;

/** An input parameter of a {@link Procedure stored procedure}. */
public interface ProcedureParameter {

  /**
   * Creates a required input parameter.
   *
   * @param name the name of the parameter
   * @param dataType the type of the parameter
   * @return the constructed stored procedure parameter
   */
  static ProcedureParameter required(String name, DataType dataType) {
    return new ProcedureParameterImpl(name, dataType, true);
  }

  /**
   * Creates an optional input parameter.
   *
   * @param name the name of the parameter.
   * @param dataType the type of the parameter.
   * @return the constructed optional stored procedure parameter
   */
  static ProcedureParameter optional(String name, DataType dataType) {
    return new ProcedureParameterImpl(name, dataType, false);
  }

  /** Returns the name of this parameter. */
  String name();

  /** Returns the type of this parameter. */
  DataType dataType();

  /** Returns true if this parameter is required. */
  boolean required();
}
