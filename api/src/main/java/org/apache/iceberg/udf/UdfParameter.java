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

import javax.annotation.Nullable;

/** A parameter declared in a UDF definition. */
public interface UdfParameter {

  /** The parameter name. */
  String name();

  /**
   * The parameter data type, encoded as a type string for primitives/semi-structured types or as a
   * JSON object for nested types (struct, list, map).
   */
  Object type();

  /** Optional documentation string. */
  @Nullable
  String doc();
}
