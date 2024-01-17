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
package org.apache.iceberg.expressions;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import org.apache.iceberg.types.Types;

public abstract class BaseTestEvaluator {
  protected static final Types.StructType STRUCT =
      Types.StructType.of(
          required(13, "x", Types.IntegerType.get()),
          required(14, "y", Types.DoubleType.get()),
          optional(15, "z", Types.IntegerType.get()),
          optional(
              16,
              "s1",
              Types.StructType.of(
                  Types.NestedField.required(
                      17,
                      "s2",
                      Types.StructType.of(
                          Types.NestedField.required(
                              18,
                              "s3",
                              Types.StructType.of(
                                  Types.NestedField.required(
                                      19,
                                      "s4",
                                      Types.StructType.of(
                                          Types.NestedField.required(
                                              20, "i", Types.IntegerType.get()))))))))),
          optional(
              21,
              "s5",
              Types.StructType.of(
                  Types.NestedField.required(
                      22,
                      "s6",
                      Types.StructType.of(
                          Types.NestedField.required(23, "f", Types.FloatType.get()))))));
}
