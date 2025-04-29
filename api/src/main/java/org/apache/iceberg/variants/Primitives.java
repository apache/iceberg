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
package org.apache.iceberg.variants;

class Primitives {
  static final int TYPE_NULL = 0;
  static final int TYPE_TRUE = 1;
  static final int TYPE_FALSE = 2;
  static final int TYPE_INT8 = 3;
  static final int TYPE_INT16 = 4;
  static final int TYPE_INT32 = 5;
  static final int TYPE_INT64 = 6;
  static final int TYPE_DOUBLE = 7;
  static final int TYPE_DECIMAL4 = 8;
  static final int TYPE_DECIMAL8 = 9;
  static final int TYPE_DECIMAL16 = 10;
  static final int TYPE_DATE = 11;
  static final int TYPE_TIMESTAMPTZ = 12; // equivalent to timestamptz
  static final int TYPE_TIMESTAMPNTZ = 13; // equivalent to timestamp
  static final int TYPE_FLOAT = 14;
  static final int TYPE_BINARY = 15;
  static final int TYPE_STRING = 16;
  static final int TYPE_TIME = 17;
  static final int TYPE_TIMESTAMPTZ_NANOS = 18;
  static final int TYPE_TIMESTAMPNTZ_NANOS = 19;
  static final int TYPE_UUID = 20;

  static final int PRIMITIVE_TYPE_SHIFT = 2;

  private Primitives() {}
}
