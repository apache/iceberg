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

public class VariantConstants {
  public static final byte VERSION = 1; // Variant version
  public static final int SIZE_LIMIT = 1 << 24; // metadata and value size limits

  // The lower 4 bits of the first metadata byte contain the version.
  public static final byte VERSION_MASK = 0x0F;

  public static final int MAX_DECIMAL4_PRECISION = 9;
  public static final int MAX_DECIMAL8_PRECISION = 18;
  public static final int MAX_DECIMAL16_PRECISION = 38;

  private VariantConstants() {}
}
